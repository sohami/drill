/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.fn.impl.ValueVectorHashHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.work.filter.BloomFilter;
import org.apache.drill.exec.work.filter.RuntimeFilterWritable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A RuntimeFilterRecordBatch steps over the ScanBatch. If the ScanBatch participates
 * in the HashJoinBatch and can be applied by a RuntimeFilter, it will generate a filtered
 * SV2, otherwise will generate a same recordCount-originalRecordCount SV2 which will not affect
 * the Query's performance ,but just do a memory transfer by the later RemovingRecordBatch op.
 */
public class RuntimeFilterRecordBatch extends AbstractSingleRecordBatch<Filter> {
  private SelectionVector2 sv2;

  private ValueVectorHashHelper.Hash64 hash64;
  private Map<String, Integer> field2id = new HashMap<>();
  private List<String> toFilterFields;
  private int originalRecordCount;
  private int recordCount;
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RuntimeFilterRecordBatch.class);

  public RuntimeFilterRecordBatch(Filter pop, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(pop, context, incoming);
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public int getRecordCount() {
    return sv2.getCount();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv2;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return null;
  }

  @Override
  protected IterOutcome doWork() {
    container.transferIn(incoming.getContainer());
    originalRecordCount = incoming.getRecordCount();
    sv2.setOriginalRecordCount(originalRecordCount);
    try {
      applyRuntimeFilter();
    } catch (SchemaChangeException e) {
      throw new UnsupportedOperationException(e);
    }
    return getFinalOutcome(false);
  }

  @Override
  public void close() {
    if (sv2 != null) {
      sv2.clear();
    }
    super.close();
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    if (sv2 != null) {
      sv2.clear();
    }

    switch (incoming.getSchema().getSelectionVectorMode()) {
      case NONE:
        if (sv2 == null) {
          sv2 = new SelectionVector2(oContext.getAllocator());
        }
        for (final VectorWrapper<?> v : incoming) {
          container.addOrGet(v.getField(), callBack);
        }
        break;
      case TWO_BYTE:
        sv2 = new SelectionVector2(oContext.getAllocator());
        for (final VectorWrapper<?> v : incoming) {
          container.addOrGet(v.getField(), callBack);
        }
        break;
      case FOUR_BYTE:

      default:
        throw new UnsupportedOperationException();
    }

    if (container.isSchemaChanged()) {
      container.buildSchema(SelectionVectorMode.TWO_BYTE);
      return true;
    }
    return false;
  }

  /**
   *
   * @return True means rows are filtered by the RuntimeFilter , SelectionVector2 is set.
   * False means not affected by the RuntimeFilter, SelectionVector2 is no set.
   * @throws SchemaChangeException
   */
  private void applyRuntimeFilter() throws SchemaChangeException {
    RuntimeFilterWritable runtimeFilterWritable = context.getRuntimeFilter();
    if (runtimeFilterWritable == null) {
      sv2.setRecordCount(incoming.getRecordCount());
      return;
    }
    if (originalRecordCount <= 0) {
      sv2.setRecordCount(0);
      return ;
    }
    List<BloomFilter> bloomFilters = runtimeFilterWritable.unwrap();
    if (hash64 == null) {
      ValueVectorHashHelper hashHelper = new ValueVectorHashHelper(this, context);
      try {
        //generate hash helper
        this.toFilterFields = runtimeFilterWritable.getRuntimeFilterBDef().getProbeFieldsList();
        List<LogicalExpression> hashFieldExps = new ArrayList<>();
        List<TypedFieldId> typedFieldIds = new ArrayList<>();
        for (String toFilterField : toFilterFields) {
          SchemaPath schemaPath = new SchemaPath(new PathSegment.NameSegment(toFilterField), ExpressionPosition.UNKNOWN);
          TypedFieldId typedFieldId = container.getValueVectorId(schemaPath);
          this.field2id.put(toFilterField, typedFieldId.getFieldIds()[0]);
          typedFieldIds.add(typedFieldId);
          ValueVectorReadExpression toHashFieldExp = new ValueVectorReadExpression(typedFieldId);
          hashFieldExps.add(toHashFieldExp);
        }
        hash64 = hashHelper.getHash64(hashFieldExps.toArray(new LogicalExpression[hashFieldExps.size()]), typedFieldIds.toArray(new TypedFieldId[typedFieldIds.size()]));
      } catch (Exception e) {
        throw UserException.internalError(e).build(logger);
      }
    }
    sv2.allocateNew(originalRecordCount);
    //To make each independent bloom filter work together to construct a final filter result: BitSet.
    BitSet bitSet = new BitSet(originalRecordCount);
    for (int i = 0; i < toFilterFields.size(); i++) {
      BloomFilter bloomFilter = bloomFilters.get(i);
      String fieldName = toFilterFields.get(i);
      computeBitSet(field2id.get(fieldName), bloomFilter, bitSet);
    }

    int svIndex = 0;
    int tmpFilterRows = 0;
    for (int i = 0; i < originalRecordCount; i++) {
      boolean contain = bitSet.get(i);
      if (contain) {
        sv2.setIndex(svIndex, i);
        svIndex++;
      } else {
        tmpFilterRows++;
      }
    }

    if (tmpFilterRows > 0 && tmpFilterRows == originalRecordCount) {
      //all rows of the batch was filtered
      recordCount = 0;
      logger.debug("filter {} rows by the RuntimeFilter", tmpFilterRows);
    }
    if (tmpFilterRows > 0 && tmpFilterRows != originalRecordCount ) {
      //partial of the rows was filtered
      //totalFilterRows = totalFilterRows + tmpFilterRows;
      recordCount = svIndex;
      logger.debug("filter {} rows by the RuntimeFilter", tmpFilterRows);
    }
    //no rows filtered
    if (tmpFilterRows == 0) {
      recordCount = originalRecordCount;
    }
    sv2.setRecordCount(recordCount);
    return ;
  }

  private void computeBitSet(int fieldId, BloomFilter bloomFilter, BitSet bitSet) throws SchemaChangeException {
    for (int rowIndex = 0; rowIndex < recordCount; rowIndex++) {
      long hash = hash64.hash64Code(rowIndex, 0, fieldId);
      boolean contain = bloomFilter.find(hash);
      if (contain) {
        bitSet.set(rowIndex, true);
      } else {
        bitSet.set(rowIndex, false);
      }
    }
  }
}
