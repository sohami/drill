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
package org.apache.drill.exec.work.filter;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.Consumer;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.ops.SendingAccountor;
import org.apache.drill.exec.ops.StatusHandler;
import org.apache.drill.exec.physical.PhysicalPlan;

import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.BroadcastExchange;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.Wrapper;
import org.apache.drill.exec.planner.physical.HashAggPrel;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.SortPrel;
import org.apache.drill.exec.planner.physical.StreamAggPrel;
import org.apache.drill.exec.planner.physical.TopNPrel;
import org.apache.drill.exec.planner.physical.visitor.BasePrelVisitor;
import org.apache.drill.exec.proto.BitData;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class traverses the physical operator tree to find the HashJoin operator
 * for which is JPPD (join predicate push down) is possible. The prerequisite to do JPPD
 * is:
 * 1. The join condition is equality
 * 2. The physical join node is a HashJoin one
 * 3. The probe side children of the HashJoin node should not contain a blocking operator like HashAgg
 */
public class RuntimeFilterManager {

  private Wrapper rootWrapper;
  //HashJoin node's major fragment id to its corresponding probe side nodes's endpoints
  private Map<Integer, List<CoordinationProtos.DrillbitEndpoint>> joinMjId2probdeScanEps = new HashMap<>();
  //HashJoin node's major fragment id to its corresponding probe side nodes's number
  private Map<Integer, Integer> joinMjId2scanSize = new ConcurrentHashMap<>();
  //HashJoin node's major fragment id to its corresponding probe side scan node's belonging major fragment id
  private Map<Integer, Integer> joinMjId2ScanMjId = new HashMap<>();
  //HashJoin node's major fragment id to its aggregated RuntimeFilterWritable
  private Map<Integer, RuntimeFilterWritable> joinMjId2AggregatedRF = new ConcurrentHashMap<>();

  private DrillbitContext drillbitContext;

  private SendingAccountor sendingAccountor = new SendingAccountor();

  private String lineSeparator;

  private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterManager.class);

  /**
   * This class maintains context for the runtime join push down's filter management. It
   * does a traversal of the physical operators by leveraging the root wrapper which indirectly
   * holds the global PhysicalOperator tree and contains the minor fragment endpoints.
   * @param workUnit
   * @param drillbitContext
   */
  public RuntimeFilterManager(QueryWorkUnit workUnit, DrillbitContext drillbitContext) {
    this.rootWrapper = workUnit.getRootWrapper();
    this.drillbitContext = drillbitContext;
    lineSeparator = System.lineSeparator();
  }


  /**
   * Step 1 :
   * Generate a possible RuntimeFilter of a HashJoinPrel, left some BF parameters of the generated RuntimeFilter
   * to be set later.
   * @param hashJoinPrel
   * @return null or a partial information RuntimeFilterDef
   */
  public static RuntimeFilterDef generateRuntimeFilter(HashJoinPrel hashJoinPrel) {
    JoinRelType joinRelType = hashJoinPrel.getJoinType();
    JoinInfo joinInfo = hashJoinPrel.analyzeCondition();
    boolean allowJoin = (joinInfo.isEqui()) && (joinRelType == JoinRelType.INNER || joinRelType == JoinRelType.RIGHT);
    if (!allowJoin) {
      return null;
    }
    List<BloomFilterDef> bloomFilterDefs = new ArrayList<>();
    //find the possible left scan node of the left join key
    GroupScan groupScan = null;
    RelNode left = hashJoinPrel.getLeft();
    List<String> leftFields = left.getRowType().getFieldNames();
    List<Integer> leftKeys = hashJoinPrel.getLeftKeys();
    RelMetadataQuery metadataQuery = left.getCluster().getMetadataQuery();
    for (Integer leftKey : leftKeys) {
      String leftFieldName = leftFields.get(leftKey);
      //This also avoids the left field of the join condition with a function call.
      ScanPrel scanPrel = findLeftScanPrel(leftFieldName, left);
      if (scanPrel != null) {
        boolean encounteredBlockNode = containBlockNode((Prel) left, scanPrel);
        if (encounteredBlockNode) {
          continue;
        }
        //Collect NDV from the Metadata
        RelDataType scanRowType = scanPrel.getRowType();
        RelDataTypeField field = scanRowType.getField(leftFieldName, true, true);
        int index = field.getIndex();
        Double ndv = metadataQuery.getDistinctRowCount(scanPrel, ImmutableBitSet.of(index), null);
        if (ndv == null) {
          //If NDV is not supplied, we use the row count to estimate the ndv.
          ndv = left.estimateRowCount(metadataQuery) * 0.1;
        }
        //only the probe side field name and hash seed is definite, other information left to pad later
        BloomFilterDef bloomFilterDef = new BloomFilterDef(0, 0,false, leftFieldName);
        bloomFilterDef.setLeftNDV(ndv);
        bloomFilterDefs.add(bloomFilterDef);
        groupScan = scanPrel.getGroupScan();
      }
    }
    if (bloomFilterDefs.size() > 0) {
      //only bloom filters parameter & probe side GroupScan is definitely set here
      RuntimeFilterDef runtimeFilterDef = new RuntimeFilterDef(true, false, bloomFilterDefs, false);
      runtimeFilterDef.setProbeSideGroupScan(groupScan);
      return  runtimeFilterDef;
    }
    return null;
  }

  /**
   * Step 2:
   * Complement the RuntimeFilter information
   * @param plan
   * @param queryContext
   */
  public static void complementRuntimeFilterInfo(PhysicalPlan plan, QueryContext queryContext) {
    final PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();
    RuntimeFilterInfoPaddingHelper runtimeFilterInfoPaddingHelper = new RuntimeFilterInfoPaddingHelper(queryContext);
    rootOperator.accept(runtimeFilterInfoPaddingHelper, null);
  }


  /**
   * Step3 :
   * This method is to collect the parallel information of the RuntimetimeFilters. Then it res a RuntimeFilterRouting to
   * record the relationship between the RuntimeFilter producers and consumers.
   */
  public void collectRuntimeFilterParallelAndControlInfo(Pointer<String> textPlan) {
    Map<String, String> mjOpIdPair2runtimeFilter = new HashMap<>();
    RuntimeFilterParallelismCollector runtimeFilterParallelismCollector = new RuntimeFilterParallelismCollector();
    rootWrapper.getNode().getRoot().accept(runtimeFilterParallelismCollector, null);
    List<RFHelperHolder> holders = runtimeFilterParallelismCollector.getHolders();

    for (RFHelperHolder holder : holders) {
      List<CoordinationProtos.DrillbitEndpoint> probeSideEndpoints = holder.getProbeSideScanEndpoints();
      int probeSideScanMajorId = holder.getProbeSideScanMajorId();
      int joinNodeMajorId = holder.getJoinMajorId();
      RuntimeFilterDef runtimeFilterDef = holder.getRuntimeFilterDef();
      boolean sendToForeman = runtimeFilterDef.isSendToForeman();
      //mark the runtime filter info to the profile
      int probeSideScanOpId = holder.getProbeSideScanOpId();
      List<BloomFilterDef> bloomFilterDefs = runtimeFilterDef.getBloomFilterDefs();
      String mjOpIdPair = String.format("%02d-%02d", probeSideScanMajorId, probeSideScanOpId);
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("RuntimeFilter[");
      for (BloomFilterDef bloomFilterDef : bloomFilterDefs) {
        stringBuilder.append(bloomFilterDef.toString()).append(",");
      }
      stringBuilder.append("]");
      String runtimeFiltersJson = stringBuilder.toString();
      mjOpIdPair2runtimeFilter.put(mjOpIdPair, runtimeFiltersJson);
      if (sendToForeman) {
        //send RuntimeFilter to Foreman
        joinMjId2probdeScanEps.put(joinNodeMajorId, probeSideEndpoints);
        joinMjId2scanSize.put(joinNodeMajorId, probeSideEndpoints.size());
        joinMjId2ScanMjId.put(joinNodeMajorId, probeSideScanMajorId);
      }
    }
    reconstructTextPlan(textPlan, mjOpIdPair2runtimeFilter);
  }


  public void waitForComplete() {
    sendingAccountor.waitForSendComplete();
  }

  /**
   * This method is passively invoked by receiving a runtime filter from the network
   * @param runtimeFilterWritable
   */
  public void registerRuntimeFilter(RuntimeFilterWritable runtimeFilterWritable) {
    BitData.RuntimeFilterBDef runtimeFilterB = runtimeFilterWritable.getRuntimeFilterBDef();
    int majorId = runtimeFilterB.getMajorFragmentId();
    UserBitShared.QueryId queryId = runtimeFilterB.getQueryId();
    List<String> probeFields = runtimeFilterB.getProbeFieldsList();
    logger.info("RuntimeFilterManager receives a runtime filter , majorId:{}, queryId:{}", majorId, QueryIdHelper.getQueryId(queryId));
    int size;
    synchronized (this) {
      size = joinMjId2scanSize.get(majorId);
      Preconditions.checkState(size > 0);
      RuntimeFilterWritable aggregatedRuntimeFilter = joinMjId2AggregatedRF.get(majorId);
      if (aggregatedRuntimeFilter == null) {
        aggregatedRuntimeFilter = runtimeFilterWritable;
      } else {
        aggregatedRuntimeFilter.aggregate(runtimeFilterWritable);
      }
      joinMjId2AggregatedRF.put(majorId, aggregatedRuntimeFilter);
      size--;
      joinMjId2scanSize.put(majorId, size);
    }
    if (size == 0) {
      broadcastAggregatedRuntimeFilter(majorId, queryId, probeFields);
    }
  }

  /**
   * Find a join condition's left input source scan Prel. If we can't find a target scan Prel then this
   * RuntimeFilter can not pushed down to a probe side scan Prel.
   * @param fieldName left join condition field Name
   * @param leftRelNode left RelNode of a BiRel or the SingleRel
   * @return a left scan Prel which contains the left join condition name or null
   */
  private static ScanPrel findLeftScanPrel(String fieldName, RelNode leftRelNode) {
    if (leftRelNode instanceof ScanPrel) {
      RelDataType scanRowType = leftRelNode.getRowType();
      RelDataTypeField field = scanRowType.getField(fieldName, true, true);
      if (field != null) {
        //found
        return (ScanPrel)leftRelNode;
      } else {
        return null;
      }
    } else if (leftRelNode instanceof RelSubset) {
      RelNode bestNode = ((RelSubset) leftRelNode).getBest();
      if (bestNode != null) {
        return findLeftScanPrel(fieldName, bestNode);
      } else {
        return null;
      }
    } else {
      List<RelNode> relNodes = leftRelNode.getInputs();
      RelNode leftNode = relNodes.get(0);
      return findLeftScanPrel(fieldName, leftNode);
    }
  }

  private static boolean containBlockNode(Prel startNode, Prel endNode) {
    BlockNodeVisitor blockNodeVisitor = new BlockNodeVisitor();
    startNode.accept(blockNodeVisitor, endNode);
    return blockNodeVisitor.isEncounteredBlockNode();
  }

  private void reconstructTextPlan(Pointer<String> textPlan, Map<String, String> mjOpIdPair2runtimeFilter) {
    if (textPlan != null && textPlan.value != null && !mjOpIdPair2runtimeFilter.isEmpty()) {
      String[] lines = textPlan.value.split(lineSeparator);
      Set<String> idPairs = mjOpIdPair2runtimeFilter.keySet();
      StringBuilder stringBuilder = new StringBuilder();
      for (String line : lines) {
        for (String idPair : idPairs) {
          if (line.startsWith(idPair)) {
            line = line + " : " + mjOpIdPair2runtimeFilter.get(idPair);
          }
        }
        stringBuilder.append(line).append(lineSeparator);
      }
      textPlan.value = stringBuilder.toString();
    }
  }

  private void broadcastAggregatedRuntimeFilter(int joinMajorId, UserBitShared.QueryId queryId, List<String> probeFields) {
    List<CoordinationProtos.DrillbitEndpoint> scanNodeEps = joinMjId2probdeScanEps.get(joinMajorId);
    int scanNodeMjId = joinMjId2ScanMjId.get(joinMajorId);
    for (int minorId = 0; minorId < scanNodeEps.size(); minorId++) {
      BitData.RuntimeFilterBDef.Builder builder = BitData.RuntimeFilterBDef.newBuilder();
      for (String probeField : probeFields) {
        builder.addProbeFields(probeField);
      }
      BitData.RuntimeFilterBDef runtimeFilterBDef = builder
        .setQueryId(queryId)
        .setMajorFragmentId(scanNodeMjId)
        .setMinorFragmentId(minorId)
        .build();
      RuntimeFilterWritable aggregatedRuntimeFilter = joinMjId2AggregatedRF.get(joinMajorId);
      RuntimeFilterWritable runtimeFilterWritable = new RuntimeFilterWritable(runtimeFilterBDef, aggregatedRuntimeFilter.getData());
      CoordinationProtos.DrillbitEndpoint drillbitEndpoint = scanNodeEps.get(minorId);

      DataTunnel dataTunnel = drillbitContext.getDataConnectionsPool().getTunnel(drillbitEndpoint);
      Consumer<RpcException> exceptionConsumer = new Consumer<RpcException>() {
        @Override
        public void accept(final RpcException e) {
          //logger.error("fail to broadcast a runtime filter to the probe side scan node", e);
        }

        @Override
        public void interrupt(final InterruptedException e) {
          //logger.error("fail to broadcast a runtime filter to the probe side scan node", e);
        }
      };
      RpcOutcomeListener<GeneralRPCProtos.Ack> statusHandler = new StatusHandler(exceptionConsumer, sendingAccountor);
      AccountingDataTunnel accountingDataTunnel = new AccountingDataTunnel(dataTunnel, sendingAccountor, statusHandler);
      accountingDataTunnel.sendRuntimeFilter(runtimeFilterWritable);
    }
  }


  /**
   * Find all the previous defined runtime filters to complement their information.
   */
  private static class RuntimeFilterInfoPaddingHelper extends AbstractPhysicalVisitor<Void, RFHelperHolder, RuntimeException> {

    private boolean enableRuntimeFilter;

    private int bloomFilterSizeInBytesDef;

    private double fpp;

    public RuntimeFilterInfoPaddingHelper(QueryContext queryContext) {
      this.enableRuntimeFilter = queryContext.getOption(ExecConstants.HASHJOIN_ENABLE_RUNTIME_FILTER_KEY).bool_val;
      this.bloomFilterSizeInBytesDef = queryContext.getOption(ExecConstants.HASHJOIN_BLOOM_FILTER_DEFAULT_SIZE_KEY).int_val;
      this.fpp = queryContext.getOption(ExecConstants.HASHJOIN_BLOOM_FILTER_FPP_KEY).float_val;
    }

    @Override
    public Void visitExchange(Exchange exchange, RFHelperHolder holder) throws RuntimeException {
      if (holder != null) {
        boolean broadcastExchange = exchange instanceof BroadcastExchange;
        if (holder.isFromBuildSide()) {
          //To the build side ,we need to identify whether the HashJoin's direct children have a Broadcast node to mark
          //this HashJoin as BroadcastHashJoin
          holder.setEncounteredBroadcastExchange(broadcastExchange);
        }
      }
      return visitOp(exchange, holder);
    }

    @Override
    public Void visitOp(PhysicalOperator op, RFHelperHolder holder) throws RuntimeException {
      boolean isHashJoinOp = op instanceof HashJoinPOP;
      if (isHashJoinOp && !enableRuntimeFilter) {
        HashJoinPOP hashJoinPOP = (HashJoinPOP) op;
        hashJoinPOP.setRuntimeFilterDef(null);
      }
      if (isHashJoinOp && enableRuntimeFilter) {
        HashJoinPOP hashJoinPOP = (HashJoinPOP) op;
        RuntimeFilterDef runtimeFilterDef = hashJoinPOP.getRuntimeFilterDef();
        if (runtimeFilterDef != null) {
          //TODO check whether to enable RuntimeFilter according to the NDV percent
          /**
          double threshold = 0.5;
          double percent = leftNDV / rightDNV;
          if (percent > threshold ) {
            hashJoinPOP.setRuntimeFilterDef(null);
            return visitChildren(op, holder);
          }
           */
          runtimeFilterDef.setGenerateBloomFilter(true);
          if (holder == null) {
            holder = new RFHelperHolder();
          }
          GroupScan probeSideScanOp = runtimeFilterDef.getProbeSideGroupScan();
          org.apache.drill.exec.physical.base.PhysicalOperator left = hashJoinPOP.getLeft();
          left.accept(this, holder);
          int probeSideScanOpId = probeSideScanOp.getOperatorId();
          holder.setProbeSideScanOpId(probeSideScanOpId);
          //explore the right build side children to find potential RuntimeFilters.
          PhysicalOperator right = hashJoinPOP.getRight();
          holder.setFromBuildSide(true);
          right.accept(this, holder);
          boolean buildSideEncountererdBroadcastExchange = holder.isEncounteredBroadcastExchange();
          if (buildSideEncountererdBroadcastExchange) {
            runtimeFilterDef.setSendToForeman(false);
          } else {
            runtimeFilterDef.setSendToForeman(true);
          }
          List<BloomFilterDef> bloomFilterDefs = runtimeFilterDef.getBloomFilterDefs();
          for (BloomFilterDef bloomFilterDef : bloomFilterDefs) {
            Double leftNDV = bloomFilterDef.getLeftNDV();
            int bloomFilterSizeInBytes = bloomFilterSizeInBytesDef;
            if (leftNDV != null && leftNDV != 0.0) {
              bloomFilterSizeInBytes = BloomFilter.optimalNumOfBytes(leftNDV.longValue(), fpp);
            }
            if (buildSideEncountererdBroadcastExchange) {
              bloomFilterDef.setLocal(true);
            } else {
              bloomFilterDef.setLocal(false);
            }
            bloomFilterDef.setNumBytes(bloomFilterSizeInBytes);
          }
          return null;
        }
      }
      return visitChildren(op, holder);
    }
  }

  /**
   * Collect the runtime filter parallelism related information such as join node major/minor fragment id , probe side scan node's
   * major/minor fragment id, probe side node's endpoints.
   */
  protected class RuntimeFilterParallelismCollector extends AbstractPhysicalVisitor<Void, RFHelperHolder, RuntimeException> {

    private List<RFHelperHolder> holders = new ArrayList<>();

    @Override
    public Void visitOp(PhysicalOperator op, RFHelperHolder holder) throws RuntimeException {
      boolean isHashJoinOp = op instanceof HashJoinPOP;
      if (isHashJoinOp) {
        HashJoinPOP hashJoinPOP = (HashJoinPOP) op;
        RuntimeFilterDef runtimeFilterDef = hashJoinPOP.getRuntimeFilterDef();
        if (runtimeFilterDef != null) {
          if (holder == null) {
            holder = new RFHelperHolder();
            holders.add(holder);
          }
          holder.setRuntimeFilterDef(runtimeFilterDef);
          GroupScan probeSideScanOp = runtimeFilterDef.getProbeSideGroupScan();
          Wrapper container = findPhysicalOpContainer(rootWrapper, hashJoinPOP);
          int majorFragmentId = container.getMajorFragmentId();
          holder.setJoinMajorId(majorFragmentId);
          Wrapper probeSideScanContainer = findPhysicalOpContainer(container, probeSideScanOp);
          int probeSideScanMjId = probeSideScanContainer.getMajorFragmentId();
          int probeSideScanOpId = probeSideScanOp.getOperatorId();
          List<CoordinationProtos.DrillbitEndpoint> probeSideScanEps = probeSideScanContainer.getAssignedEndpoints();
          holder.setProbeSideScanEndpoints(probeSideScanEps);
          holder.setProbeSideScanMajorId(probeSideScanMjId);
          holder.setProbeSideScanOpId(probeSideScanOpId);
        }
      }
      return visitChildren(op, holder);
    }

    public List<RFHelperHolder> getHolders() {
      return holders;
    }
  }

  private class WrapperOperatorsVisitor extends AbstractPhysicalVisitor<Void, Void, RuntimeException> {

    private PhysicalOperator targetOp;

    private Fragment fragment;

    private boolean contain = false;

    public WrapperOperatorsVisitor(PhysicalOperator targetOp, Fragment fragment) {
      this.targetOp = targetOp;
      this.fragment = fragment;
    }

    @Override
    public Void visitExchange(Exchange exchange, Void value) throws RuntimeException {
      List<Fragment.ExchangeFragmentPair> exchangeFragmentPairs = fragment.getReceivingExchangePairs();
      for (Fragment.ExchangeFragmentPair exchangeFragmentPair : exchangeFragmentPairs) {
        boolean same = exchange == exchangeFragmentPair.getExchange();
        if (same) {
          return null;
        }
      }
      return exchange.getChild().accept(this, value);
    }

    @Override
    public Void visitOp(PhysicalOperator op, Void value) throws RuntimeException {
      boolean same = op == targetOp;
      if (!same) {
        for (PhysicalOperator child : op) {
          child.accept(this, value);
        }
      } else {
        contain = true;
      }
      return null;
    }

    public boolean isContain() {
      return contain;
    }
  }

  private boolean containsPhysicalOperator(Wrapper wrapper, PhysicalOperator op) {
    WrapperOperatorsVisitor wrapperOpsVistitor = new WrapperOperatorsVisitor(op, wrapper.getNode());
    wrapper.getNode().getRoot().accept(wrapperOpsVistitor, null);
    return wrapperOpsVistitor.isContain();
  }

  private Wrapper findPhysicalOpContainer(Wrapper wrapper, PhysicalOperator op) {
    boolean contain = containsPhysicalOperator(wrapper, op);
    if (contain) {
      return wrapper;
    }
    List<Wrapper> dependencies = wrapper.getFragmentDependencies();
    if (CollectionUtils.isEmpty(dependencies)) {
      return null;
    }
    for (Wrapper dependencyWrapper : dependencies) {
      Wrapper opContainer = findPhysicalOpContainer(dependencyWrapper, op);
      if (opContainer != null) {
        return opContainer;
      }
    }
    //should not be here
    throw new IllegalStateException("It should not be here!");
  }

  private static class BlockNodeVisitor extends BasePrelVisitor<Void, Prel, RuntimeException> {

    private boolean encounteredBlockNode;

    @Override
    public Void visitPrel(Prel prel, Prel endValue) throws RuntimeException {
      if (prel == endValue) {
        return null;
      }

      Prel currentPrel;
      if (prel instanceof RelSubset) {
        currentPrel = (Prel) ((RelSubset) prel).getBest();
      } else {
        currentPrel = prel;
      }

      if (currentPrel == null) {
        return null;
      }
      if (currentPrel instanceof StreamAggPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof HashAggPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof SortPrel) {
        encounteredBlockNode = true;
        return null;
      }

      if (currentPrel instanceof TopNPrel) {
        encounteredBlockNode = true;
        return null;
      }

      for (Prel subPrel : currentPrel) {
        visitPrel(subPrel, endValue);
      }
      return null;
    }

    public boolean isEncounteredBlockNode() {
      return encounteredBlockNode;
    }
  }

  /**
   * RuntimeFilter helper util holder
   */
  private static class RFHelperHolder {

    private int joinMajorId;

    private int probeSideScanMajorId;

    private int probeSideScanOpId;

    private List<CoordinationProtos.DrillbitEndpoint> probeSideScanEndpoints;

    //whether this join operator is a partitioned HashJoin or broadcast HashJoin,
    //also single node HashJoin is not expected to do JPPD.
    private boolean encounteredBroadcastExchange;

    private boolean fromBuildSide;

    private RuntimeFilterDef runtimeFilterDef;


    public List<CoordinationProtos.DrillbitEndpoint> getProbeSideScanEndpoints() {
      return probeSideScanEndpoints;
    }

    public void setProbeSideScanEndpoints(List<CoordinationProtos.DrillbitEndpoint> probeSideScanEndpoints) {
      this.probeSideScanEndpoints = probeSideScanEndpoints;
    }

    public int getJoinMajorId() {
      return joinMajorId;
    }

    public void setJoinMajorId(int joinMajorId) {
      this.joinMajorId = joinMajorId;
    }

    public int getProbeSideScanMajorId() {
      return probeSideScanMajorId;
    }

    public void setProbeSideScanMajorId(int probeSideScanMajorId) {
      this.probeSideScanMajorId = probeSideScanMajorId;
    }


    public int getProbeSideScanOpId() {
      return probeSideScanOpId;
    }

    public void setProbeSideScanOpId(int probeSideScanOpId) {
      this.probeSideScanOpId = probeSideScanOpId;
    }

    public boolean isEncounteredBroadcastExchange() {
      return encounteredBroadcastExchange;
    }

    public void setEncounteredBroadcastExchange(boolean encounteredBroadcastExchange) {
      this.encounteredBroadcastExchange = encounteredBroadcastExchange;
    }

    public boolean isFromBuildSide() {
      return fromBuildSide;
    }

    public void setFromBuildSide(boolean fromBuildSide) {
      this.fromBuildSide = fromBuildSide;
    }

    public RuntimeFilterDef getRuntimeFilterDef() {
      return runtimeFilterDef;
    }

    public void setRuntimeFilterDef(RuntimeFilterDef runtimeFilterDef) {
      this.runtimeFilterDef = runtimeFilterDef;
    }
  }
}
