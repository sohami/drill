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
package org.apache.drill.exec.resourcemgr.selectionpolicy;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.resourcemgr.ResourcePool;
import org.apache.drill.exec.resourcemgr.exception.QueueSelectionException;

import java.util.List;

public class DefaultQueueSelection implements QueueSelectionPolicy {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultQueueSelection.class);

  @Override
  public ResourcePool selectQueue(List<ResourcePool> allPools, QueryContext queryContext) throws QueueSelectionException {
    for (ResourcePool pool : allPools) {
      if (pool.isDefaultPool()) {
        return pool;
      }
    }

    throw new QueueSelectionException(String.format("There is no default pool to select from list of pools provided " +
      "for the query. Details[Pools list: {}, Query: {}", allPools, queryContext.getQueryId()));
  }
}