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

import org.apache.drill.exec.rpc.NamedThreadFactory;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This sink receives the RuntimeFilters from the netty thread,
 * aggregates them in an async thread, supplies the aggregated
 * one to the fragment running thread.
 */
public class RuntimeFilterSink implements AutoCloseable {

  private AtomicInteger currentBookId = new AtomicInteger(0);

  private int staleBookId = 0;

  private RuntimeFilterWritable aggregated = null;

  private Queue<RuntimeFilterWritable> rfQueue = new ConcurrentLinkedQueue<>();

  private AtomicBoolean running = new AtomicBoolean(true);

  public void aggregate(RuntimeFilterWritable runtimeFilterWritable) {
    rfQueue.add(runtimeFilterWritable);
    if (currentBookId.get() == 0) {
      AsyncAggregateWorker asyncAggregateWorker = new AsyncAggregateWorker();
      Thread asyncAggregateThread = new NamedThreadFactory("RFAggregating-").newThread(asyncAggregateWorker);
      asyncAggregateThread.start();
    }
  }

  public RuntimeFilterWritable fetchLatestAggregatedOne() {
    return aggregated;
  }

  /**
   * whether there's a fresh aggregated RuntimeFilter
   * @return
   */
  public boolean hasFreshOne() {
    if (currentBookId.get() > staleBookId) {
      staleBookId = currentBookId.get();
      return true;
    }
    return false;
  }

  /**
   * whether there's a usable RuntimeFilter.
   * @return
   */
  public boolean containOne() {
    return aggregated != null;
  }

  @Override
  public void close() throws Exception {
    running.compareAndSet(true, false);
    if (containOne()) {
      aggregated.close();
    }
    RuntimeFilterWritable toClear;
    while ((toClear = rfQueue.poll()) != null) {
      toClear.close();
    }
  }

  class AsyncAggregateWorker implements Runnable {

    @Override
    public void run() {
      while (running.get()) {
        RuntimeFilterWritable toAggregate = rfQueue.poll();
        if (toAggregate != null) {
          if (aggregated != null) {
            aggregated.aggregate(toAggregate);
            currentBookId.incrementAndGet();
          } else {
            aggregated = toAggregate;
          }
        }
      }
    }
  }
}


