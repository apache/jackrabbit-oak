/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import java.util.List;
import java.util.function.Predicate;

public class IncrementalIndexStoreMetadata {

    private String beforeCheckpoint;
    private String afterCheckpoint;
    private String storeType;
    private String strategy;

    private List<String> preferredPaths;
    private Predicate<String> pathPredicate;

    public IncrementalIndexStoreMetadata(IncrementalIndexStoreSortStrategy indexStoreSortStrategy) {
        this.beforeCheckpoint = indexStoreSortStrategy.getBeforeCheckpoint();
        this.afterCheckpoint = indexStoreSortStrategy.getAfterCheckpoint();
        this.storeType = indexStoreSortStrategy.getStoreType();
        this.strategy = indexStoreSortStrategy.getStrategyName();
        this.preferredPaths = indexStoreSortStrategy.getPreferredPaths();
        this.pathPredicate = indexStoreSortStrategy.getPathPredicate();
    }

    public String getBeforeCheckpoint() {
        return beforeCheckpoint;
    }

    public String getAfterCheckpoint() {
        return afterCheckpoint;
    }

    public String getStrategy() {
        return strategy;
    }

    public String getStoreType() {
        return storeType;
    }


    public List<String> getPreferredPaths() {
        return preferredPaths;
    }

    public Predicate<String> getPathPredicate() {
        return pathPredicate;
    }
}
