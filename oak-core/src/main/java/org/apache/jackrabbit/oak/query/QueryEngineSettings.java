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
package org.apache.jackrabbit.oak.query;

/**
 * Settings of the query engine.
 */
public class QueryEngineSettings {
    
    private static final int DEFAULT_QUERY_LIMIT_IN_MEMORY = 
            Integer.getInteger("oak.queryLimitInMemory", 10000);
    
    private static final int DEFAULT_QUERY_LIMIT_READS = 
            Integer.getInteger("oak.queryLimitReads", 100000);    
    
    private long limitInMemory = DEFAULT_QUERY_LIMIT_IN_MEMORY;
    
    private long limitReads = DEFAULT_QUERY_LIMIT_READS;
    
    /**
     * Get the limit on how many nodes a query may read at most into memory, for
     * "order by" and "distinct" queries. If this limit is exceeded, the query
     * throws an exception.
     * 
     * @return the limit
     */
    public long getLimitInMemory() {
        return limitInMemory;
    }
    
    /**
     * Change the limit.
     * 
     * @param limitInMemory the new limit
     */
    public void setLimitInMemory(long limitInMemory) {
        this.limitInMemory = limitInMemory;
    }
    
    /**
     * Get the limit on how many nodes a query may read at most (raw read
     * operations, including skipped nodes). If this limit is exceeded, the
     * query throws an exception.
     * 
     * @return the limit
     */
    public long getLimitReads() {
        return limitReads;
    }
    
    /**
     * Change the limit.
     * 
     * @param limitReads the new limit
     */
    public void setLimitReads(long limitReads) {
        this.limitReads = limitReads;
    }
    
}
