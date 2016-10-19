/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.api.jmx;

public interface QueryEngineSettingsMBean {
    String TYPE = "QueryEngineSettings";
    
    /**
     * Get the limit on how many nodes a query may read at most into memory, for
     * "order by" and "distinct" queries. If this limit is exceeded, the query
     * throws an exception.
     * 
     * @return the limit
     */
    long getLimitInMemory();
    
    /**
     * Change the limit.
     * 
     * @param limitInMemory the new limit
     */
    void setLimitInMemory(long limitInMemory);
    
    /**
     * Get the limit on how many nodes a query may read at most (raw read
     * operations, including skipped nodes). If this limit is exceeded, the
     * query throws an exception.
     * 
     * @return the limit
     */
    long getLimitReads();
    
    /**
     * Change the limit.
     * 
     * @param limitReads the new limit
     */
    void setLimitReads(long limitReads);
    
    /**
     * Whether queries that don't use an index will fail (throw an exception).
     * The default is false.
     * 
     * @return true if they fail
     */
    boolean getFailTraversal();

    /**
     * Set whether queries that don't use an index will fail (throw an exception).
     * 
     * @param failTraversal the new value for this setting
     */
    void setFailTraversal(boolean failTraversal);

}
