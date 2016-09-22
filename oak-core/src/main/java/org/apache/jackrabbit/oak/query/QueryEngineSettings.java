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

import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;

/**
 * Settings of the query engine.
 */
public class QueryEngineSettings extends AnnotatedStandardMBean implements QueryEngineSettingsMBean {
    
    /**
     * the flag used to turn on/off the optimisations on top of the {@link Query} object.
     * {@code -Doak.query.sql2optimisation}
     */
    public static final String SQL2_OPTIMISATION_FLAG = "oak.query.sql2optimisation";
    
    private static final int DEFAULT_QUERY_LIMIT_IN_MEMORY = 
            Integer.getInteger("oak.queryLimitInMemory", Integer.MAX_VALUE);
    
    private static final int DEFAULT_QUERY_LIMIT_READS = 
            Integer.getInteger("oak.queryLimitReads", Integer.MAX_VALUE);    
    
    private static final boolean DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX = 
            Boolean.getBoolean("oak.queryFullTextComparisonWithoutIndex");
    
    private long limitInMemory = DEFAULT_QUERY_LIMIT_IN_MEMORY;
    
    private long limitReads = DEFAULT_QUERY_LIMIT_READS;
    
    private boolean fullTextComparisonWithoutIndex = 
            DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX;
    
    private boolean sql2Optimisation = Boolean.parseBoolean(System.getProperty(SQL2_OPTIMISATION_FLAG, "true"));

    /**
     * Create a new query engine settings object. Creating the object is
     * relatively slow, and at runtime, as few such objects as possible should
     * be created (ideally, only one per Oak instance). Creating new instances
     * also means they can not be configured using JMX, as one would expect.
     */
    public QueryEngineSettings() {
        super(QueryEngineSettingsMBean.class);
    }

    /**
     * Get the limit on how many nodes a query may read at most into memory, for
     * "order by" and "distinct" queries. If this limit is exceeded, the query
     * throws an exception.
     * 
     * @return the limit
     */
    @Override
    public long getLimitInMemory() {
        return limitInMemory;
    }
    
    /**
     * Change the limit.
     * 
     * @param limitInMemory the new limit
     */
    @Override
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
    @Override
    public long getLimitReads() {
        return limitReads;
    }
    
    /**
     * Change the limit.
     * 
     * @param limitReads the new limit
     */
    @Override
    public void setLimitReads(long limitReads) {
        this.limitReads = limitReads;
    }
    
    public void setFullTextComparisonWithoutIndex(boolean fullTextComparisonWithoutIndex) {
        this.fullTextComparisonWithoutIndex = fullTextComparisonWithoutIndex;
    }
    
    public boolean getFullTextComparisonWithoutIndex() {
        return fullTextComparisonWithoutIndex;
    }
    
    public boolean isSql2Optimisation() {
        return sql2Optimisation;
    }
}
