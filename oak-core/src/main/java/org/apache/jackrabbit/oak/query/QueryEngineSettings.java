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

/**
 * Settings of the query engine.
 */
public class QueryEngineSettings implements QueryEngineSettingsMBean {
    
    /**
     * the flag used to turn on/off the optimisations on top of the {@link Query} object.
     * {@code -Doak.query.sql2optimisation}
     */
    public static final String SQL2_OPTIMISATION_FLAG = "oak.query.sql2optimisation";
    
    private static final int DEFAULT_QUERY_LIMIT_IN_MEMORY = 
            Integer.getInteger("oak.queryLimitInMemory", Integer.MAX_VALUE);
    
    private static final int DEFAULT_QUERY_LIMIT_READS = 
            Integer.getInteger("oak.queryLimitReads", Integer.MAX_VALUE);
    
    private static final boolean DEFAULT_FAIL_TRAVERSAL =
            Boolean.getBoolean("oak.queryFailTraversal");
    
    private static final boolean DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX = 
            Boolean.getBoolean("oak.queryFullTextComparisonWithoutIndex");
    
    private long limitInMemory = DEFAULT_QUERY_LIMIT_IN_MEMORY;
    
    private long limitReads = DEFAULT_QUERY_LIMIT_READS;
    
    private boolean failTraversal = DEFAULT_FAIL_TRAVERSAL;
    
    private boolean fullTextComparisonWithoutIndex = 
            DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX;
    
    private boolean sql2Optimisation = Boolean.parseBoolean(System.getProperty(SQL2_OPTIMISATION_FLAG, "true"));
    
    public QueryEngineSettings() {
    }
    
    @Override
    public long getLimitInMemory() {
        return limitInMemory;
    }
    
    @Override
    public void setLimitInMemory(long limitInMemory) {
        this.limitInMemory = limitInMemory;
    }
    
    @Override
    public long getLimitReads() {
        return limitReads;
    }
    
    @Override
    public void setLimitReads(long limitReads) {
        this.limitReads = limitReads;
    }
    
    @Override
    public boolean getFailTraversal() {
        return failTraversal;
    }

    @Override
    public void setFailTraversal(boolean failTraversal) {
        this.failTraversal = failTraversal;
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
