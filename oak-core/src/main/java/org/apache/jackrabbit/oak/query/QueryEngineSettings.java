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
import org.apache.jackrabbit.oak.query.stats.QueryStatsMBean;
import org.apache.jackrabbit.oak.query.stats.QueryStatsMBeanImpl;
import org.apache.jackrabbit.oak.query.stats.QueryStatsReporter;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;

/**
 * Settings of the query engine.
 */
public class QueryEngineSettings implements QueryEngineSettingsMBean, QueryLimits {
    
    /**
     * the flag used to turn on/off the optimisations on top of the {@code org.apache.jackrabbit.oak.query.Query} object.
     * {@code -Doak.query.sql2optimisation}
     */
    public static final String SQL2_OPTIMISATION_FLAG = "oak.query.sql2optimisation";
    
    public static final String SQL2_OPTIMISATION_FLAG_2 = "oak.query.sql2optimisation2";
    
    public static final boolean SQL2_OPTIMIZATION_2 = 
            Boolean.parseBoolean(System.getProperty(SQL2_OPTIMISATION_FLAG_2, "true"));

    public static final String OAK_QUERY_LIMIT_IN_MEMORY = "oak.queryLimitInMemory";

    // should be the same as QueryEngineSettingsService.DEFAULT_QUERY_LIMIT_IN_MEMORY
    public static final int DEFAULT_QUERY_LIMIT_IN_MEMORY =
            Integer.getInteger(OAK_QUERY_LIMIT_IN_MEMORY, 500000);

    public static final String OAK_QUERY_LIMIT_READS = "oak.queryLimitReads";

    // should be the same as QueryEngineSettingsService.DEFAULT_QUERY_LIMIT_READS
    public static final int DEFAULT_QUERY_LIMIT_READS =
            Integer.getInteger(OAK_QUERY_LIMIT_READS, 100000);

    public static final String OAK_QUERY_FAIL_TRAVERSAL = "oak.queryFailTraversal";
    private static final boolean DEFAULT_FAIL_TRAVERSAL =
            Boolean.getBoolean(OAK_QUERY_FAIL_TRAVERSAL);

    private static final boolean DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX =
            Boolean.getBoolean("oak.queryFullTextComparisonWithoutIndex");
    
    private long limitInMemory = DEFAULT_QUERY_LIMIT_IN_MEMORY;
    
    private long limitReads = DEFAULT_QUERY_LIMIT_READS;
    
    private boolean failTraversal = DEFAULT_FAIL_TRAVERSAL;
    
    private boolean fullTextComparisonWithoutIndex = 
            DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX;
    
    private boolean sql2Optimisation = 
            Boolean.parseBoolean(System.getProperty(SQL2_OPTIMISATION_FLAG, "true"));

    private static final String OAK_FAST_QUERY_SIZE = "oak.fastQuerySize";
    public static final boolean DEFAULT_FAST_QUERY_SIZE = Boolean.getBoolean(OAK_FAST_QUERY_SIZE);
    private boolean fastQuerySize = DEFAULT_FAST_QUERY_SIZE;

    private QueryStatsMBeanImpl queryStats = new QueryStatsMBeanImpl(this);

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

    @Override
    public boolean isFastQuerySize() {
        return fastQuerySize;
    }

    @Override
    public void setFastQuerySize(boolean fastQuerySize) {
        this.fastQuerySize = fastQuerySize;
        System.setProperty(OAK_FAST_QUERY_SIZE, String.valueOf(fastQuerySize));
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

    public QueryStatsMBean getQueryStats() {
        return queryStats;
    }
    
    public QueryStatsReporter getQueryStatsReporter() {
        return queryStats;
    }

    @Override
    public String toString() {
        return "QueryEngineSettings{" +
                "limitInMemory=" + limitInMemory +
                ", limitReads=" + limitReads +
                ", failTraversal=" + failTraversal +
                ", fullTextComparisonWithoutIndex=" + fullTextComparisonWithoutIndex +
                ", sql2Optimisation=" + sql2Optimisation +
                ", fastQuerySize=" + fastQuerySize +
                '}';
    }
    
}