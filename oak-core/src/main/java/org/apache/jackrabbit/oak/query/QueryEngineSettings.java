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

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.jackrabbit.oak.query.stats.QueryStatsMBean;
import org.apache.jackrabbit.oak.query.stats.QueryStatsMBeanImpl;
import org.apache.jackrabbit.oak.query.stats.QueryStatsReporter;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    public static final long DEFAULT_QUERY_LIMIT_IN_MEMORY =
            Long.getLong(OAK_QUERY_LIMIT_IN_MEMORY, 500000);

    public static final String OAK_QUERY_LIMIT_READS = "oak.queryLimitReads";

    // should be the same as QueryEngineSettingsService.DEFAULT_QUERY_LIMIT_READS
    public static final long DEFAULT_QUERY_LIMIT_READS =
            Long.getLong(OAK_QUERY_LIMIT_READS, 100000);

    public static final String OAK_QUERY_PREFETCH_COUNT = "oak.prefetchCount";

    public static final String FT_NAME_PREFETCH_FOR_QUERIES = "FT_OAK-10490";

    public static final String FT_NAME_IMPROVED_IS_NULL_COST = "FT_OAK-10532";

    public static final String FT_OPTIMIZE_IN_RESTRICTIONS_FOR_FUNCTIONS = "FT_OAK-11214";

    public static final int DEFAULT_PREFETCH_COUNT = Integer.getInteger(OAK_QUERY_PREFETCH_COUNT, -1);

    public static final String OAK_QUERY_FAIL_TRAVERSAL = "oak.queryFailTraversal";
    private static final boolean DEFAULT_FAIL_TRAVERSAL =
            Boolean.getBoolean(OAK_QUERY_FAIL_TRAVERSAL);

    private static final boolean DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX =
            Boolean.getBoolean("oak.queryFullTextComparisonWithoutIndex");
    
    private long limitInMemory = DEFAULT_QUERY_LIMIT_IN_MEMORY;
    
    private long limitReads = DEFAULT_QUERY_LIMIT_READS;

    private int prefetchCount = DEFAULT_PREFETCH_COUNT;

    private boolean failTraversal = DEFAULT_FAIL_TRAVERSAL;
    
    private boolean fullTextComparisonWithoutIndex = 
            DEFAULT_FULL_TEXT_COMPARISON_WITHOUT_INDEX;
    
    private boolean sql2Optimisation = 
            Boolean.parseBoolean(System.getProperty(SQL2_OPTIMISATION_FLAG, "true"));

    private static final String OAK_FAST_QUERY_SIZE = "oak.fastQuerySize";
    public static final boolean DEFAULT_FAST_QUERY_SIZE = Boolean.getBoolean(OAK_FAST_QUERY_SIZE);
    private boolean fastQuerySize = DEFAULT_FAST_QUERY_SIZE;

    private StrictPathRestriction strictPathRestriction = StrictPathRestriction.DISABLE;

    private final QueryStatsMBeanImpl queryStats = new QueryStatsMBeanImpl(this);

    /**
     * StatisticsProvider used to record query side metrics.
     */
    private final StatisticsProvider statisticsProvider;

    private final QueryValidator queryValidator = new QueryValidator();

    private String[] classNamesIgnoredInCallTrace = new String[] {};


    private static final String OAK_QUERY_LENGTH_WARN_LIMIT = "oak.query.length.warn.limit";
    private static final String OAK_QUERY_LENGTH_ERROR_LIMIT = "oak.query.length.error.limit";

    private final long queryLengthWarnLimit = Long.getLong(OAK_QUERY_LENGTH_WARN_LIMIT, 1024 * 1024); // 1 MB
    private final long queryLengthErrorLimit = Long.getLong(OAK_QUERY_LENGTH_ERROR_LIMIT, 100 * 1024 * 1024); //100MB

    private Feature prefetchFeature;
    private Feature improvedIsNullCostFeature;
    private Feature optimizeInRestrictionsForFunctions;

    private String autoOptionsMappingJson = "{}";
    private QueryOptions.AutomaticQueryOptionsMapping autoOptionsMapping = new QueryOptions.AutomaticQueryOptionsMapping(autoOptionsMappingJson);

    public long getQueryLengthWarnLimit() {
        return queryLengthWarnLimit;
    }

    public long getQueryLengthErrorLimit() {
        return queryLengthErrorLimit;
    }

    public QueryEngineSettings() {
        statisticsProvider = StatisticsProvider.NOOP;
    }

    public QueryEngineSettings(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    public void setPrefetchFeature(@Nullable Feature prefetch) {
        this.prefetchFeature = prefetch;
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
    public void setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }

    @Override
    public int getPrefetchCount() {
        if (prefetchCount == -1) {
            return prefetchFeature != null && prefetchFeature.isEnabled() ?
                    20 : 0;
        }
        return prefetchCount;
    }

    @Override
    public void setAutoOptionsMappingJson(String json) {
        autoOptionsMappingJson = json;
        autoOptionsMapping = new QueryOptions.AutomaticQueryOptionsMapping(json);
    }

    @Override
    public String getAutoOptionsMappingJson() {
        return autoOptionsMappingJson;
    }

    public QueryOptions.AutomaticQueryOptionsMapping getAutomaticQueryOptions() {
        return autoOptionsMapping;
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

    public void setImprovedIsNullCostFeature(@Nullable Feature feature) {
        this.improvedIsNullCostFeature = feature;
    }

    @Override
    public boolean getImprovedIsNullCost() {
        // enabled if the feature toggle is not used
        return improvedIsNullCostFeature == null || improvedIsNullCostFeature.isEnabled();
    }

    public void setOptimizeInRestrictionsForFunctions(@Nullable Feature feature) {
        this.optimizeInRestrictionsForFunctions = feature;
    }

    @Override
    public boolean getOptimizeInRestrictionsForFunctions() {
        // enabled if the feature toggle is not used
        return optimizeInRestrictionsForFunctions == null || optimizeInRestrictionsForFunctions.isEnabled();
    }

    public String getStrictPathRestriction() {
        return strictPathRestriction.name();
    }

    public void setStrictPathRestriction(String strictPathRestriction) {
        this.strictPathRestriction = StrictPathRestriction.stringToEnum(strictPathRestriction);
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

    public StatisticsProvider getStatisticsProvider() {
        return statisticsProvider;
    }

    @Override
    public void setQueryValidatorPattern(String key, String pattern, String comment, boolean failQuery) {
        queryValidator.setPattern(key, pattern, comment, failQuery);
    }

    @Override
    public String getQueryValidatorJson() {
        return queryValidator.getJson();
    }

    public QueryValidator getQueryValidator() {
        return queryValidator;
    }
    
    public void setIgnoredClassNamesInCallTrace(@NotNull String[] packageNames) {
        classNamesIgnoredInCallTrace = packageNames;
    }

    public @NotNull String[] getIgnoredClassNamesInCallTrace() {
        return classNamesIgnoredInCallTrace;
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
                ", prefetchCount=" + prefetchCount +
                ", classNamesIgnoredInCallTrace=" + Arrays.toString(classNamesIgnoredInCallTrace) +
                '}';
    }

}
