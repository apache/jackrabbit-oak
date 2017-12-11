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
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = QueryEngineSettingsService.Configuration.class)
public class QueryEngineSettingsService {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Query Engine Settings Service",
            description = "Various settings exposed by Oak QueryEngine. Note that settings done by system property " +
                    "supersedes the one defined via OSGi config"
    )
    @interface Configuration {

        @AttributeDefinition(
                name = "In memory limit",
                description = "Maximum number of entries that can be held in memory while evaluating any query"
        )
        int queryLimitInMemory() default DEFAULT_QUERY_LIMIT_IN_MEMORY;

        @AttributeDefinition(
                name = "In memory read limit",
                description = "Maximum number of results which can be read by any query"
        )
        int queryLimitReads() default DEFAULT_QUERY_LIMIT_READS;

        @AttributeDefinition(
                name = "Fail traversal",
                description = "If enabled any query execution which results in traversal would fail."
        )
        boolean queryFailTraversal() default DEFAULT_QUERY_FAIL_TRAVERSAL;

        @AttributeDefinition(
                name = "Fast result size",
                description = "Whether the query result size (QueryResult.getSize()) should return an estimation for queries that return many nodes. " +
                        "The estimate will be larger or equal the actual result size, as it includes unindexed properties and nodes that are not accessible. " +
                        "If disabled, for such cases -1 is returned. " +
                        "Note: even if enabled, getSize may still return -1 if the index used does not support the feature." 
        )
        boolean fastQuerySize() default false;
    }

    // should be the same as QueryEngineSettings.DEFAULT_QUERY_LIMIT_IN_MEMORY
    private static final int DEFAULT_QUERY_LIMIT_IN_MEMORY = 500000;
    static final String QUERY_LIMIT_IN_MEMORY = "queryLimitInMemory";

    // should be the same as QueryEngineSettings.DEFAULT_QUERY_LIMIT_READS
    private static final int DEFAULT_QUERY_LIMIT_READS = 100000;
    static final String QUERY_LIMIT_READS = "queryLimitReads";

    private static final boolean DEFAULT_QUERY_FAIL_TRAVERSAL = false;
    static final String QUERY_FAIL_TRAVERSAL = "queryFailTraversal";
    
    static final String QUERY_FAST_QUERY_SIZE = "fastQuerySize";

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference
    private QueryEngineSettingsMBean queryEngineSettings;

    @Activate
    private void activate(BundleContext context, Configuration config) {
        if (System.getProperty(QueryEngineSettings.OAK_QUERY_LIMIT_IN_MEMORY) == null) {
            int queryLimitInMemory = config.queryLimitInMemory();
            queryEngineSettings.setLimitInMemory(queryLimitInMemory);
        } else {
            logMsg(QUERY_LIMIT_IN_MEMORY, QueryEngineSettings.OAK_QUERY_LIMIT_IN_MEMORY);
        }

        if (System.getProperty(QueryEngineSettings.OAK_QUERY_LIMIT_READS) == null) {
            int queryLimitReads = config.queryLimitReads();
            queryEngineSettings.setLimitReads(queryLimitReads);
        } else {
            logMsg(QUERY_LIMIT_IN_MEMORY, QueryEngineSettings.OAK_QUERY_LIMIT_READS);
        }

        if (System.getProperty(QueryEngineSettings.OAK_QUERY_FAIL_TRAVERSAL) == null) {
            boolean failTraversal = config.queryFailTraversal();
            queryEngineSettings.setFailTraversal(failTraversal);
        } else {
            logMsg(QUERY_FAIL_TRAVERSAL, QueryEngineSettings.OAK_QUERY_FAIL_TRAVERSAL);
        }

        boolean fastQuerySizeSysProp = QueryEngineSettings.DEFAULT_FAST_QUERY_SIZE;
        boolean fastQuerySizeFromConfig = config.fastQuerySize();
        queryEngineSettings.setFastQuerySize(fastQuerySizeFromConfig || fastQuerySizeSysProp);

        log.info("Initialize QueryEngine settings {}", queryEngineSettings);
    }

    private void logMsg(String key, String sysPropKey) {
        log.info("For {} using value {} defined via system property {}", key,
                System.getProperty(sysPropKey), sysPropKey);
    }

}
