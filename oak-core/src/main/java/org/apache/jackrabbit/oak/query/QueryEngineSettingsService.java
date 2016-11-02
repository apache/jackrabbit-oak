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

import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
        policy = ConfigurationPolicy.REQUIRE,
        metatype = true,
        label = "Apache Jackrabbit Query Engine Settings Service",
        description = "Various settings exposed by Oak QueryEngine. Note that settings done by system property " +
                "supersedes the one defined via OSGi config"
)
public class QueryEngineSettingsService {
    private static final int DEFAULT_QUERY_LIMIT_IN_MEMORY = Integer.MAX_VALUE;
    @Property(
            intValue = DEFAULT_QUERY_LIMIT_IN_MEMORY,
            label = "In memory limit",
            description = "Maximum number of entries that can be held in memory while evaluating any query"
    )
    static final String QUERY_LIMIT_IN_MEMORY = "queryLimitInMemory";
    private static final int DEFAULT_QUERY_LIMIT_READS = Integer.MAX_VALUE;
    @Property(
            intValue = DEFAULT_QUERY_LIMIT_READS,
            label = "In memory read limit",
            description = "Maximum number of results which can be read by any query"
    )
    static final String QUERY_LIMIT_READS = "queryLimitReads";
    private static final boolean DEFAULT_QUERY_FAIL_TRAVERSAL = false;
    @Property(
            boolValue = DEFAULT_QUERY_FAIL_TRAVERSAL,
            label = "Fail traversal",
            description = "If enabled any query execution which results in traversal would fail."
    )
    static final String QUERY_FAIL_TRAVERSAL = "queryFailTraversal";

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference
    private QueryEngineSettingsMBean queryEngineSettings;

    @Activate
    private void activate(BundleContext context, Map<String, Object> config) {
        if (System.getProperty(QueryEngineSettings.OAK_QUERY_LIMIT_IN_MEMORY) == null) {
            int queryLimitInMemory = PropertiesUtil.toInteger(config.get(QUERY_LIMIT_IN_MEMORY),
                    DEFAULT_QUERY_LIMIT_IN_MEMORY);
            queryEngineSettings.setLimitInMemory(queryLimitInMemory);
        } else {
            logMsg(QUERY_LIMIT_IN_MEMORY, QueryEngineSettings.OAK_QUERY_LIMIT_IN_MEMORY);
        }

        if (System.getProperty(QueryEngineSettings.OAK_QUERY_LIMIT_READS) == null) {
            int queryLimitReads = PropertiesUtil.toInteger(config.get(QUERY_LIMIT_READS),
                    DEFAULT_QUERY_LIMIT_READS);
            queryEngineSettings.setLimitReads(queryLimitReads);
        } else {
            logMsg(QUERY_LIMIT_IN_MEMORY, QueryEngineSettings.OAK_QUERY_LIMIT_READS);
        }

        if (System.getProperty(QueryEngineSettings.OAK_QUERY_FAIL_TRAVERSAL) == null) {
            boolean failTraversal = PropertiesUtil.toBoolean(config.get(QUERY_FAIL_TRAVERSAL),
                    DEFAULT_QUERY_FAIL_TRAVERSAL);
            queryEngineSettings.setFailTraversal(failTraversal);
        } else {
            logMsg(QUERY_FAIL_TRAVERSAL, QueryEngineSettings.OAK_QUERY_FAIL_TRAVERSAL);
        }

        log.info("Initialize QueryEngine settings {}", queryEngineSettings);
    }

    private void logMsg(String key, String sysPropKey) {
        log.info("For {} using value {} defined via system property {}", key,
                System.getProperty(sysPropKey), sysPropKey);
    }
}
