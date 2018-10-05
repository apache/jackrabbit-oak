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
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class QueryEngineSettingsServiceTest {
    private static final String UNSET = "UNSET";
    @Rule
    public final OsgiContext context = new OsgiContext();

    private final Set<String> sysPropNames = ImmutableSet.of(
            QueryEngineSettings.OAK_QUERY_LIMIT_IN_MEMORY,
            QueryEngineSettings.OAK_QUERY_LIMIT_READS,
            QueryEngineSettings.OAK_QUERY_FAIL_TRAVERSAL
    );

    private QueryEngineSettingsService settingsService = new QueryEngineSettingsService();
    private Map<String, String> sysPropValues = Maps.newHashMap();

    @Before
    public void setUp(){
        collectExistingValues();
    }

    @After
    public void resetSysProps(){
        for (Map.Entry<String, String> e : sysPropValues.entrySet()){
            String value = e.getValue();
            if (UNSET.equals(value)){
                System.clearProperty(e.getKey());
            } else {
                System.setProperty(e.getKey(), e.getValue());
            }
        }
    }

    @Test
    public void osgiConfig() throws Exception{
        QueryEngineSettings settings = new QueryEngineSettings();
        context.registerService(QueryEngineSettingsMBean.class, settings);

        Map<String, Object> config = Maps.newHashMap();
        config.put(QueryEngineSettingsService.QUERY_LIMIT_READS, 100);
        config.put(QueryEngineSettingsService.QUERY_LIMIT_IN_MEMORY, 142);
        config.put(QueryEngineSettingsService.QUERY_FAIL_TRAVERSAL, true);
        config.put(QueryEngineSettingsService.QUERY_FAST_QUERY_SIZE, true);

        context.registerInjectActivateService(settingsService, config);
        assertEquals(100, settings.getLimitReads());
        assertEquals(142, settings.getLimitInMemory());
        assertTrue(settings.getFailTraversal());
        assertTrue(settings.isFastQuerySize());
    }

    @Test
    public void sysPropSupercedes() throws Exception{
        System.setProperty(QueryEngineSettings.OAK_QUERY_LIMIT_IN_MEMORY, String.valueOf(QueryEngineSettings
                .DEFAULT_QUERY_LIMIT_IN_MEMORY));
        System.setProperty(QueryEngineSettings.OAK_QUERY_LIMIT_READS, String.valueOf(QueryEngineSettings
                .DEFAULT_QUERY_LIMIT_READS));
        System.setProperty(QueryEngineSettings.OAK_QUERY_FAIL_TRAVERSAL, "false");

        QueryEngineSettings settings = new QueryEngineSettings();
        context.registerService(QueryEngineSettingsMBean.class, settings);

        Map<String, Object> config = Maps.newHashMap();
        config.put(QueryEngineSettingsService.QUERY_LIMIT_READS, 100);
        config.put(QueryEngineSettingsService.QUERY_LIMIT_IN_MEMORY, 142);
        config.put(QueryEngineSettingsService.QUERY_FAIL_TRAVERSAL, true);

        context.registerInjectActivateService(settingsService, config);
        assertEquals(QueryEngineSettings
                .DEFAULT_QUERY_LIMIT_READS, settings.getLimitReads());
        assertEquals(QueryEngineSettings
                .DEFAULT_QUERY_LIMIT_IN_MEMORY, settings.getLimitInMemory());
        assertFalse(settings.getFailTraversal());
    }

    private void collectExistingValues() {
        for(String key : sysPropNames){
            String value = System.getProperty(key);
            if (value != null){
                sysPropValues.put(key, value);
            } else {
                sysPropValues.put(key, UNSET);
            }
        }
    }

}