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
package org.apache.jackrabbit.oak.query.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class QueryStatsTest {

    private Properties systemProperties;
    private int maxQuerySize = 2000;

    @Before
    public void setup() {
        systemProperties =(Properties) System.getProperties().clone();
        System.setProperty("oak.query.maxQuerySize", "" + maxQuerySize);
    }

    @After
    public void teardown() {
        System.setProperties(systemProperties);
    }

    @Test
    public void testEviction() throws InterruptedException {
        QueryStatsMBeanImpl bean = new QueryStatsMBeanImpl(new QueryEngineSettings());
        for (int i = 0; i < 10010; i++) {
            bean.getQueryExecution("old" + i, "");
            if (i % 100 == 0) {
                Thread.sleep(1);
            }
        }
        assertEquals(1, bean.getEvictionCount());
        // remain around 5000
        
        Thread.sleep(5);
        for (int i = 0; i < 10; i++) {
            bean.getQueryExecution("slow" + i, "").execute(10000);
        }
        Thread.sleep(5);
        
        assertEquals(1, bean.getEvictionCount());
        for (int i = 0; i < 10010; i++) {
            bean.getQueryExecution("new" + i, "");
            if (i % 100 == 0) {
                Thread.sleep(1);
            }
        }
        assertEquals(3, bean.getEvictionCount());
        String json = bean.asJson();
        for (int i = 0; i < 10; i++) {
            assertTrue(json.indexOf("slow" + i) >= 0);
        }
        assertTrue(json.indexOf("old") < 0);
    }

    @Test
    public void testTruncation() throws InterruptedException {
        QueryEngineSettings qes = new QueryEngineSettings();
        QueryStatsMBeanImpl bean = new QueryStatsMBeanImpl(qes);
        String generatedString = RandomStringUtils.random(5000, true, false);
        bean.getQueryExecution(generatedString, "");

        String data = bean.getPopularQueries().values().iterator().next().toString();
        assertFalse(data.contains(generatedString));
        String statLog = new StringBuilder()
                .append("Truncated query: ")
                .append(generatedString.substring(0, maxQuerySize >> 1))
                .append(" ...... ")
                .append(generatedString.substring(generatedString.length() - (maxQuerySize >> 1)))
                .toString();
        assertTrue(data.contains(statLog));
    }
}
