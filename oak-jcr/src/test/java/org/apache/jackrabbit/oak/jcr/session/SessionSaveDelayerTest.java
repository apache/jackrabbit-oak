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
package org.apache.jackrabbit.oak.jcr.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for {@link SessionSaveDelayer}.
 */
public class SessionSaveDelayerTest {

    private static final String ENABLED_PROP_NAME = "oak.sessionSaveDelayer";
    private static final String CONFIG_PROP_NAME = "oak.sessionSaveDelayerConfig";
    
    @Rule 
    public TemporarySystemProperty temp;
  
    private Whiteboard whiteboard;
    private SessionSaveDelayer delayer;

    @Before
    public void setUp() {
        whiteboard = new DefaultWhiteboard();
        delayer = new SessionSaveDelayer(whiteboard);
    }

    @After
    public void tearDown() {
        if (delayer != null) {
            delayer.close();
        }
    }

    @Test
    public void testGetCurrentStackTrace() {
        String stackTrace = SessionSaveDelayerConfig.getCurrentStackTrace();
        assertNotNull(stackTrace);
        assertTrue(stackTrace.contains("testGetCurrentStackTrace"));
        assertTrue(stackTrace.contains("at "));
    }

    @Test
    public void testDelayIfNeededDisabled() {
        System.clearProperty(ENABLED_PROP_NAME);
        delayer = new SessionSaveDelayer(whiteboard);        
        long delay = delayer.delayIfNeeded(null);
        assertEquals(0, delay);
    }

    @Test
    public void testDelayIfNeededEnabledViaSystemProperty() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.clearProperty(CONFIG_PROP_NAME);
        delayer = new SessionSaveDelayer(whiteboard);
        long delay = delayer.delayIfNeeded(null);
        assertEquals(0, delay);
    }

    @Test
    public void testDelayIfNeededWithSystemPropertyConfig() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        delayer = new SessionSaveDelayer(whiteboard);
        long delay = delayer.delayIfNeeded(null);
        assertEquals(100_000L, delay);
    }

    @Test
    public void testDelayIfNeededWithJMXConfig() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        RepositoryManagementMBean mbean = mock(RepositoryManagementMBean.class);
        when(mbean.getSessionSaveDelayerConfig()).thenReturn("{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.02,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        whiteboard.register(RepositoryManagementMBean.class, mbean, Map.of());
        delayer = new SessionSaveDelayer(whiteboard);
        long delay = delayer.delayIfNeeded(null);
        assertEquals(20_000L, delay);
    }

    @Test
    public void testDelayIfNeededWithNonMatchingThreadName() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 5.0,\n" +
                "      \"threadNameRegex\": \"non-matching-pattern\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        delayer = new SessionSaveDelayer(whiteboard);
        long delay = delayer.delayIfNeeded(null);
        assertEquals(0, delay);
    }

    @Test
    public void testDelayIfNeededWithInvalidConfig() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{ invalid json }");
        delayer = new SessionSaveDelayer(whiteboard);
        long delay = delayer.delayIfNeeded(null);
        assertEquals(0, delay);
    }

    @Test
    public void testDelayIfNeededConfigCaching() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        delayer = new SessionSaveDelayer(whiteboard);
        long delay1 = delayer.delayIfNeeded(null);
        assertEquals(100_000L, delay1);
        long delay2 = delayer.delayIfNeeded(null);
        assertEquals(100_000L, delay2);
    }

    @Test
    public void testDelayIfNeededEmptyConfig() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "");
        delayer = new SessionSaveDelayer(whiteboard);
                long delay = delayer.delayIfNeeded(null);
        assertEquals(0, delay);
    }

    @Test
    public void testDelayIfNeededAfterClose() {
        long delay = delayer.delayIfNeeded(null);
        assertEquals(0, delay);
    }

    @Test
    public void testJMXConfigOverridesSystemProperty() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        RepositoryManagementMBean mbean = mock(RepositoryManagementMBean.class);
        when(mbean.getSessionSaveDelayerConfig()).thenReturn("{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.3,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        
        whiteboard.register(RepositoryManagementMBean.class, mbean, Map.of());        
        delayer = new SessionSaveDelayer(whiteboard);
        long delay = delayer.delayIfNeeded(null);
        assertEquals(300_000L, delay); 
    }

    @Test
    public void testDelayIfNeededWithMultipleEntries() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \"non-matching\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": 0.2,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        
        delayer = new SessionSaveDelayer(whiteboard);
        long delay = delayer.delayIfNeeded(null);
        assertEquals(200_000L, delay);
    }

    @Test
    public void testDelayIfNeededWithUserDataPattern() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"admin.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        
        delayer = new SessionSaveDelayer(whiteboard);
        
        // Test with matching userData
        long delay = delayer.delayIfNeeded("admin");
        assertEquals(100_000L, delay);
        
        delay = delayer.delayIfNeeded("admin123");
        assertEquals(100_000L, delay);
        
        // Test with non-matching userData
        delay = delayer.delayIfNeeded("user");
        assertEquals(0L, delay);
        
        // Test with null userData
        delay = delayer.delayIfNeeded(null);
        assertEquals(0L, delay);
    }

    @Test
    public void testDelayIfNeededWithUserDataPatternComplexRegex() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.2,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"(admin|root|system)@.*\\\\.com$\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        
        delayer = new SessionSaveDelayer(whiteboard);
        
        // Test with matching email patterns
        long delay = delayer.delayIfNeeded("admin@example.com");
        assertEquals(200_000L, delay);
        
        delay = delayer.delayIfNeeded("root@company.com");
        assertEquals(200_000L, delay);
        
        delay = delayer.delayIfNeeded("system@test.com");
        assertEquals(200_000L, delay);
        
        // Test with non-matching patterns
        delay = delayer.delayIfNeeded("user@example.com");
        assertEquals(0L, delay);
        
        delay = delayer.delayIfNeeded("admin@example.org");
        assertEquals(0L, delay);
        
        delay = delayer.delayIfNeeded("admin");
        assertEquals(0L, delay);
    }

    @Test
    public void testDelayIfNeededWithUserDataPatternMultipleEntries() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"admin.*\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": 0.2,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"guest.*\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": 0.3,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        
        delayer = new SessionSaveDelayer(whiteboard);
        
        // Test first entry matches
        long delay = delayer.delayIfNeeded("admin");
        assertEquals(100_000L, delay);
        
        delay = delayer.delayIfNeeded("admin123");
        assertEquals(100_000L, delay);
        
        // Test second entry matches
        delay = delayer.delayIfNeeded("guest123");
        assertEquals(200_000L, delay);
        
        // Test third entry matches (no userData pattern)
        delay = delayer.delayIfNeeded("normalUser");
        assertEquals(300_000L, delay);
        
        // Test with null userData - should match third entry
        delay = delayer.delayIfNeeded(null);
        assertEquals(300_000L, delay);
    }

    @Test
    public void testDelayIfNeededWithUserDataPatternAndJMX() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        
        RepositoryManagementMBean mbean = mock(RepositoryManagementMBean.class);
        when(mbean.getSessionSaveDelayerConfig()).thenReturn("{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.15,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"privileged.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        
        whiteboard.register(RepositoryManagementMBean.class, mbean, Map.of());
        delayer = new SessionSaveDelayer(whiteboard);
        
        // Test with matching userData
        long delay = delayer.delayIfNeeded("privilegedUser");
        assertEquals(150_000L, delay);
        
        // Test with non-matching userData
        delay = delayer.delayIfNeeded("normalUser");
        assertEquals(0L, delay);
    }

    @Test
    public void testDelayIfNeededWithUserDataPatternCaseInsensitive() {
        System.setProperty(ENABLED_PROP_NAME, "true");
        System.setProperty(CONFIG_PROP_NAME, "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"(?i)ADMIN.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        
        delayer = new SessionSaveDelayer(whiteboard);
        
        // Test case-insensitive matching
        long delay = delayer.delayIfNeeded("admin");
        assertEquals(100_000L, delay);
        
        delay = delayer.delayIfNeeded("ADMIN");
        assertEquals(100_000L, delay);
        
        delay = delayer.delayIfNeeded("Admin123");
        assertEquals(100_000L, delay);
        
        delay = delayer.delayIfNeeded("administrator");
        assertEquals(100_000L, delay);
        
        // Test non-matching
        delay = delayer.delayIfNeeded("user");
        assertEquals(0L, delay);
    }



} 