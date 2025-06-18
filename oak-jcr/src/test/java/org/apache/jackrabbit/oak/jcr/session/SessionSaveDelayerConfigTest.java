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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

/**
 * Tests for {@link SessionSaveDelayerConfig}.
 */
public class SessionSaveDelayerConfigTest {

    @Test
    public void testEmptyConfiguration() {
        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson("");
        assertNotNull(config);
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testNullConfiguration() {
        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(null);
        assertNotNull(config);
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testBasicConfiguration() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.5,\n" +
                "      \"threadNameRegex\": \"worker-\\\\d+\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": 1,\n" +
                "      \"threadNameRegex\": \"thread-.*\",\n" +
                "      \"stackTraceRegex\": \".*SomeClass.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(2, entries.size());

        SessionSaveDelayerConfig.DelayEntry first = entries.get(0);
        assertEquals(500_000L, first.getDelayNanos());
        assertEquals("worker-\\d+", first.getThreadNamePattern().pattern());
        assertNull(first.getStackTracePattern());

        SessionSaveDelayerConfig.DelayEntry second = entries.get(1);
        assertEquals(1_000_000L, second.getDelayNanos());
        assertEquals("thread-.*", second.getThreadNamePattern().pattern());
        assertNotNull(second.getStackTracePattern());
        assertEquals(".*SomeClass.*", second.getStackTracePattern().pattern());
    }

    @Test
    public void testEntryMatching() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1,\n" +
                "      \"threadNameRegex\": \"thread-.*\",\n" +
                "      \"stackTraceRegex\": \".*SomeClass.*\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": 0.5,\n" +
                "      \"threadNameRegex\": \"worker-\\\\d+\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();

        SessionSaveDelayerConfig.DelayEntry first = entries.get(0);
        SessionSaveDelayerConfig.DelayEntry second = entries.get(1);

        assertTrue(first.matches("thread-123", "at com.example.SomeClass.method()"));
        assertTrue(first.matches("thread-abc", "SomeClass is here"));
        assertFalse(first.matches("thread-123", "no matching class"));
        assertFalse(first.matches("thread-123", null));
        assertFalse(first.matches("worker-123", "at com.example.SomeClass.method()"));

        assertTrue(second.matches("worker-123", "any stack trace"));
        assertTrue(second.matches("worker-456", null));
        assertFalse(second.matches("worker-abc", "any stack trace"));
        assertFalse(second.matches("thread-123", "any stack trace"));
    }

    @Test
    public void testConfigurationWithMissingFields() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1,\n" +
                "      \"threadNameRegex\": \"thread-.*\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"threadNameRegex\": \"worker-\\\\d+\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": 0.5\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        // Only the first entry should be valid (has both delay and threadNameRegex)
        assertEquals(1, entries.size());
        
        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);
        assertEquals(1000_000L, entry.getDelayNanos());
        assertEquals("thread-.*", entry.getThreadNamePattern().pattern());
        assertNull(entry.getStackTracePattern());
    }

    @Test
    public void testConfigurationWithInvalidValues() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": \"invalid\",\n" +
                "      \"threadNameRegex\": \"thread-.*\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": -100,\n" +
                "      \"threadNameRegex\": \"worker-\\\\d+\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delay\": 500,\n" +
                "      \"threadNameRegex\": \"[invalid-regex\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        // All entries should be invalid and skipped
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testEmptyEntriesArray() {
        String json = "{\n" +
                "  \"entries\": []\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testConfigurationWithoutEntriesProperty() {
        String json = "{\n" +
                "  \"someOtherProperty\": \"value\"\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testInvalidJsonThrowsException() {
        String invalidJson = "{ invalid json }";

        try {
            SessionSaveDelayerConfig.fromJson(invalidJson);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Failed to parse JSON configuration"));
        }
    }

    @Test
    public void testDelayConfigToString() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1.0,\n" +
                "      \"threadNameRegex\": \"thread-.*\",\n" +
                "      \"stackTraceRegex\": \".*SomeClass.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        
        assertEquals("{\n"
                + "  \"entries\": [{\n"
                + "    \"delayMillis\": 1.0, \"threadNameRegex\": \"thread-.*\", \"stackTraceRegex\": \".*SomeClass.*\"\n"
                + "  }]\n"
                + "}", config.toString());
    }

    @Test
    public void testComplexRegexPatterns() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 2,\n" +
                "      \"threadNameRegex\": \"(?i)pool-\\\\d+-thread-\\\\d+\",\n" +
                "      \"stackTraceRegex\": \".*\\\\.(save|update|delete)\\\\(.*\\\\).*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);
        assertEquals(2_000_000L, entry.getDelayNanos());

        // Test case-insensitive thread name matching
        assertTrue(entry.matches("pool-1-thread-5", "at com.example.Service.save()"));
        assertTrue(entry.matches("POOL-2-THREAD-10", "at com.example.Service.update()"));
        
        // Test stack trace pattern matching
        assertTrue(entry.matches("pool-1-thread-1", "at com.example.Repository.delete(Repository.java:100)"));
        assertFalse(entry.matches("pool-1-thread-1", "at com.example.Service.get()"));
    }
} 