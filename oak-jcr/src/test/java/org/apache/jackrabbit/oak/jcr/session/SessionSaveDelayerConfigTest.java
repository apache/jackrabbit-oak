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

        assertTrue(first.matches("thread-123", null, "at com.example.SomeClass.method()"));
        assertTrue(first.matches("thread-abc", null, "SomeClass is here"));
        assertFalse(first.matches("thread-123", null, "no matching class"));
        assertFalse(first.matches("thread-123", null, null));
        assertFalse(first.matches("worker-123", null, "at com.example.SomeClass.method()"));

        assertTrue(second.matches("worker-123", null, "any stack trace"));
        assertTrue(second.matches("worker-456", null, null));
        assertFalse(second.matches("worker-abc", null, "any stack trace"));
        assertFalse(second.matches("thread-123", null, "any stack trace"));
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
        assertTrue(entry.matches("pool-1-thread-5", null, "at com.example.Service.save()"));
        assertTrue(entry.matches("POOL-2-THREAD-10", null, "at com.example.Service.update()"));
        
        // Test stack trace pattern matching
        assertTrue(entry.matches("pool-1-thread-1", null, "at com.example.Repository.delete(Repository.java:100)"));
        assertFalse(entry.matches("pool-1-thread-1", null, "at com.example.Service.get()"));
    }

    @Test
    public void testUserDataPatternBasic() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1.0,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"admin.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);
        assertEquals(1_000_000L, entry.getDelayNanos());

        // Test userDataPattern matching
        assertTrue(entry.matches("any-thread", "admin", null));
        assertTrue(entry.matches("any-thread", "admin123", null));
        assertTrue(entry.matches("any-thread", "adminUser", null));
        assertFalse(entry.matches("any-thread", "user", null));
        assertFalse(entry.matches("any-thread", "testAdmin", null));
        assertFalse(entry.matches("any-thread", null, null)); // null userData should not match
    }

    @Test
    public void testUserDataPatternWithComplexRegex() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.5,\n" +
                "      \"threadNameRegex\": \"worker-.*\",\n" +
                "      \"userDataRegex\": \"(admin|root|system)@.*\\\\.com$\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);

        // Test complex userDataPattern matching
        assertTrue(entry.matches("worker-1", "admin@example.com", null));
        assertTrue(entry.matches("worker-2", "root@company.com", null));
        assertTrue(entry.matches("worker-3", "system@test.com", null));
        assertFalse(entry.matches("worker-1", "user@example.com", null));
        assertFalse(entry.matches("worker-1", "admin@example.org", null));
        assertFalse(entry.matches("worker-1", "admin", null));
        assertFalse(entry.matches("other-thread", "admin@example.com", null)); // thread name doesn't match
    }

    @Test
    public void testUserDataPatternCombinedWithStackTrace() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 2.0,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"privileged.*\",\n" +
                "      \"stackTraceRegex\": \".*Session\\\\.save.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);

        // All conditions must match
        assertTrue(entry.matches("any-thread", "privilegedUser", "at javax.jcr.Session.save(Session.java:123)"));
        assertFalse(entry.matches("any-thread", "privilegedUser", "at javax.jcr.Session.refresh(Session.java:456)")); // stack trace doesn't match
        assertFalse(entry.matches("any-thread", "normalUser", "at javax.jcr.Session.save(Session.java:123)")); // userData doesn't match
        assertFalse(entry.matches("any-thread", null, "at javax.jcr.Session.save(Session.java:123)")); // null userData
    }

    @Test
    public void testUserDataPatternMultipleEntries() {
        String json = "{\n" +
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
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        
        // Test that first matching entry is used
        assertEquals(100_000L, config.getDelayNanos("thread-1", "admin", null));
        assertEquals(200_000L, config.getDelayNanos("thread-1", "guest123", null));
        assertEquals(300_000L, config.getDelayNanos("thread-1", "normalUser", null)); // matches third entry (no userData pattern)
        assertEquals(300_000L, config.getDelayNanos("thread-1", null, null)); // matches third entry (no userData pattern)
    }

    @Test
    public void testUserDataPatternWithInvalidRegex() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1.0,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"[invalid-regex\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        // Entry should be skipped due to invalid regex
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testUserDataPatternEmptyString() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1.0,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);

        // Empty pattern should match empty string but not non-empty strings
        assertTrue(entry.matches("any-thread", "", null));
        assertTrue(entry.matches("any-thread", "anything", null)); // empty regex matches anything using find()
        assertFalse(entry.matches("any-thread", null, null)); // null userData should not match
    }

    @Test
    public void testUserDataPatternCaseInsensitive() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1.0,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"(?i)ADMIN.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);

        // Test case-insensitive matching
        assertTrue(entry.matches("any-thread", "admin", null));
        assertTrue(entry.matches("any-thread", "ADMIN", null));
        assertTrue(entry.matches("any-thread", "Admin123", null));
        assertTrue(entry.matches("any-thread", "administrator", null));
        assertFalse(entry.matches("any-thread", "user", null));
    }

    @Test 
    public void testGetDelayNanosWithUserDataPattern() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 1.0,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"admin.*\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"delayMillis\": 0.5,\n" +
                "      \"threadNameRegex\": \".*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        
        // When userData matches first entry's pattern
        assertEquals(1_000_000L, config.getDelayNanos("thread-1", "admin", null));
        assertEquals(1_000_000L, config.getDelayNanos("thread-1", "admin123", null));
        
        // When userData doesn't match first entry but matches second (no userData pattern)
        assertEquals(500_000L, config.getDelayNanos("thread-1", "user", null));
        assertEquals(500_000L, config.getDelayNanos("thread-1", "guest", null));
        
        // When userData is null, first entry shouldn't match but second should
        assertEquals(500_000L, config.getDelayNanos("thread-1", null, null));
    }

    @Test
    public void testRateLimitingBasic() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"maxSavesPerSecond\": 2.0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);
        assertEquals(100_000L, entry.getBaseDelayNanos());
        assertEquals(2.0, entry.getMaxSavesPerSecond(), 0.001);

        // First call should only have base delay
        long firstDelay = entry.getDelayNanos();
        assertEquals(100_000L, firstDelay);

        // Second call immediately should have additional rate limit delay
        // With 2 saves per second, minimum interval is 500ms
        long secondDelay = entry.getDelayNanos();
        assertTrue("Second delay should be >= base delay + rate limit delay", 
                secondDelay >= 100_000L + 400_000_000L); // ~500ms in nanos
    }

    @Test
    public void testRateLimitingWithZeroMaxSaves() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"maxSavesPerSecond\": 0.0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        List<SessionSaveDelayerConfig.DelayEntry> entries = config.getEntries();
        assertEquals(1, entries.size());

        SessionSaveDelayerConfig.DelayEntry entry = entries.get(0);
        assertEquals(0.0, entry.getMaxSavesPerSecond(), 0.001);

        // All calls should only have base delay since rate limiting is disabled
        assertEquals(100_000L, entry.getDelayNanos());
        assertEquals(100_000L, entry.getDelayNanos());
        assertEquals(100_000L, entry.getDelayNanos());
    }

    @Test
    public void testRateLimitingWithNegativeMaxSaves() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"maxSavesPerSecond\": -1.0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        // Entry should be skipped due to negative maxSavesPerSecond
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testRateLimitingWithInvalidMaxSaves() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"maxSavesPerSecond\": \"invalid\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        assertNotNull(config);

        // Entry should be skipped due to invalid maxSavesPerSecond
        assertTrue(config.getEntries().isEmpty());
    }

    @Test
    public void testRateLimitingHighFrequency() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.0,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"maxSavesPerSecond\": 10.0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        SessionSaveDelayerConfig.DelayEntry entry = config.getEntries().get(0);

        // With 10 saves per second, minimum interval is 100ms
        long firstDelay = entry.getDelayNanos();
        assertEquals(0L, firstDelay); // No base delay

        long secondDelay = entry.getDelayNanos();
        assertTrue("Should have rate limit delay", secondDelay >= 90_000_000L); // ~100ms in nanos
    }



    @Test
    public void testRateLimitingCombinedWithUserDataPattern() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"userDataRegex\": \"admin.*\",\n" +
                "      \"maxSavesPerSecond\": 1.0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        SessionSaveDelayerConfig.DelayEntry entry = config.getEntries().get(0);

        // Test rate limiting only applies when entry matches
        assertTrue(entry.matches("thread-1", "admin", null));
        assertFalse(entry.matches("thread-1", "user", null));

        // Rate limiting should work for matching entries
        long firstDelay = entry.getDelayNanos();
        assertEquals(100_000L, firstDelay);

        long secondDelay = entry.getDelayNanos();
        assertTrue("Should have rate limit delay", secondDelay >= 1_000_000_000L); // ~1000ms
    }

    @Test
    public void testRateLimitingJsonSerialization() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.5,\n" +
                "      \"threadNameRegex\": \"worker-.*\",\n" +
                "      \"maxSavesPerSecond\": 3.5\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        String serialized = config.toString();

        // Check that serialization includes maxSavesPerSecond
        assertTrue(serialized.contains("\"maxSavesPerSecond\": 3.5"));
        assertTrue(serialized.contains("\"delayMillis\": 0.5"));
        assertTrue(serialized.contains("\"threadNameRegex\": \"worker-.*\""));
    }

    @Test
    public void testRateLimitingJsonSerializationWithoutMaxSaves() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.5,\n" +
                "      \"threadNameRegex\": \"worker-.*\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        String serialized = config.toString();

        // Check that serialization doesn't include maxSavesPerSecond when it's 0
        assertFalse(serialized.contains("maxSavesPerSecond"));
    }

    @Test
    public void testGetDelayNanosWithRateLimit() {
        String json = "{\n" +
                "  \"entries\": [\n" +
                "    {\n" +
                "      \"delayMillis\": 0.1,\n" +
                "      \"threadNameRegex\": \".*\",\n" +
                "      \"maxSavesPerSecond\": 2.0\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        SessionSaveDelayerConfig config = SessionSaveDelayerConfig.fromJson(json);
        
        // First call should only have base delay
        long firstDelay = config.getDelayNanos("thread-1", null, null);
        assertEquals(100_000L, firstDelay);

        // Second call should have additional rate limit delay
        long secondDelay = config.getDelayNanos("thread-1", null, null);
        assertTrue("Should have rate limit delay", secondDelay >= 400_000_000L);
    }
} 