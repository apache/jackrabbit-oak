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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration parser for a session save delay JSON configuration:
 * <pre>
 * {
 *   "entries": [
 *     {
 *       "delayMillis": 1.0,
 *       "threadNameRegex": "thread-.*",
 *       "stackTraceRegex": ".*SomeClass.*"
 *     },
 *     {
 *       "delayMillis": 0.5,
 *       "threadNameRegex": "worker-\\d+"
 *     }
 *   ]
 * }
 * </pre>
 */
public class SessionSaveDelayerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(SessionSaveDelayerConfig.class);

    private final List<DelayEntry> entries;

    public SessionSaveDelayerConfig(@NotNull List<DelayEntry> entries) {
        this.entries = new ArrayList<>(entries);
    }

    @NotNull
    public static SessionSaveDelayerConfig fromJson(@NotNull String jsonConfig) throws IllegalArgumentException {
        if (Strings.isNullOrEmpty(jsonConfig)) {
            return new SessionSaveDelayerConfig(List.of());
        }
        try {
            JsopTokenizer tokenizer = new JsopTokenizer(jsonConfig);
            tokenizer.read('{');
            JsonObject root = JsonObject.create(tokenizer);
            List<DelayEntry> entries = new ArrayList<>();
            String entriesJson = root.getProperties().get("entries");
            if (entriesJson != null) {
                JsopTokenizer entryTokenizer = new JsopTokenizer(entriesJson);
                entryTokenizer.read('[');
                if (!entryTokenizer.matches(']')) {
                    do {
                        if (entryTokenizer.matches('{')) {
                            DelayEntry entry = parseDelayEntry(JsonObject.create(entryTokenizer));
                            if (entry != null) {
                                entries.add(entry);
                            }
                        } else {
                            throw new IllegalArgumentException("Expected object in entries array");
                        }
                    } while (entryTokenizer.matches(','));
                    entryTokenizer.read(']');
                }
            }
            return new SessionSaveDelayerConfig(entries);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON configuration: " + e.getMessage(), e);
        }
    }

    public List<DelayEntry> getEntries() {
        return entries;
    }

    public long getDelayNanos(@NotNull String threadName, @Nullable String userData, @Nullable String stackTrace) {
        for (DelayEntry d : entries) {
            if (d.matches(threadName, userData, stackTrace)) {
                return d.getDelayNanos();
            }
        }
        return 0;
    }

    @Nullable
    private static DelayEntry parseDelayEntry(JsonObject entryObj) {
        String delayMillis = entryObj.getProperties().get("delayMillis");
        String threadNameRegex = entryObj.getProperties().get("threadNameRegex");
        String userDataRegex = entryObj.getProperties().get("userDataRegex");
        String stackTraceRegex = entryObj.getProperties().get("stackTraceRegex");
        String maxSavesPerSecond = entryObj.getProperties().get("maxSavesPerSecond");
        if (delayMillis == null || threadNameRegex == null) {
            LOG.warn("Skipping entry with missing required fields (delay or threadNameRegex)");
            return null;
        }
        try {
            double delay = Double.parseDouble(delayMillis);
            if (delay < 0) {
                LOG.warn("Skipping entry with negative delay");
                return null;
            }
            double maxSaves = 0.0;
            if (maxSavesPerSecond != null) {
                maxSaves = Double.parseDouble(maxSavesPerSecond);
                if (maxSaves < 0) {
                    LOG.warn("Skipping entry with negative maxSavesPerSecond");
                    return null;
                }
            }
            Pattern threadPattern = Pattern.compile(JsopTokenizer.decodeQuoted(threadNameRegex));
            Pattern stackPattern = null;
            if (stackTraceRegex != null) {
                stackPattern = Pattern.compile(JsopTokenizer.decodeQuoted(stackTraceRegex));
            }
            Pattern userDataPattern = null;
            if (userDataRegex != null) {
                userDataPattern = Pattern.compile(JsopTokenizer.decodeQuoted(userDataRegex));
            }
            return new DelayEntry(delay, threadPattern, userDataPattern, stackPattern, maxSaves);
        } catch (NumberFormatException e) {
            LOG.warn("Skipping entry with invalid delay value or maxSavesPerSecond: {}", e.getMessage());
            return null;
        } catch (PatternSyntaxException e) {
            LOG.warn("Skipping entry with invalid regex pattern: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public String toString() {
        JsopBuilder json = new JsopBuilder();
        json.object().key("entries").array();
        for (DelayEntry entry : entries) {
            entry.toJson(json);
        }
        json.endArray().endObject();
        return JsopBuilder.prettyPrint(json.toString());
    }

    /**
     * Gets the stack trace of the current thread as a string.
     *
     * @return the current stack trace as a formatted string, or null if no stack trace is available
     */
    @Nullable
    public static String getCurrentStackTrace() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        if (stackTrace == null || stackTrace.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < stackTrace.length; i++) {
            if (i > 0) {
                sb.append("\n\tat ");
            } else {
                sb.append("at ");
            }
            sb.append(stackTrace[i]);
        }
        return sb.toString();
    }

    public static class DelayEntry {
        private final long baseDelayNanos;
        private final Pattern threadNamePattern;
        private final Pattern stackTracePattern;
        private final Pattern userDataPattern;
        private final double maxSavesPerSecond;
        private final AtomicLong lastMatch = new AtomicLong(0);

        public DelayEntry(double delayMillis, @NotNull Pattern threadNamePattern, @Nullable Pattern userDataPattern, @Nullable Pattern stackTracePattern, double maxSavesPerSecond) {
            this.baseDelayNanos = (long) (delayMillis * 1_000_000);
            this.threadNamePattern = threadNamePattern;
            this.userDataPattern = userDataPattern;
            this.stackTracePattern = stackTracePattern;
            this.maxSavesPerSecond = maxSavesPerSecond;
        }

        public long getDelayNanos() {
            long totalDelayNanos = baseDelayNanos;
            if (maxSavesPerSecond > 0) {
                long currentTime = System.currentTimeMillis();
                double intervalMs = 1000.0 / maxSavesPerSecond;
                long lastMatchTime = lastMatch.get();
                if (lastMatchTime > 0) {
                    long nextAllowedTime = lastMatchTime + (long) intervalMs;
                    if (currentTime < nextAllowedTime) {
                        long rateLimitDelayMs = nextAllowedTime - currentTime;
                        totalDelayNanos += rateLimitDelayMs * 1_000_000;
                    }
                }
                lastMatch.set(currentTime);
            }
            return totalDelayNanos;
        }

        public long getBaseDelayNanos() {
            return baseDelayNanos;
        }

        public double getMaxSavesPerSecond() {
            return maxSavesPerSecond;
        }

        @NotNull
        public Pattern getThreadNamePattern() {
            return threadNamePattern;
        }

        @Nullable
        public Pattern getStackTracePattern() {
            return stackTracePattern;
        }

        @Nullable
        public Pattern getUserDataPattern() {
            return userDataPattern;
        }

        boolean matches(@NotNull String threadName, @Nullable String userData, @Nullable String stackTrace) {
            if (!threadNamePattern.matcher(threadName).matches()) {
                return false;
            }
            if (userDataPattern != null) {
                if (userData == null) {
                    return false;
                }
                if (!userDataPattern.matcher(userData).find()) {
                    return false;
                }
            }
            if (stackTracePattern != null) {
                if (stackTrace == null) {
                    stackTrace = SessionSaveDelayerConfig.getCurrentStackTrace();
                }
                return stackTracePattern.matcher(stackTrace).find();
            }
            return true;
        }

        @Override
        public String toString() {
            return toJson(new JsopBuilder()).toString();
        }

        public JsopBuilder toJson(JsopBuilder json) {
            json.object();
            double delayMillis = baseDelayNanos / 1_000_000.0;
            json.key("delayMillis").encodedValue(Double.toString(delayMillis));
            json.key("threadNameRegex").value(threadNamePattern.pattern());
            if (userDataPattern != null) {
                json.key("userDataRegex").value(userDataPattern.pattern());
            }
            if (stackTracePattern != null) {
                json.key("stackTraceRegex").value(stackTracePattern.pattern());
            }
            if (maxSavesPerSecond > 0) {
                json.key("maxSavesPerSecond").encodedValue(Double.toString(maxSavesPerSecond));
            }
            return json.endObject();
        }

    }
}
