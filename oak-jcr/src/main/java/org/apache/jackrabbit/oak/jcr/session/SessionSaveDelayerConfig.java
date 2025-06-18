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

    public long getDelayNanos(@NotNull String threadName, @Nullable String stackTrace) {
        for (DelayEntry d : entries) {
            if (d.matches(threadName, stackTrace)) {
                return d.delayNanos;
            }
        }
        return 0;
    }

    @Nullable
    private static DelayEntry parseDelayEntry(JsonObject entryObj) {
        String delayMillis = entryObj.getProperties().get("delayMillis");
        String threadNameRegex = entryObj.getProperties().get("threadNameRegex");
        String stackTraceRegex = entryObj.getProperties().get("stackTraceRegex");
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
            Pattern threadPattern = Pattern.compile(JsopTokenizer.decodeQuoted(threadNameRegex));
            Pattern stackPattern = null;
            if (stackTraceRegex != null) {
                stackPattern = Pattern.compile(JsopTokenizer.decodeQuoted(stackTraceRegex));
            }
            return new DelayEntry(delay, threadPattern, stackPattern);
        } catch (NumberFormatException e) {
            LOG.warn("Skipping entry with invalid delay value: {}", delayMillis);
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

    public static class DelayEntry {
        private final long delayNanos;
        private final Pattern threadNamePattern;
        private final Pattern stackTracePattern;

        public DelayEntry(double delayMillis, @NotNull Pattern threadNamePattern, @Nullable Pattern stackTracePattern) {
            this.delayNanos = (long) (delayMillis * 1_000_000);
            this.threadNamePattern = threadNamePattern;
            this.stackTracePattern = stackTracePattern;
        }

        public long getDelayNanos() {
            return delayNanos;
        }

        @NotNull
        public Pattern getThreadNamePattern() {
            return threadNamePattern;
        }

        @Nullable
        public Pattern getStackTracePattern() {
            return stackTracePattern;
        }

        boolean matches(@NotNull String threadName, @Nullable String stackTrace) {
            if (!threadNamePattern.matcher(threadName).matches()) {
                return false;
            }
            if (stackTracePattern != null) {
                if (stackTrace == null) {
                    stackTrace = SessionSaveDelayer.getCurrentStackTrace();
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
            double delayMillis = delayNanos / 1_000_000.0;
            json.key("delayMillis").encodedValue(Double.toString(delayMillis));
            json.key("threadNameRegex").value(threadNamePattern.pattern());
            if (stackTracePattern != null) {
                json.key("stackTraceRegex").value(stackTracePattern.pattern());
            }
            return json.endObject();
        }

    }
}
