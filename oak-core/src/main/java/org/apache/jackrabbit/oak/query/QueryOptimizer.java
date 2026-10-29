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
package org.apache.jackrabbit.oak.query;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndexMerger;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repository-scoped, ordered query rewrite rules.
 */
public class QueryOptimizer {

    static final String RULES_FILE = "queryRules.json";
    static final int MAX_CONFIG_BYTES = 1024 * 1024;
    static final int MAX_RULES = 1000;
    static final long CHAR_ACCESSES_PER_CHARACTER = 10_000;

    private static final Logger LOG = LoggerFactory.getLogger(QueryOptimizer.class);
    private static final long REFRESH_MILLIS = 10_000;
    private static final long NEXT_LOG_MILLIS = 10_000;

    private final LongSupplier clock;
    private final ReentrantLock refreshLock = new ReentrantLock();
    private final AtomicLong nextBudgetWarning = new AtomicLong();
    private NodeStore store;
    private volatile List<Rule> rules = List.of();
    private volatile long nextRefresh;
    private volatile PropertyState lastData;
    private volatile PropertyState lastModified;

    public QueryOptimizer() {
        this(System::currentTimeMillis);
    }

    QueryOptimizer(LongSupplier clock) {
        this.clock = clock;
    }

    public void init(NodeStore store) {
        this.store = store;
    }

    String rewrite(String statement, String language, long maxLength) throws ParseException {
        if (SQL2Parser.isInternal(statement)) {
            return statement;
        }
        refresh();
        if (language.endsWith(QueryEngineImpl.NO_LITERALS)) {
            language = language.substring(0, language.length() - QueryEngineImpl.NO_LITERALS.length());
        }
        String original = statement;
        RegexBudget budget = new RegexBudget(CHAR_ACCESSES_PER_CHARACTER * original.length());
        int ruleIndex = 0;
        // A single forward pass: later rules see the result of earlier ones.
        for (Rule rule : rules) {
            ruleIndex++;
            if (rule.language != null && !rule.language.equals(language)) {
                continue;
            }
            try {
                Matcher matcher = rule.find.matcher(new BudgetedCharSequence(statement, budget));
                if (!matcher.find()) {
                    continue;
                }
                StringBuilder result = new StringBuilder();
                do {
                    matcher.appendReplacement(result, rule.replace);
                    checkLength(result.length(), maxLength);
                } while (matcher.find());
                matcher.appendTail(result);
                checkLength(result.length(), maxLength);
                statement = result.toString();
                if (rule.stop) {
                    break;
                }
            } catch (RegexBudgetExceededException e) {
                long now = clock.getAsLong();
                long next = nextBudgetWarning.get();
                if (now >= next && nextBudgetWarning.compareAndSet(next, now + NEXT_LOG_MILLIS)) {
                    LOG.warn("Query rule {} exceeded regex character-access budget {}; using the original query",
                            ruleIndex, budget.limit);
                }
                return original;
            } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                ParseException failure = new ParseException("Invalid query rule replacement", 0);
                failure.initCause(e);
                throw failure;
            }
        }
        return statement;
    }

    private static void checkLength(int length, long maxLength) throws ParseException {
        if (length > maxLength) {
            throw new ParseException("Rewritten query length " + length
                    + " is larger than max supported query length: " + maxLength, 0);
        }
    }

    private void refresh() {
        NodeStore nodeStore = store;
        if (nodeStore == null || clock.getAsLong() < nextRefresh || !refreshLock.tryLock()) {
            return;
        }
        try {
            long now = clock.getAsLong();
            if (now < nextRefresh) {
                return;
            }
            nextRefresh = now + REFRESH_MILLIS;
            NodeState file = nodeStore.getRoot().getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
                    .getChildNode(DiffIndexMerger.DIFF_INDEX_OPTIMIZER).getChildNode(RULES_FILE);
            if (!file.exists()) {
                rules = List.of();
                lastData = null;
                lastModified = null;
                return;
            }
            NodeState content = file.getChildNode(JcrConstants.JCR_CONTENT);
            PropertyState modified = content.getProperty(JcrConstants.JCR_LASTMODIFIED);
            if (modified != null && modified.equals(lastModified)) {
                return;
            }
            PropertyState data = content.getProperty(JcrConstants.JCR_DATA);
            if (data == null) {
                throw new IllegalArgumentException("Missing jcr:content/jcr:data");
            }
            if (data.equals(lastData)) {
                lastModified = modified;
                return;
            }
            List<Rule> updated;
            try (InputStream in = data.getValue(Type.BINARY).getNewStream()) {
                byte[] bytes = in.readNBytes(MAX_CONFIG_BYTES + 1);
                if (bytes.length > MAX_CONFIG_BYTES) {
                    throw new IllegalArgumentException("Query rules file is too large");
                }
                updated = parseRules(new String(bytes, StandardCharsets.UTF_8));
            }
            rules = updated;
            lastData = data;
            lastModified = modified;
        } catch (IOException | IllegalArgumentException | IllegalStateException e) {
            LOG.warn("Cannot refresh query rules; keeping the last valid rules", e);
        } finally {
            refreshLock.unlock();
        }
    }

    private static List<Rule> parseRules(String json) {
        JsopTokenizer reader = new JsopTokenizer(json);
        List<Rule> result = new ArrayList<>();
        reader.read('[');
        if (!reader.matches(']')) {
            do {
                if (result.size() == MAX_RULES) {
                    throw new IllegalArgumentException("Too many query rules");
                }
                result.add(readRule(reader));
            } while (reader.matches(','));
            reader.read(']');
        }
        reader.read(JsopReader.END);
        return List.copyOf(result);
    }

    private static Rule readRule(JsopTokenizer reader) {
        String find = null;
        String replace = null;
        String language = null;
        boolean stop = false;
        reader.read('{');
        if (!reader.matches('}')) {
            do {
                String key = reader.readString();
                reader.read(':');
                switch (key) {
                    case "find":
                        find = reader.readString();
                        break;
                    case "replace":
                        replace = reader.readString();
                        break;
                    case "language":
                        language = reader.readString();
                        break;
                    case "options":
                        stop = readOptions(reader);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown query rule field: " + key);
                }
            } while (reader.matches(','));
            reader.read('}');
        }
        if (find == null || replace == null) {
            throw new IllegalArgumentException("Query rules require find and replace strings");
        }
        return new Rule(Pattern.compile(find), replace, language, stop);
    }

    private static boolean readOptions(JsopTokenizer reader) {
        boolean stop = false;
        reader.read('{');
        if (!reader.matches('}')) {
            do {
                String key = reader.readString();
                reader.read(':');
                if (!"stop".equals(key)) {
                    throw new IllegalArgumentException("Unknown query rule option: " + key);
                }
                if (reader.matches(JsopReader.TRUE)) {
                    stop = true;
                } else {
                    reader.read(JsopReader.FALSE);
                    stop = false;
                }
            } while (reader.matches(','));
            reader.read('}');
        }
        return stop;
    }

    static final class RegexBudget {
        private final long limit;
        private long remaining;

        RegexBudget(long limit) {
            this.limit = limit;
            this.remaining = limit;
        }
    }

    static final class BudgetedCharSequence implements CharSequence {
        private final String text;
        private final RegexBudget budget;

        BudgetedCharSequence(String text, RegexBudget budget) {
            this.text = text;
            this.budget = budget;
        }

        @Override
        public int length() {
            return text.length();
        }

        @Override
        public char charAt(int index) {
            if (budget.remaining == 0) {
                throw new RegexBudgetExceededException();
            }
            budget.remaining--;
            return text.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new BudgetedCharSequence(text.substring(start, end), budget);
        }

        @Override
        public String toString() {
            return text;
        }
    }

    private static final class RegexBudgetExceededException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        RegexBudgetExceededException() {
            super(null, null, false, false);
        }
    }

    private static final class Rule {
        final Pattern find;
        final String replace;
        final String language;
        final boolean stop;

        Rule(Pattern find, String replace, String language, boolean stop) {
            this.find = find;
            this.replace = replace;
            this.language = language;
            this.stop = stop;
        }
    }
}
