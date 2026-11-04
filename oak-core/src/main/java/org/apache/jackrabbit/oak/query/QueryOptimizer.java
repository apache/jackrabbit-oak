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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndexMerger;
import org.apache.jackrabbit.oak.query.regex.BudgetedCharSequence;
import org.apache.jackrabbit.oak.query.regex.RegexBudget;
import org.apache.jackrabbit.oak.query.regex.RegexBudgetExceededException;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryOptimizer.class);
    private static final long REFRESH_MILLIS = 10_000;
    private static final String FIND = "find";
    private static final String REPLACE = "replace";
    private static final String FIND_REGEX = "findRegex";
    private static final String REPLACE_REGEX = "replaceRegex";
    private static final String LANGUAGE = "language";
    private static final String STOP = "stop";
    private static final String LOG = "log";

    private final LongSupplier clock;
    private final ReentrantLock refreshLock = new ReentrantLock();
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

    String rewrite(String statement, String language) throws ParseException {
        if (SQL2Parser.isInternal(statement)) {
            return statement;
        }
        refresh();
        String ruleLanguage = language;
        if (ruleLanguage.endsWith(QueryEngineImpl.NO_LITERALS)) {
            ruleLanguage = ruleLanguage.substring(0, ruleLanguage.length() - QueryEngineImpl.NO_LITERALS.length());
        }
        String original = statement;
        RegexBudget budget = new RegexBudget(CHAR_ACCESSES_PER_CHARACTER * original.length());
        int ruleIndex = 0;
        // A single forward pass: later rules see the result of earlier ones.
        for (Rule rule : rules) {
            ruleIndex++;
            if (rule.language != null && !rule.language.equals(ruleLanguage)) {
                continue;
            }
            try {
                Matcher matcher = rule.find.matcher(new BudgetedCharSequence(statement, budget));
                if (!matcher.find()) {
                    continue;
                }
                if (rule.replace == null) {
                    if (rule.log) {
                        LOGGER.info("Matched {} query: {}", language, statement);
                    }
                } else {
                    StringBuilder result = new StringBuilder();
                    do {
                        matcher.appendReplacement(result, rule.replace);
                    } while (matcher.find());
                    matcher.appendTail(result);
                    String rewritten = result.toString();
                    if (rule.log && !statement.equals(rewritten)) {
                        LOGGER.info("Rewriting {} query: {} to {}", language, statement, rewritten);
                    }
                    statement = rewritten;
                }
                if (rule.stop) {
                    break;
                }
            } catch (RegexBudgetExceededException e) {
                LOGGER.warn("Query rule {} exceeded regex character-access budget {}; using the original query",
                        ruleIndex, budget.getLimit());
                return original;
            } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                ParseException failure = new ParseException("Invalid query rule replacement", 0);
                failure.initCause(e);
                throw failure;
            }
        }
        return statement;
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
            LOGGER.warn("Cannot refresh query rules; keeping the last valid rules", e);
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
                reader.read('{');
                result.add(readRule(JsonObject.create(reader)));
            } while (reader.matches(','));
            reader.read(']');
        }
        reader.read(JsopReader.END);
        return List.copyOf(result);
    }

    private static Rule readRule(JsonObject json) {
        Map<String, String> properties = json.getProperties();
        if (!Set.of(FIND, REPLACE, FIND_REGEX, REPLACE_REGEX, LANGUAGE, STOP, LOG).containsAll(properties.keySet())
                || !json.getChildren().isEmpty()) {
            throw new IllegalArgumentException("Invalid query rule field");
        }
        boolean regex = properties.containsKey(FIND_REGEX) || properties.containsKey(REPLACE_REGEX);
        if (regex && (properties.containsKey(FIND) || properties.containsKey(REPLACE))) {
            throw new IllegalArgumentException("Cannot mix template and regex query rule fields");
        }
        String find = properties.get(regex ? FIND_REGEX : FIND);
        String replace = properties.get(regex ? REPLACE_REGEX : REPLACE);
        String language = properties.get(LANGUAGE);
        if (find == null) {
            throw new IllegalArgumentException("Query rules require a find or findRegex string");
        }
        find = JsopTokenizer.decodeQuoted(find);
        replace = replace == null ? null : JsopTokenizer.decodeQuoted(replace);
        QueryTemplate template = regex ? null : new QueryTemplate(find, replace);
        return new Rule(regex ? Pattern.compile(find) : template.find,
                regex ? replace : template.replace,
                language == null ? null : JsopTokenizer.decodeQuoted(language),
                readBoolean(properties, STOP, false), readBoolean(properties, LOG, true));
    }

    private static boolean readBoolean(Map<String, String> properties, String name, boolean defaultValue) {
        String value = properties.getOrDefault(name, Boolean.toString(defaultValue));
        if (!"true".equals(value) && !"false".equals(value)) {
            throw new IllegalArgumentException("Query rule option " + name + " must be a boolean");
        }
        return Boolean.parseBoolean(value);
    }

    private static final class Rule {
        final Pattern find;
        final String replace;
        final String language;
        final boolean stop;
        final boolean log;

        Rule(Pattern find, String replace, String language, boolean stop, boolean log) {
            this.find = find;
            this.replace = replace;
            this.language = language;
            this.stop = stop;
            this.log = log;
        }
    }
}
