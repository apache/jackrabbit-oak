/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.text.ParseException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A validator for query. Invalid queries either log a warning, or throw an
 * exception when trying to execute.
 */
public class QueryValidator {

    private static final Logger LOG = LoggerFactory.getLogger(QueryValidator.class);

    /**
     * The name of the query validator node.
     */
    public static final String QUERY_VALIDATOR = "queryValidator";

    /**
     * The next time to log a warning for a query, in milliseconds.
     */
    private static final int NEXT_LOG_MILLIS = 10 * 1000;
    
    /**
     * The map of invalid query patterns.
     */
    private final ConcurrentSkipListMap<String, ProblematicQueryPattern> map = new ConcurrentSkipListMap<>();
    
    /**
     * Add a pattern.
     * 
     * @param key the key
     * @param pattern the pattern regular expression - if empty, the entry is removed
     * @param comment the comment
     * @param failQuery - if true, trying to run such a query will fail;
     *            otherwise the queries that will work, but will log a warning.
     *            A warning is logged at most once every 10 seconds.
     */
    public void setPattern(String key, String pattern, String comment, boolean failQuery) {
        LOG.debug("set pattern key={} pattern={} comment={} failQuery={}", key, pattern, comment, failQuery);
        if (pattern.isEmpty()) {
            map.remove(key);
        } else {
            ProblematicQueryPattern p = new ProblematicQueryPattern(key, pattern, comment, failQuery);
            map.put(key, p);
        }
    }

    /**
     * Get the current set of pattern data.
     * 
     * @return the json representation
     */
    public String getJson() {
        JsopBuilder b = new JsopBuilder().array();
        for (ProblematicQueryPattern p : map.values()) {
            b.newline().encodedValue(p.getJson());
        }
        return b.endArray().toString();
    }

    /**
     * Check if a query is valid. It is either valid, logs a warning, or throws a exception if invalid.
     * 
     * @param statement the query statement
     * @throws ParseException if it is invalid
     */
    public void checkStatement(String statement) throws ParseException {
        if (map.isEmpty()) {
            // the normal case: no patterns defined
            return;
        }
        for (ProblematicQueryPattern p : map.values()) {
            p.checkStatement(statement);
        }
    }
    
    public void init(NodeStore store) {
        NodeState def = store.getRoot().getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME).
                getChildNode(QUERY_VALIDATOR);
        if (!def.exists()) {
            return;
        }
        for (ChildNodeEntry e : def.getChildNodeEntries()) {
            String key = e.getName();
            NodeState n = e.getNodeState();
            PropertyState p = n.getProperty("pattern");
            if (p == null) {
                continue;
            }
            String pattern;
            if (p.isArray()) {
                int len = p.count();
                StringBuilder buff = new StringBuilder();
                for (int i = 0; i < len; i++) {
                    if (buff.length() > 0) {
                        buff.append(".*");
                    }
                    buff.append(Pattern.quote(p.getValue(Type.STRING, i)));
                }
                pattern = buff.toString();
            } else {
                pattern = p.getValue(Type.STRING);
            }
            String comment = n.getProperty("comment").getValue(Type.STRING);
            boolean failQuery = n.getProperty("failQuery").getValue(Type.BOOLEAN);
            if (pattern != null && comment != null) {
                setPattern(key, pattern, comment, failQuery);
            }
        }
    }
    
    /**
     * A query pattern definition.
     */
    private static class ProblematicQueryPattern {
        
        private final String key;
        private final String pattern;
        private final String comment;
        private final Pattern compiledPattern; 
        private final boolean failQuery;
        private long executedLast;
        private long executedCount;
        
        ProblematicQueryPattern(String key, String pattern, String comment, boolean failQuery) {
            this.key = key;
            this.pattern = pattern;
            this.comment = comment;
            this.compiledPattern = Pattern.compile(pattern);
            this.failQuery = failQuery;
        }
        
        void checkStatement(String statement) throws ParseException {
            if (!compiledPattern.matcher(statement).matches()) {
                return;
            }
            executedCount++;
            long previousExecuted = executedLast;
            long now = System.currentTimeMillis();
            executedLast = now;
            if (failQuery) {
                String message = "Query is blacklisted: statement=" + statement + " pattern=" + pattern;
                ParseException p = new ParseException(message, 0);
                LOG.warn(message, p);
                throw p;
            } else {
                String message = "Query is questionable, but executed: statement=" + statement + " pattern=" + pattern;
                if (previousExecuted + NEXT_LOG_MILLIS < now) {
                    LOG.warn(message, new Exception("QueryValidator"));
                } else {
                    LOG.debug(message, new Exception("QueryValidator"));
                }
            }
        }
        
        String getJson() {
            return new JsopBuilder().object().newline().
                    key("key").value(key).newline().
                    key("pattern").value(pattern).newline().
                    key("comment").value(comment).newline().
                    key("failQuery").value(failQuery).newline().
                    key("executedLast").value(
                            executedLast == 0 ? "" : new java.sql.Timestamp(executedLast).toString()).newline().
                    key("executedCount").value(executedCount).newline().
                    endObject().toString();
        }
    }

}
