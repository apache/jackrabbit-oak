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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.query.stats.QueryRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A query options (or "hints") that are used to customize the way the query is processed.
 */
public class QueryOptions {
    
    private static final Logger LOG = LoggerFactory.getLogger(QueryOptions.class);

    public Traversal traversal = Traversal.DEFAULT;
    public String indexName;
    public String indexTag;
    public Optional<Long> limit = Optional.empty();
    public Optional<Long> offset = Optional.empty();
    public List<String> prefetch = Collections.emptyList();
    public Optional<Integer> prefetchCount = Optional.empty();
    
    public enum Traversal {
        // traversing without index is OK for this query, and does not fail or log a warning
        OK, 
        // traversing is OK, but logs a warning
        WARN, 
        // traversing will fail the query
        FAIL,
        // the default setting
        DEFAULT
    };

    public QueryOptions() {
    }

    public QueryOptions(QueryOptions defaultValues) {
        traversal = defaultValues.traversal;
        indexName = defaultValues.indexName;
        indexTag = defaultValues.indexTag;
        limit = defaultValues.limit;
        offset = defaultValues.offset;
        prefetch = defaultValues.prefetch;
        prefetchCount = defaultValues.prefetchCount;
    }

    QueryOptions(JsonObject json) {
        Map<String, String> map = json.getProperties();
        String x = map.get("traversal");
        if (x != null) {
            traversal = Traversal.valueOf(x);
        }
        x = map.get("indexName");
        if (x != null) {
            indexName = x;
        }
        x = map.get("indexTag");
        if (x != null) {
            indexTag = x;
        }
        x = map.get("limit");
        if (x != null) {
            try {
                limit = Optional.of(Long.parseLong(x));
            } catch (NumberFormatException e) {
                LOG.warn("Invalid limit {}", x);
            }
        }
        x = map.get("prefetches");
        if (x != null) {
            try {
                prefetchCount = Optional.of(Integer.parseInt(x));
            } catch (NumberFormatException e) {
                LOG.warn("Invalid prefetch count {}", x);
            }
        }
        x = map.get("prefetch");
        if (x != null) {
            ArrayList<String> list = new ArrayList<>();
            JsopTokenizer t = new JsopTokenizer(x);
            t.read('[');
            do {
                list.add(t.readString());
            } while (t.matches(','));
            t.read(']');
            prefetch = list;
        }
    }

    public static class AutomaticQueryOptionsMapping {

        private final HashMap<String, QueryOptions> map = new HashMap<>();

        public AutomaticQueryOptionsMapping(String json) {
            try {
                JsonObject obj = JsonObject.fromJson(json, true);
                for (Map.Entry<String, JsonObject> e : obj.getChildren().entrySet()) {
                    String statement = e.getKey();
                    QueryOptions options = new QueryOptions(e.getValue());
                    String queryPattern = QueryRecorder.simplifySafely(statement);
                    map.put(queryPattern, options);
                }
            } catch (IllegalArgumentException e) {
                LOG.warn("Can not parse {}", json, e);
            }
        }

        public QueryOptions getDefaultValues(String statement) {
            if (map.isEmpty()) {
                // no need to simplify if there are no entries
                // (which is probably most of the time)
                return new QueryOptions();
            }
            String queryPattern = QueryRecorder.simplifySafely(statement);
            return map.computeIfAbsent(queryPattern, v -> new QueryOptions());
        }

    }

}
