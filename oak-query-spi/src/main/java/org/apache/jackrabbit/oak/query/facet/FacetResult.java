/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.query.facet;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparingInt;

import java.util.ArrayList;

/**
 * A facet result is a wrapper for {@link javax.jcr.query.QueryResult} capable of returning information about facets
 * stored in the query result {@link javax.jcr.query.Row}s.
 */
public class FacetResult {

    private final Map<String, List<Facet>> perDimFacets = new HashMap<String, List<Facet>>();

    public FacetResult(QueryResult queryResult) {
        try {
            RowIterator rows = queryResult.getRows();
            if (rows.hasNext()) {
                Row row = rows.nextRow();
                parseJson(queryResult.getColumnNames(), columnName -> {
                    Value value = row.getValue(columnName);
                    return value == null ? null : value.getString();
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public FacetResult(String[] columnNames, FacetResultRow...rows) {
        try {
            for (FacetResultRow row : rows) {
                parseJson(columnNames, row);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> asColumnToFacetJsonMap() {
        Map<String, String> json = new HashMap<>();
        for (Map.Entry<String, List<Facet>> entry : perDimFacets.entrySet()) {
            JsopBuilder builder = new JsopBuilder();
            builder.object();

            for (Facet f : entry.getValue()) {
                builder.key(f.getLabel());
                builder.value(f.getCount());
            }

            builder.endObject();

            json.put(QueryConstants.REP_FACET + "(" + entry.getKey() + ")", builder.toString());
        }

        return json;
    }

    private void parseJson(String[] columnNames, FacetResultRow row) throws Exception {
        for (String column : columnNames) {
            if (column.startsWith(QueryConstants.REP_FACET)) {
                String dimension = column.substring(QueryConstants.REP_FACET.length() + 1, column.length() - 1);
                String value = row.getValue(column);
                if (value != null) {
                    String jsonFacetString = value;
                    parseJson(dimension, jsonFacetString);
                }
            }
        }
    }

    private void parseJson(String dimension, String jsonFacetString) {
        JsopTokenizer jsopTokenizer = new JsopTokenizer(jsonFacetString);
        List<Facet> facets = perDimFacets.get(dimension);
        Map<String, Facet> facetsMap = new LinkedHashMap<>();
        if (facets != null) {
            for (Facet facet : facets) {
                if (!facetsMap.containsKey(facet.getLabel())) {
                    facetsMap.put(facet.getLabel(), facet);
                }
            }
        }
        int c;
        String label = null;
        int count;
        while ((c = jsopTokenizer.read()) != JsopReader.END) {
            if (JsopReader.STRING == c) {
                label = jsopTokenizer.getEscapedToken();
            } else if (JsopReader.NUMBER == c) {
                count = Integer.parseInt(jsopTokenizer.getEscapedToken());
                if (label != null) {
                    if (facetsMap.containsKey(label)) {
                        count += facetsMap.get(label).getCount();
                    }
                    facetsMap.put(label, new Facet(label, count));
                }
                label = null;
            }
        }
        facets = new ArrayList<>(facetsMap.values());
        Collections.sort(facets, reverseOrder(comparingInt(Facet::getCount)));
        perDimFacets.put(dimension, facets);
    }

    @NotNull
    public Set<String> getDimensions() {
        return perDimFacets.keySet();
    }

    @Nullable
    public List<Facet> getFacets(@NotNull String dimension) {
        return perDimFacets.get(dimension);
    }

    /**
     * A query result facet, composed by its label and count.
     */
    public static class Facet {

        private final String label;
        private final int count;

        Facet(String label, int count) {
            this.label = label;
            this.count = count;
        }

        /**
         * get the facet label
         * @return a label
         */
        @NotNull
        public String getLabel() {
            return label;
        }

        /**
         * get the facet count
         * @return an integer
         */
        public int getCount() {
            return count;
        }
    }

    public interface FacetResultRow {
        String getValue(String columnName) throws Exception;
    }
}

