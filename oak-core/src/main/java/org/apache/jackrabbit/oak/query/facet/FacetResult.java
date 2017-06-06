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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;

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
                for (String column : queryResult.getColumnNames()) {
                    if (column.startsWith(QueryConstants.REP_FACET)) {
                        String dimension = column.substring(QueryConstants.REP_FACET.length() + 1, column.length() - 1);
                        Value value = row.getValue(column);
                        if (value != null) {
                            String jsonFacetString = value.getString();
                            parseJson(dimension, jsonFacetString);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void parseJson(String dimension, String jsonFacetString) {
        JsopTokenizer jsopTokenizer = new JsopTokenizer(jsonFacetString);
        List<Facet> facets = new LinkedList<Facet>();
        int c;
        String label = null;
        int count;
        while ((c = jsopTokenizer.read()) != JsopReader.END) {
            if (JsopReader.STRING == c) {
                label = jsopTokenizer.getEscapedToken();
            } else if (JsopReader.NUMBER == c) {
                count = Integer.parseInt(jsopTokenizer.getEscapedToken());
                if (label != null) {
                    facets.add(new Facet(label, count));
                }
                label = null;
            }
        }
        perDimFacets.put(dimension, facets);
    }

    @Nonnull
    public Set<String> getDimensions() {
        return perDimFacets.keySet();
    }

    @CheckForNull
    public List<Facet> getFacets(@Nonnull String dimension) {
        return perDimFacets.get(dimension);
    }

    /**
     * A query result facet, composed by its label and count.
     */
    public static class Facet {

        private final String label;
        private final int count;

        private Facet(String label, int count) {
            this.label = label;
            this.count = count;
        }

        /**
         * get the facet label
         * @return a label
         */
        @Nonnull
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
}

