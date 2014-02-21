/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import java.util.Collection;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Solr based {@link QueryIndex}
 */
public class SolrQueryIndex implements QueryIndex {

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndex.class);
    public static final String TYPE = "solr";

    private final String name;
    private final SolrServer solrServer;
    private final OakSolrConfiguration configuration;

    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration) {
        this.name = name;
        this.solrServer = solrServer;
        this.configuration = configuration;
    }

    @Override
    public String getIndexName() {
        return name;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        // TODO : estimate no of returned values and 0 is not good for no restrictions
        return (filter.getPropertyRestrictions() != null ? filter.getPropertyRestrictions().size() * 0.1 : 0)
                + (filter.getFulltextConditions() != null ? filter.getFulltextConditions().size() * 0.01 : 0)
                + (filter.getPathRestriction() != null ? 0.2 : 0);
    }

    @Override
    public String getPlan(Filter filter, NodeState nodeState) {
        return getQuery(filter).toString();
    }

    private SolrQuery getQuery(Filter filter) {

        SolrQuery solrQuery = new SolrQuery();
        setDefaults(solrQuery);

        StringBuilder queryBuilder = new StringBuilder();

        for (String pt : filter.getPrimaryTypes()) {
            queryBuilder.append("jcr\\:primaryType").append(':').append(partialEscape(pt));
        }

        Filter.PathRestriction pathRestriction = filter.getPathRestriction();
        if (pathRestriction != null) {
            String path = purgePath(filter);
            String fieldName = configuration.getFieldForPathRestriction(pathRestriction);
            if (fieldName != null) {
                queryBuilder.append(fieldName);
                queryBuilder.append(':');
                queryBuilder.append(path);
                queryBuilder.append(" ");
            }
        }
        Collection<Filter.PropertyRestriction> propertyRestrictions = filter.getPropertyRestrictions();
        if (propertyRestrictions != null && !propertyRestrictions.isEmpty()) {
            for (Filter.PropertyRestriction pr : propertyRestrictions) {
                if (pr.propertyName.contains("/")) {
                    // cannot handle child-level property restrictions
                    continue;
                }
                String first = null;
                if (pr.first != null) {
                    first = partialEscape(String.valueOf(pr.first.getValue(pr.first.getType()))).toString();
                }
                String last = null;
                if (pr.last != null) {
                    last = partialEscape(String.valueOf(pr.last.getValue(pr.last.getType()))).toString();
                }

                String prField = configuration.getFieldForPropertyRestriction(pr);
                CharSequence fieldName = partialEscape(prField != null ?
                        prField : pr.propertyName);
                if ("jcr\\:path".equals(fieldName.toString())) {
                    queryBuilder.append(configuration.getPathField());
                    queryBuilder.append(':');
                    queryBuilder.append(first);
                } else {
                    queryBuilder.append(fieldName).append(':');
                    if (pr.first != null && pr.last != null && pr.first.equals(pr.last)) {
                        queryBuilder.append(first);
                    } else if (pr.first == null && pr.last == null) {
                        queryBuilder.append('*');
                    } else if ((pr.first != null && pr.last == null) || (pr.last != null && pr.first == null) || (!pr.first.equals(pr.last))) {
                        // TODO : need to check if this works for all field types (most likely not!)
                        queryBuilder.append(createRangeQuery(first, last, pr.firstIncluding, pr.lastIncluding));
                    } else if (pr.isLike) {
                        // TODO : the current parameter substitution is not expected to work well
                        queryBuilder.append(partialEscape(String.valueOf(pr.first.getValue(pr.first.getType())).replace('%', '*').replace('_', '?')));
                    } else {
                        throw new RuntimeException("[unexpected!] not handled case");
                    }
                }
                queryBuilder.append(" ");
            }
        }

        Collection<String> fulltextConditions = filter.getFulltextConditions();
        for (String fulltextCondition : fulltextConditions) {
            queryBuilder.append(fulltextCondition).append(" ");
        }
        if(queryBuilder.length() == 0) {
            queryBuilder.append("*:*");
        }
        String escapedQuery = queryBuilder.toString();
        solrQuery.setQuery(escapedQuery);

        if (log.isDebugEnabled()) {
            log.debug("JCR query {} has been converted to Solr query {}",
                    filter.getQueryStatement(), solrQuery.toString());
        }

        return solrQuery;
    }

    private void setDefaults(SolrQuery solrQuery) {
        solrQuery.setParam("q.op", "AND");

        // TODO : change this to be not hard coded
        solrQuery.setParam("df", "catch_all");

        // TODO : can we handle this better?
        solrQuery.setParam("rows", String.valueOf(Integer.MAX_VALUE));
    }

    private static String createRangeQuery(String first, String last, boolean firstIncluding, boolean lastIncluding) {
        // TODO : handle inclusion / exclusion of bounds
        return "[" + (first != null ? first : "*") + " TO " + (last != null ? last : "*") + "]";
    }

    private static String purgePath(Filter filter) {
        return partialEscape(filter.getPath()).toString();
    }


    // partially borrowed from SolrPluginUtils#partialEscape
    private static CharSequence partialEscape(CharSequence s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '!' || c == '(' || c == ')' ||
                    c == ':' || c == '^' || c == '[' || c == ']' || c == '/' ||
                    c == '{' || c == '}' || c == '~' || c == '*' || c == '?' ||
                    c == '-' || c == ' ') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb;
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        Cursor cursor;
        try {
            SolrQuery query = getQuery(filter);
            if (log.isDebugEnabled()) {
                log.debug("sending query {}", query);
            }
            QueryResponse queryResponse = solrServer.query(query);
            cursor = new SolrCursor(queryResponse);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }


    private class SolrCursor implements Cursor {

        private final SolrDocumentList results;

        private int i;

        public SolrCursor(QueryResponse queryResponse) {
            this.results = queryResponse.getResults();
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return results != null && i < results.size();
        }

        @Override
        public void remove() {
            results.remove(i);
        }

        public IndexRow next() {
            if (i < results.size()) {
                final SolrDocument doc = results.get(i);
                i++;
                return new IndexRow() {
                    @Override
                    public String getPath() {
                        return String.valueOf(doc.getFieldValue(
                                configuration.getPathField()));
                    }

                    @Override
                    public PropertyValue getValue(String columnName) {
                        Object o = doc.getFieldValue(columnName);
                        return o == null ? null : PropertyValues.newString(o.toString());
                    }

                };
            } else {
                return null;
            }
        }
    }
}
