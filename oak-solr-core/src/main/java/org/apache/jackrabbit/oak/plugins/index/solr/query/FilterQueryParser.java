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
import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.plugins.index.solr.util.SolrUtils.getSortingField;
import static org.apache.jackrabbit.oak.plugins.index.solr.util.SolrUtils.partialEscape;

/**
 * the {@link FilterQueryParser} can parse {@link org.apache.jackrabbit.oak.spi.query.Filter}s
 * and transform them into {@link org.apache.solr.client.solrj.SolrQuery}s and / or Solr query {@code String}s.
 */
class FilterQueryParser {

    private static final Logger log = LoggerFactory.getLogger(FilterQueryParser.class);

    static SolrQuery getQuery(Filter filter, QueryIndex.IndexPlan plan, OakSolrConfiguration configuration) {

        SolrQuery solrQuery = new SolrQuery();
        setDefaults(solrQuery, configuration);

        StringBuilder queryBuilder = new StringBuilder();

        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft != null) {
            queryBuilder.append(parseFullTextExpression(ft, configuration));
            queryBuilder.append(' ');
        } else if (filter.getFulltextConditions() != null) {
            Collection<String> fulltextConditions = filter.getFulltextConditions();
            for (String fulltextCondition : fulltextConditions) {
                queryBuilder.append(fulltextCondition).append(" ");
            }
        }

        List<QueryIndex.OrderEntry> sortOrder = plan.getSortOrder();
        if (sortOrder != null) {
            for (QueryIndex.OrderEntry orderEntry : sortOrder) {
                SolrQuery.ORDER order;
                if (QueryIndex.OrderEntry.Order.ASCENDING.equals(orderEntry.getOrder())) {
                    order = SolrQuery.ORDER.asc;
                } else {
                    order = SolrQuery.ORDER.desc;
                }
                String sortingField;
                if (JcrConstants.JCR_PATH.equals(orderEntry.getPropertyName())) {
                    sortingField = partialEscape(configuration.getPathField()).toString();
                } else if (JcrConstants.JCR_SCORE.equals(orderEntry.getPropertyName())) {
                    sortingField = "score";
                } else {
                    if (orderEntry.getPropertyName().indexOf('/') >= 0) {
                        log.warn("cannot sort on relative properties, ignoring {} clause", orderEntry);
                        continue; // sorting by relative properties not supported until index time aggregation is supported
                    }
                    sortingField = partialEscape(getSortingField(orderEntry.getPropertyType().tag(), orderEntry.getPropertyName())).toString();
                }
                solrQuery.addOrUpdateSort(sortingField, order);
            }
        }

        Collection<Filter.PropertyRestriction> propertyRestrictions = filter.getPropertyRestrictions();
        if (propertyRestrictions != null && !propertyRestrictions.isEmpty()) {
            for (Filter.PropertyRestriction pr : propertyRestrictions) {
                if (pr.isNullRestriction()) {
                    // can not use full "x is null"
                    continue;
                }
                // facets
                if (QueryConstants.REP_FACET.equals(pr.propertyName)) {
                    solrQuery.setFacetMinCount(1);
                    solrQuery.setFacet(true);
                    String value = pr.first.getValue(Type.STRING);
                    solrQuery.addFacetField(value.substring(QueryConstants.REP_FACET.length() + 1, value.length() - 1) + "_facet");
                }

                // native query support
                if (SolrQueryIndex.NATIVE_SOLR_QUERY.equals(pr.propertyName) || SolrQueryIndex.NATIVE_LUCENE_QUERY.equals(pr.propertyName)) {
                    String nativeQueryString = String.valueOf(pr.first.getValue(pr.first.getType()));
                    if (isSupportedHttpRequest(nativeQueryString)) {
                        // pass through the native HTTP Solr request
                        String requestHandlerString = nativeQueryString.substring(0, nativeQueryString.indexOf('?'));
                        if (!"select".equals(requestHandlerString)) {
                            if (requestHandlerString.charAt(0) != '/') {
                                requestHandlerString = "/" + requestHandlerString;
                            }
                            solrQuery.setRequestHandler(requestHandlerString);
                        }
                        String parameterString = nativeQueryString.substring(nativeQueryString.indexOf('?') + 1);
                        for (String param : parameterString.split("&")) {
                            String[] kv = param.split("=");
                            if (kv.length != 2) {
                                throw new RuntimeException("Unparsable native HTTP Solr query");
                            } else {
                                // more like this
                                if ("/mlt".equals(requestHandlerString)) {
                                    if ("stream.body".equals(kv[0])) {
                                        kv[0] = "q";
                                        String mltFlString = "mlt.fl=";
                                        int mltFlIndex = parameterString.indexOf(mltFlString);
                                        if (mltFlIndex > -1) {
                                            int beginIndex = mltFlIndex + mltFlString.length();
                                            int endIndex = parameterString.indexOf('&', beginIndex);
                                            String fields;
                                            if (endIndex > beginIndex) {
                                                fields = parameterString.substring(beginIndex, endIndex);
                                            } else {
                                                fields = parameterString.substring(beginIndex);
                                            }
                                            kv[1] = "_query_:\"{!dismax qf=" + fields + " q.op=OR}" + kv[1] + "\"";
                                        }
                                    }
                                    if ("mlt.fl".equals(kv[0]) && ":path".equals(kv[1])) {
                                        // rep:similar passes the path of the node to find similar documents for in the :path
                                        // but needs its indexed content to find similar documents
                                        kv[1] = configuration.getCatchAllField();
                                    }
                                }
                                if ("/spellcheck".equals(requestHandlerString)) {
                                    if ("term".equals(kv[0])) {
                                        kv[0] = "spellcheck.q";
                                    }
                                    solrQuery.setParam("spellcheck", true);
                                }
                                if ("/suggest".equals(requestHandlerString)) {
                                    if ("term".equals(kv[0])) {
                                        kv[0] = "suggest.q";
                                    }
                                    solrQuery.setParam("suggest", true);
                                }
                                solrQuery.setParam(kv[0], kv[1]);
                            }
                        }
                        return solrQuery;
                    } else {
                        queryBuilder.append(nativeQueryString);
                    }
                } else {
                    if (SolrQueryIndex.isIgnoredProperty(pr, configuration)) {
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
                        if (pr.first != null && pr.last != null && pr.first.equals(pr.last)) {
                            queryBuilder.append(fieldName).append(':');
                            queryBuilder.append(first);
                        } else if (pr.first == null && pr.last == null) {
                            if (!queryBuilder.toString().contains(fieldName + ":")) {
                                queryBuilder.append(fieldName).append(':');
                                queryBuilder.append('*');
                            }
                        } else if ((pr.first != null && pr.last == null) || (pr.last != null && pr.first == null) || (!pr.first.equals(pr.last))) {
                            // TODO : need to check if this works for all field types (most likely not!)
                            queryBuilder.append(fieldName).append(':');
                            queryBuilder.append(createRangeQuery(first, last, pr.firstIncluding, pr.lastIncluding));
                        } else if (pr.isLike) {
                            // TODO : the current parameter substitution is not expected to work well
                            queryBuilder.append(fieldName).append(':');
                            queryBuilder.append(partialEscape(String.valueOf(pr.first.getValue(pr.first.getType())).replace('%', '*').replace('_', '?')));
                        } else {
                            throw new RuntimeException("[unexpected!] not handled case");
                        }
                    }
                }
                queryBuilder.append(" ");
            }
        }

        if (configuration.useForPrimaryTypes()) {
            String[] pts = filter.getPrimaryTypes().toArray(new String[filter.getPrimaryTypes().size()]);
            StringBuilder ptQueryBuilder = new StringBuilder();
            for (int i = 0; i < pts.length; i++) {
                String pt = pts[i];
                if (i == 0) {
                    ptQueryBuilder.append("(");
                }
                if (i > 0 && i < pts.length) {
                    ptQueryBuilder.append("OR ");
                }
                ptQueryBuilder.append("jcr\\:primaryType").append(':').append(partialEscape(pt)).append(" ");
                if (i == pts.length - 1) {
                    ptQueryBuilder.append(")");
                    ptQueryBuilder.append(' ');
                }
            }
            solrQuery.addFilterQuery(ptQueryBuilder.toString());
        }

        if (filter.getQueryStatement() != null && filter.getQueryStatement().contains(QueryConstants.REP_EXCERPT)) {
            if (!solrQuery.getHighlight()) {
                // enable highlighting
                solrQuery.setHighlight(true);
                // defaults
                solrQuery.set("hl.fl", "*");
                solrQuery.set("hl.encoder", "html");
                solrQuery.set("hl.mergeContiguous", true);
                solrQuery.setHighlightSimplePre("<strong>");
                solrQuery.setHighlightSimplePost("</strong>");
            }
        }

        if (configuration.useForPathRestrictions()) {
            Filter.PathRestriction pathRestriction = filter.getPathRestriction();
            if (pathRestriction != null) {
                String path = purgePath(filter, plan.getPathPrefix());
                String fieldName = configuration.getFieldForPathRestriction(pathRestriction);
                if (fieldName != null) {
                    if (pathRestriction.equals(Filter.PathRestriction.ALL_CHILDREN)) {
                        solrQuery.addFilterQuery(fieldName + ':' + path);
                    } else {
                        queryBuilder.append(fieldName);
                        queryBuilder.append(':');
                        queryBuilder.append(path);
                    }
                }
            }
        }

        if (configuration.collapseJcrContentNodes()) {
            solrQuery.addFilterQuery("{!collapse field=" + configuration.getCollapsedPathField() + " min=" +
                    configuration.getPathDepthField() + " hint=top_fc nullPolicy=expand}");
        }

        if (queryBuilder.length() == 0) {
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

    private static String parseFullTextExpression(FullTextExpression ft, final OakSolrConfiguration configuration) {
        final StringBuilder fullTextString = new StringBuilder();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextOr or) {
                fullTextString.append('(');
                for (int i = 0; i < or.list.size(); i++) {
                    if (i > 0 && i < or.list.size()) {
                        fullTextString.append(" OR ");
                    }
                    FullTextExpression e = or.list.get(i);
                    String orTerm = parseFullTextExpression(e, configuration);
                    fullTextString.append(orTerm);
                }
                fullTextString.append(')');
                fullTextString.append(' ');
                return true;
            }

            @Override
            public boolean visit(FullTextContains contains) {
                return contains.getBase().accept(this);
            }

            @Override
            public boolean visit(FullTextAnd and) {
                fullTextString.append('(');
                for (int i = 0; i < and.list.size(); i++) {
                    if (i > 0 && i < and.list.size()) {
                        fullTextString.append(" AND ");
                    }
                    FullTextExpression e = and.list.get(i);
                    String andTerm = parseFullTextExpression(e, configuration);
                    fullTextString.append(andTerm);
                }
                fullTextString.append(')');
                fullTextString.append(' ');
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                if (term.isNot()) {
                    fullTextString.append('-');
                }
                String p = term.getPropertyName();
                if (p != null && p.indexOf('/') >= 0) {
                    p = getName(p);
                }
                if (p == null || "*".equals(p)) {
                    p = configuration.getCatchAllField();
                }
                fullTextString.append(partialEscape(p));
                fullTextString.append(':');
                String termText = term.getText();
                if (termText.indexOf(' ') > 0) {
                    fullTextString.append('"');
                }
                fullTextString.append(termText.replace("/", "\\/").replace(":", "\\:"));
                if (termText.indexOf(' ') > 0) {
                    fullTextString.append('"');
                }
                String boost = term.getBoost();
                if (boost != null) {
                    fullTextString.append('^');
                    fullTextString.append(boost);
                }
                fullTextString.append(' ');
                return true;
            }
        });
        return fullTextString.toString();
    }

    private static boolean isSupportedHttpRequest(String nativeQueryString) {
        // the query string starts with ${supported-handler.selector}?
        return nativeQueryString.matches("(suggest|spellcheck|mlt|query|select|get)\\\\?.*");
    }

    private static void setDefaults(SolrQuery solrQuery, OakSolrConfiguration configuration) {
        solrQuery.setParam("q.op", "AND");
        solrQuery.setParam("fl", configuration.getPathField() + " score");
        String catchAllField = configuration.getCatchAllField();
        if (catchAllField != null && catchAllField.length() > 0) {
            solrQuery.setParam("df", catchAllField);
        }

        solrQuery.setParam("rows", String.valueOf(configuration.getRows()));
    }

    private static String createRangeQuery(String first, String last, boolean firstIncluding, boolean lastIncluding) {
        // TODO : handle inclusion / exclusion of bounds
        return "[" + (first != null ? first : "*") + " TO " + (last != null ? last : "*") + "]";
    }

    private static String purgePath(Filter filter, String pathPrefix) {
        String path = filter.getPath();
        if (pathPrefix != null && pathPrefix.length() > 1 && path.startsWith(pathPrefix)) {
            path = path.substring(pathPrefix.length());
            if (path.length() == 0) {
                path = "/";
            }
        }
        return partialEscape(path).toString();
    }

}
