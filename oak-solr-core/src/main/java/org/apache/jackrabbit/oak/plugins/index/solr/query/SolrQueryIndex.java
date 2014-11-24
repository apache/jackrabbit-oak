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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.CheckForNull;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.getAncestorPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

/**
 * A Solr based {@link QueryIndex}
 */
public class SolrQueryIndex implements FulltextQueryIndex {

    private static final String NATIVE_SOLR_QUERY = "native*solr";
    private static final String NATIVE_LUCENE_QUERY = "native*lucene";

    public static final String TYPE = "solr";

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndex.class);

    private final String name;
    private final SolrServer solrServer;
    private final OakSolrConfiguration configuration;

    private final NodeAggregator aggregator;

    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration) {
        this.name = name;
        this.solrServer = solrServer;
        this.configuration = configuration;
        // TODO this index should support aggregation in the same way as the Lucene index
        this.aggregator = null;
    }

    @Override
    public String getIndexName() {
        return name;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        // cost is inverse proportional to the number of matching restrictions, infinite if no restriction matches
        return 10d / getMatchingFilterRestrictions(filter);
    }

    private int getMatchingFilterRestrictions(Filter filter) {
        int match = 0;

        // full text expressions OR full text conditions defined
        if (filter.getFullTextConstraint() != null || (filter.getFulltextConditions() != null
                && filter.getFulltextConditions().size() > 0)) {
            match++; // full text queries have usually a significant recall
        }

        // path restriction defined AND path restrictions handled
        if (filter.getPathRestriction() != null &&
                !Filter.PathRestriction.NO_RESTRICTION.equals(filter.getPathRestriction())
                && configuration.useForPathRestrictions()) {
            match++;
        }

        // primary type restriction defined AND primary type restriction handled
        if (filter.getPrimaryTypes() != null && filter.getPrimaryTypes().size() > 0
                && configuration.useForPrimaryTypes()) {
            match++;
        }

        // property restriction OR native language property restriction defined AND property restriction handled
        if (filter.getPropertyRestrictions() != null && filter.getPropertyRestrictions().size() > 0
                && (filter.getPropertyRestriction(NATIVE_SOLR_QUERY) != null || filter.getPropertyRestriction(NATIVE_LUCENE_QUERY) != null
                || configuration.useForPropertyRestrictions()) && !hasIgnoredProperties(filter.getPropertyRestrictions())) {
            match++;
        }

        return match;
    }

    private boolean hasIgnoredProperties(Collection<Filter.PropertyRestriction> propertyRestrictions) {
        for (Filter.PropertyRestriction pr : propertyRestrictions) {
            if (configuration.getIgnoredProperties().contains(pr.propertyName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getPlan(Filter filter, NodeState nodeState) {
        return getQuery(filter).toString();
    }

    private SolrQuery getQuery(Filter filter) {

        SolrQuery solrQuery = new SolrQuery();
        setDefaults(solrQuery);

        StringBuilder queryBuilder = new StringBuilder();

        FullTextExpression ft = filter.getFullTextConstraint();
        if (ft != null) {
            queryBuilder.append(getFullTextQuery(ft));
            queryBuilder.append(' ');
        } else if (filter.getFulltextConditions() != null) {
            Collection<String> fulltextConditions = filter.getFulltextConditions();
            for (String fulltextCondition : fulltextConditions) {
                queryBuilder.append(fulltextCondition).append(" ");
            }
        }

        Collection<Filter.PropertyRestriction> propertyRestrictions = filter.getPropertyRestrictions();
        if (propertyRestrictions != null && !propertyRestrictions.isEmpty()) {
            for (Filter.PropertyRestriction pr : propertyRestrictions) {
                // native query support
                if (NATIVE_SOLR_QUERY.equals(pr.propertyName) || NATIVE_LUCENE_QUERY.equals(pr.propertyName)) {
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
                                solrQuery.setParam(kv[0], kv[1]);
                            }
                        }
                        return solrQuery; // every other restriction is not considered
                    } else {
                        queryBuilder.append(nativeQueryString);
                    }
                } else {
                    if (!configuration.useForPropertyRestrictions() // Solr index not used for properties
                            || pr.propertyName.contains("/") // no child-level property restrictions
                            || "rep:excerpt".equals(pr.propertyName) // rep:excerpt is handled by the query engine
                            || configuration.getIgnoredProperties().contains(pr.propertyName) // property is explicitly ignored
                            ) {
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
            for (int i = 0; i < pts.length; i++) {
                String pt = pts[i];
                if (i == 0) {
                    queryBuilder.append("(");
                }
                if (i > 0 && i < pts.length) {
                    queryBuilder.append("OR ");
                }
                queryBuilder.append("jcr\\:primaryType").append(':').append(partialEscape(pt)).append(" ");
                if (i == pts.length - 1) {
                    queryBuilder.append(")");
                    queryBuilder.append(' ');
                }
            }
        }

        if (configuration.useForPathRestrictions()) {
            Filter.PathRestriction pathRestriction = filter.getPathRestriction();
            if (pathRestriction != null) {
                String path = purgePath(filter);
                String fieldName = configuration.getFieldForPathRestriction(pathRestriction);
                if (fieldName != null) {
                    queryBuilder.append(fieldName);
                    queryBuilder.append(':');
                    queryBuilder.append(path);
                }
            }
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

    private String getFullTextQuery(FullTextExpression ft) {
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
                    String orTerm = getFullTextQuery(e);
                    fullTextString.append(orTerm);
                }
                fullTextString.append(')');
                fullTextString.append(' ');
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                fullTextString.append('(');
                for (int i = 0; i < and.list.size(); i++) {
                    if (i > 0 && i < and.list.size()) {
                        fullTextString.append(" AND ");
                    }
                    FullTextExpression e = and.list.get(i);
                    String andTerm = getFullTextQuery(e);
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

    /**
     * Get the set of relative paths of a full-text condition. For example, for
     * the condition "contains(a/b, 'hello') and contains(c/d, 'world'), the set
     * { "a", "c" } is returned. If there are no relative properties, then one
     * entry is returned (the empty string). If there is no expression, then an
     * empty set is returned.
     *
     * @param ft the full-text expression
     * @return the set of relative paths (possibly empty)
     */
    private static Set<String> getRelativePaths(FullTextExpression ft) {
        final HashSet<String> relPaths = new HashSet<String>();
        ft.accept(new FullTextVisitor.FullTextVisitorBase() {

            @Override
            public boolean visit(FullTextTerm term) {
                String p = term.getPropertyName();
                if (p == null) {
                    relPaths.add("");
                } else if (p.startsWith("../") || p.startsWith("./")) {
                    throw new IllegalArgumentException("Relative parent is not supported:" + p);
                } else if (getDepth(p) > 1) {
                    String parent = getParentPath(p);
                    relPaths.add(parent);
                } else {
                    relPaths.add("");
                }
                return true;
            }
        });
        return relPaths;
    }

    private boolean isSupportedHttpRequest(String nativeQueryString) {
        // the query string starts with ${supported-handler.selector}?
        return nativeQueryString.matches("(mlt|query|select|get)\\\\?.*");
    }

    private void setDefaults(SolrQuery solrQuery) {
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
    public Cursor query(final Filter filter, NodeState root) {
        Cursor cursor;
        try {
            final Set<String> relPaths = filter.getFullTextConstraint() != null ? getRelativePaths(filter.getFullTextConstraint()) : Collections.<String>emptySet();
            final String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();

            final int parentDepth = getDepth(parent);


            cursor = new SolrRowCursor(new AbstractIterator<SolrResultRow>() {
                private final Set<String> seenPaths = Sets.newHashSet();
                private final Deque<SolrResultRow> queue = Queues.newArrayDeque();
                private SolrDocument lastDoc;
                public int offset = 0;

                @Override
                protected SolrResultRow computeNext() {
                    while (!queue.isEmpty() || loadDocs()) {
                        return queue.remove();
                    }
                    return endOfData();
                }

                private SolrResultRow convertToRow(SolrDocument doc) throws IOException {
                    String path = String.valueOf(doc.getFieldValue(configuration.getPathField()));
                    if (path != null) {
                        if ("".equals(path)) {
                            path = "/";
                        }
                        if (!parent.isEmpty()) {
                            path = getAncestorPath(path, parentDepth);
                            // avoid duplicate entries
                            if (seenPaths.contains(path)) {
                                return null;
                            }
                            seenPaths.add(path);
                        }

                        float score = 0f;
                        Object scoreObj = doc.get("score");
                        if (scoreObj != null) {
                            score = (Float) scoreObj;
                        }
                        return new SolrResultRow(path, score, doc);
                    }
                    return null;
                }

                /**
                 * Loads the Solr documents in batches
                 * @return true if any document is loaded
                 */
                private boolean loadDocs() {
                    SolrDocument lastDocToRecord = null;

                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("converting filter {}", filter);
                        }
                        SolrQuery query = getQuery(filter);
                        if (lastDoc != null) {
                            offset++;
                            int newOffset = offset * configuration.getRows();
                            query.setParam("start", String.valueOf(newOffset));
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("sending query {}", query);
                        }
                        SolrDocumentList docs = solrServer.query(query).getResults();

                        if (log.isDebugEnabled()) {
                            log.debug("getting docs {}", docs);
                        }

                        for (SolrDocument doc : docs) {
                            SolrResultRow row = convertToRow(doc);
                            if (row != null) {
                                queue.add(row);
                            }
                            lastDocToRecord = doc;
                        }
                    } catch (Exception e) {
                        if (log.isWarnEnabled()) {
                            log.warn("query via {} failed.", solrServer, e);
                        }
                    }
                    if (lastDocToRecord != null) {
                        this.lastDoc = lastDocToRecord;
                    }

                    return !queue.isEmpty();
                }

            }, filter.getQueryEngineSettings());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }

    static class SolrResultRow {
        final String path;
        final double score;
        SolrDocument doc;

        SolrResultRow(String path, double score) {
            this.path = path;
            this.score = score;
        }

        SolrResultRow(String path, double score, SolrDocument doc) {
            this.path = path;
            this.score = score;
            this.doc = doc;
        }

        @Override
        public String toString() {
            return String.format("%s (%1.2f)", path, score);
        }
    }

    /**
     * A cursor over Solr results. The result includes the path and the jcr:score pseudo-property as returned by Solr,
     * plus, eventually, the returned stored values if {@link org.apache.solr.common.SolrDocument} is included in the
     * {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex.SolrResultRow}.
     */
    static class SolrRowCursor implements Cursor {

        private final Cursor pathCursor;
        SolrResultRow currentRow;

        SolrRowCursor(final Iterator<SolrResultRow> it, QueryEngineSettings settings) {
            Iterator<String> pathIterator = new Iterator<String>() {

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public String next() {
                    currentRow = it.next();
                    return currentRow.path;
                }

                @Override
                public void remove() {
                    it.remove();
                }

            };
            pathCursor = new Cursors.PathCursor(pathIterator, true, settings);
        }


        @Override
        public boolean hasNext() {
            return pathCursor.hasNext();
        }

        @Override
        public void remove() {
            pathCursor.remove();
        }

        @Override
        public IndexRow next() {
            final IndexRow pathRow = pathCursor.next();
            return new IndexRow() {

                @Override
                public String getPath() {
                    return pathRow.getPath();
                }

                @Override
                public PropertyValue getValue(String columnName) {
                    // overlay the score
                    if (QueryImpl.JCR_SCORE.equals(columnName)) {
                        return PropertyValues.newDouble(currentRow.score);
                    }
                    // TODO : make inclusion of doc configurable
                    return currentRow.doc != null ? PropertyValues.newString(
                            String.valueOf(currentRow.doc.getFieldValue(columnName))) : null;
                }

            };
        }
    }

    @Override
    @CheckForNull
    public NodeAggregator getNodeAggregator() {
        return aggregator;
    }

}
