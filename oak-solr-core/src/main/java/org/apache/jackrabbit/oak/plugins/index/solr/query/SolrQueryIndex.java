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

import javax.annotation.CheckForNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.*;

/**
 * A Solr based {@link QueryIndex}
 */
public class SolrQueryIndex implements FulltextQueryIndex, QueryIndex.AdvanceFulltextQueryIndex {

    public static final String TYPE = "solr";

    static final String NATIVE_SOLR_QUERY = "native*solr";

    static final String NATIVE_LUCENE_QUERY = "native*lucene";

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndex.class);

    private final String name;
    private final SolrServer solrServer;
    private final OakSolrConfiguration configuration;

    private final NodeAggregator aggregator;
    private final LMSEstimator estimator;


    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration, NodeAggregator aggregator, LMSEstimator estimator) {
        this.name = name;
        this.solrServer = solrServer;
        this.configuration = configuration;
        this.aggregator = aggregator;
        this.estimator = estimator;
    }

    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration, NodeAggregator aggregator) {
        this(name, solrServer, configuration, aggregator, new LMSEstimator());
    }

    public SolrQueryIndex(String name, SolrServer solrServer, OakSolrConfiguration configuration) {
        this(name, solrServer, configuration, null, new LMSEstimator());
    }

    @Override
    public String getIndexName() {
        return name;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        // cost is inverse proportional to the number of matching restrictions, infinite if no restriction matches
        double cost = 10d / getMatchingFilterRestrictions(filter);
        if (log.isDebugEnabled()) {
            log.debug("Solr: cost for {} is {}", name, cost);
        }
        return cost;
    }

    int getMatchingFilterRestrictions(Filter filter) {
        int match = 0;

        // full text expressions OR full text conditions defined
        if (filter.getFullTextConstraint() != null || (filter.getFulltextConditions() != null
                && filter.getFulltextConditions().size() > 0)) {
            match++; // full text queries have usually a significant recall
        }

        // property restriction OR native language property restriction defined AND property restriction handled
        if (filter.getPropertyRestrictions() != null
                && filter.getPropertyRestrictions().size() > 0
                && (filter.getPropertyRestriction(NATIVE_SOLR_QUERY) != null
                || filter.getPropertyRestriction(NATIVE_LUCENE_QUERY) != null
                || configuration.useForPropertyRestrictions())
                && !hasIgnoredProperties(filter.getPropertyRestrictions(), configuration)) {
            match++;
        }

        // path restriction defined AND path restrictions handled
        if (filter.getPathRestriction() != null &&
                !Filter.PathRestriction.NO_RESTRICTION.equals(filter.getPathRestriction())
                && configuration.useForPathRestrictions()) {
            if (match > 0) {
                match++;
            }
        }

        // primary type restriction defined AND primary type restriction handled
        if (filter.getPrimaryTypes() != null && filter.getPrimaryTypes().size() > 0
                && configuration.useForPrimaryTypes()) {
            if (match > 0) {
                match++;
            }
        }


        return match;
    }

    private static boolean hasIgnoredProperties(Collection<Filter.PropertyRestriction> propertyRestrictions, OakSolrConfiguration configuration) {
        for (Filter.PropertyRestriction pr : propertyRestrictions) {
            if (isIgnoredProperty(pr, configuration)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getPlan(Filter filter, NodeState nodeState) {
        return FilterQueryParser.getQuery(filter, null, configuration).toString();
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

    @Override
    public Cursor query(final IndexPlan plan, final NodeState root) {
        return query(plan, plan.getSortOrder(), root);
    }

    private Cursor query(IndexPlan plan, List<OrderEntry> sortOrder, NodeState root) {
        Cursor cursor;
        try {
            Filter filter = plan.getFilter();
            final Set<String> relPaths = filter.getFullTextConstraint() != null ? getRelativePaths(filter.getFullTextConstraint())
                    : Collections.<String>emptySet();
            final String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();

            final int parentDepth = getDepth(parent);

            AbstractIterator<SolrResultRow> iterator = getIterator(filter, sortOrder, parent, parentDepth);

            cursor = new SolrRowCursor(iterator, plan, filter.getQueryEngineSettings());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }

    private AbstractIterator<SolrResultRow> getIterator(final Filter filter, final List<OrderEntry> sortOrder, final String parent, final int parentDepth) {
        return new AbstractIterator<SolrResultRow>() {
            private final Set<String> seenPaths = Sets.newHashSet();
            private final Deque<SolrResultRow> queue = Queues.newArrayDeque();
            private SolrDocument lastDoc;
            private int offset = 0;
            private boolean noDocs = false;
            public long numFound = 0;

            @Override
            protected SolrResultRow computeNext() {
                if (!queue.isEmpty() || loadDocs()) {
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

                if (noDocs) {
                    return false;
                }

                try {
                    if (log.isDebugEnabled()) {
                        log.debug("converting filter {}", filter);
                    }
                    SolrQuery query = FilterQueryParser.getQuery(filter, sortOrder, configuration);
                    if (numFound > 0) {
                        long rows = configuration.getRows();
                        long maxQueries = numFound / 2;
                        if (maxQueries > configuration.getRows()) {
                            // adjust the rows to avoid making more than 3 Solr requests for this particular query
                            rows = maxQueries;
                            query.setParam("rows", String.valueOf(rows));
                        }
                        long newOffset = configuration.getRows() + offset * rows;
                        if (newOffset >= numFound) {
                            return false;
                        }
                        query.setParam("start", String.valueOf(newOffset));
                        offset++;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("sending query {}", query);
                    }
                    QueryResponse queryResponse = solrServer.query(query);

                    SolrDocumentList docs = queryResponse.getResults();

                    if (log.isDebugEnabled()) {
                        log.debug("getting response {}", queryResponse.getHeader());
                    }

                    if (docs != null) {
                        onRetrievedDocs(filter, docs);
                        numFound = docs.getNumFound();

                        Map<String, Map<String, List<String>>> highlighting = queryResponse.getHighlighting();
                        for (SolrDocument doc : docs) {
                            // handle highlight
                            if (highlighting != null) {
                                Object pathObject = doc.getFieldValue(configuration.getPathField());
                                if (pathObject != null && highlighting.get(String.valueOf(pathObject)) != null) {
                                    Map<String, List<String>> value = highlighting.get(String.valueOf(pathObject));
                                    for (Map.Entry<String, List<String>> entry : value.entrySet()) {
                                        // all highlighted values end up in 'rep:excerpt', regardless of field match
                                        for (String v : entry.getValue()) {
                                            doc.addField(QueryImpl.REP_EXCERPT, v);
                                        }
                                    }
                                }
                            }
                            SolrResultRow row = convertToRow(doc);
                            if (row != null) {
                                queue.add(row);
                            }
                        }
                    }

                    // handle spellcheck
                    SpellCheckResponse spellCheckResponse = queryResponse.getSpellCheckResponse();
                    if (spellCheckResponse != null && spellCheckResponse.getSuggestions() != null &&
                            spellCheckResponse.getSuggestions().size() > 0) {
                        SolrDocument fakeDoc = getSpellChecks(spellCheckResponse, filter);
                        queue.add(new SolrResultRow("/", 1.0, fakeDoc));
                        noDocs = true;
                    }

                    // handle suggest
                    NamedList<Object> response = queryResponse.getResponse();
                    Map suggest = (Map) response.get("suggest");
                    if (suggest != null) {
                        Set<Map.Entry<String, Object>> suggestEntries = suggest.entrySet();
                        if (!suggestEntries.isEmpty()) {
                            SolrDocument fakeDoc = getSuggestions(suggestEntries, filter);
                            queue.add(new SolrResultRow("/", 1.0, fakeDoc));
                            noDocs = true;
                        }
                    }

                } catch (Exception e) {
                    if (log.isWarnEnabled()) {
                        log.warn("query via {} failed.", solrServer, e);
                    }
                }

                return !queue.isEmpty();
            }

        };
    }

    private SolrDocument getSpellChecks(SpellCheckResponse spellCheckResponse, Filter filter) throws SolrServerException {
        SolrDocument fakeDoc = new SolrDocument();
        List<SpellCheckResponse.Suggestion> suggestions = spellCheckResponse.getSuggestions();
        Collection<String> alternatives = new ArrayList<String>(suggestions.size());
        for (SpellCheckResponse.Suggestion suggestion : suggestions) {
            alternatives.addAll(suggestion.getAlternatives());
        }

        // ACL filter spellcheck results
        for (String alternative : alternatives) {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setParam("q", alternative);
            solrQuery.setParam("df", configuration.getCatchAllField());
            solrQuery.setParam("q.op", "AND");
            solrQuery.setParam("rows", "100");
            QueryResponse suggestQueryResponse = solrServer.query(solrQuery);
            SolrDocumentList results = suggestQueryResponse.getResults();
            if (results != null && results.getNumFound() > 0) {
                for (SolrDocument doc : results) {
                    if (filter.isAccessible(String.valueOf(doc.getFieldValue(configuration.getPathField())))) {
                        fakeDoc.addField(QueryImpl.REP_SPELLCHECK, alternative);
                        break;
                    }
                }
            }
        }

        return fakeDoc;
    }

    private SolrDocument getSuggestions(Set<Map.Entry<String, Object>> suggestEntries, Filter filter) throws SolrServerException {
        Collection<SimpleOrderedMap<Object>> retrievedSuggestions = new HashSet<SimpleOrderedMap<Object>>();
        SolrDocument fakeDoc = new SolrDocument();
        for (Map.Entry<String, Object> suggestor : suggestEntries) {
            SimpleOrderedMap<Object> suggestionResponses = ((SimpleOrderedMap) suggestor.getValue());
            for (Map.Entry<String, Object> suggestionResponse : suggestionResponses) {
                SimpleOrderedMap<Object> suggestionResults = ((SimpleOrderedMap) suggestionResponse.getValue());
                for (Map.Entry<String, Object> suggestionResult : suggestionResults) {
                    if ("suggestions".equals(suggestionResult.getKey())) {
                        ArrayList<SimpleOrderedMap<Object>> suggestions = ((ArrayList<SimpleOrderedMap<Object>>) suggestionResult.getValue());
                        if (!suggestions.isEmpty()) {
                            for (SimpleOrderedMap<Object> suggestion : suggestions) {
                                retrievedSuggestions.add(suggestion);
                            }
                        }
                    }
                }
            }
        }

        // ACL filter suggestions
        for (SimpleOrderedMap<Object> suggestion : retrievedSuggestions) {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.setParam("q", String.valueOf(suggestion.get("term")));
            solrQuery.setParam("df", configuration.getCatchAllField());
            solrQuery.setParam("q.op", "AND");
            solrQuery.setParam("rows", "100");
            QueryResponse suggestQueryResponse = solrServer.query(solrQuery);
            SolrDocumentList results = suggestQueryResponse.getResults();
            if (results != null && results.getNumFound() > 0) {
                for (SolrDocument doc : results) {
                    if (filter.isAccessible(String.valueOf(doc.getFieldValue(configuration.getPathField())))) {
                        fakeDoc.addField(QueryImpl.REP_SUGGEST, "{term=" + suggestion.get("term") + ",weight=" + suggestion.get("weight") + "}");
                        break;
                    }
                }
            }
        }
        return fakeDoc;
    }

    static boolean isIgnoredProperty(Filter.PropertyRestriction property, OakSolrConfiguration configuration) {
        if (NATIVE_LUCENE_QUERY.equals(property.propertyName) || NATIVE_SOLR_QUERY.equals(property.propertyName)) {
               return false;
        } else return (!configuration.useForPropertyRestrictions() // Solr index not used for properties
                        || (configuration.getUsedProperties().size() > 0 && !configuration.getUsedProperties().contains(property.propertyName)) // not explicitly contained in the used properties
                        || property.propertyName.contains("/") // no child-level property restrictions
                        || QueryImpl.REP_EXCERPT.equals(property.propertyName) // rep:excerpt is not handled at the property level
                        || QueryConstants.RESTRICTION_LOCAL_NAME.equals(property.propertyName)
                        || configuration.getIgnoredProperties().contains(property.propertyName));
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        // TODO : eventually provide multiple plans for (eventually) filtering by ACLs
        // TODO : eventually provide multiple plans for normal paging vs deep paging
        if (getMatchingFilterRestrictions(filter) > 0) {
            return Collections.singletonList(planBuilder(filter)
                    .setEstimatedEntryCount(estimator.estimate(filter))
                    .setSortOrder(sortOrder)
                    .build());
        } else {
            return Collections.emptyList();
        }
    }

    private IndexPlan.Builder planBuilder(Filter filter) {
        return new IndexPlan.Builder()
                .setCostPerExecution(solrServer instanceof EmbeddedSolrServer ? 1 : 2) // disk I/O + network I/O
                .setCostPerEntry(0.3) // with properly configured SolrCaches ~70% of the doc fetches should hit them
                .setFilter(filter)
                .setFulltextIndex(true)
                .setIncludesNodeData(true) // we currently include node data
                .setDelayed(true); //Solr is most usually async
    }

    void onRetrievedDocs(Filter filter, SolrDocumentList docs) {
        // estimator update
        estimator.update(filter, docs);
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        return plan.toString();
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        return query(planBuilder(filter).build(), null, rootState);
    }

    static class SolrResultRow {
        final String path;
        final double score;
        final SolrDocument doc;

        SolrResultRow(String path, double score) {
            this(path, score, null);
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
    private class SolrRowCursor implements Cursor {

        private final Cursor pathCursor;
        private final IndexPlan plan;

        SolrResultRow currentRow;

        SolrRowCursor(final Iterator<SolrResultRow> it, IndexPlan plan, QueryEngineSettings settings) {
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
            this.plan = plan;
            this.pathCursor = new Cursors.PathCursor(pathIterator, true, settings);
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
                    Collection<Object> fieldValues = currentRow.doc.getFieldValues(columnName);
                    String value;
                    if (fieldValues != null && fieldValues.size() > 0) {
                        if (fieldValues.size() > 1) {
                            value = Iterables.toString(fieldValues);
                        } else {
                            Object fieldValue = currentRow.doc.getFieldValue(columnName);
                            if (fieldValue != null) {
                                value = fieldValue.toString();
                            } else {
                                value = null;
                            }
                        }
                    } else {
                        value = Iterables.toString(Collections.emptyList());
                    }

                    return PropertyValues.newString(value);
                }

            };
        }

        @Override
        public long getSize(SizePrecision precision, long max) {
            long estimate = -1;
            switch (precision) {
                case EXACT:
                    // query solr
                    SolrQuery countQuery = FilterQueryParser.getQuery(plan.getFilter(), null, SolrQueryIndex.this.configuration);
                    countQuery.setRows(0);
                    try {
                        estimate = SolrQueryIndex.this.solrServer.query(countQuery).getResults().getNumFound();
                    } catch (SolrServerException e) {
                        log.warn("could not perform count query {}", countQuery);
                    }
                    break;
                case APPROXIMATION:
                    // estimate result size
                    estimate = SolrQueryIndex.this.estimator.estimate(plan.getFilter());
                    break;
                case FAST_APPROXIMATION:
                    // use already computed index plan's estimate
                    estimate = plan.getEstimatedEntryCount();
                    break;
            }
            return Math.min(estimate, max);
        }

    }


    @Override
    @CheckForNull
    public NodeAggregator getNodeAggregator() {
        return aggregator;
    }

}