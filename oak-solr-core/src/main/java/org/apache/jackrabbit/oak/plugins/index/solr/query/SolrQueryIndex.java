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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.NodeStateSolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.nodestate.OakSolrNodeStateConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.server.OakSolrServer;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.PathUtils.*;
import static org.apache.jackrabbit.oak.plugins.index.solr.util.SolrIndexInitializer.isSolrIndexNode;

/**
 * A Solr based {@link QueryIndex}
 */
public class SolrQueryIndex implements FulltextQueryIndex, QueryIndex.AdvanceFulltextQueryIndex {

    public static final String TYPE = "solr";

    static final String NATIVE_SOLR_QUERY = "native*solr";

    static final String NATIVE_LUCENE_QUERY = "native*lucene";

    private static double MIN_COST = 2.3;

    private final Logger log = LoggerFactory.getLogger(SolrQueryIndex.class);

    private final NodeAggregator aggregator;
    private final OakSolrConfigurationProvider fallbackOakSolrConfigurationProvider;
    private final SolrServerProvider fallbackSolrServerProvider;

    private static final Map<String, LMSEstimator> estimators = new WeakHashMap<String, LMSEstimator>();

    public SolrQueryIndex(NodeAggregator aggregator, OakSolrConfigurationProvider oakSolrConfigurationProvider, SolrServerProvider solrServerProvider) {
        this.aggregator = aggregator;
        this.fallbackOakSolrConfigurationProvider = oakSolrConfigurationProvider;
        this.fallbackSolrServerProvider = solrServerProvider;
    }

    @Override
    public double getMinimumCost() {
        return MIN_COST;
    }

    @Override
    public String getIndexName() {
        return "solr";
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    int getMatchingFilterRestrictions(Filter filter, OakSolrConfiguration configuration) {
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
        if (filter.getPrimaryTypes().size() > 0 && configuration.useForPrimaryTypes()) {
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
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
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
        Cursor cursor;
        try {

            Filter filter = plan.getFilter();
            final Set<String> relPaths = filter.getFullTextConstraint() != null ? getRelativePaths(filter.getFullTextConstraint())
                    : Collections.<String>emptySet();
            final String parent = relPaths.size() == 0 ? "" : relPaths.iterator().next();

            final int parentDepth = getDepth(parent);
            String path = plan.getPlanName();

            OakSolrConfiguration configuration = getConfiguration(path, root);
            SolrClient solrServer = getServer(path, root);
            LMSEstimator estimator = getEstimator(path);

            AbstractIterator<SolrResultRow> iterator = getIterator(filter, plan, parent, parentDepth, configuration,
                    solrServer, estimator);

            cursor = new SolrRowCursor(iterator, plan, filter.getQueryLimits(), estimator, solrServer, configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }

    private synchronized LMSEstimator getEstimator(String path) {
        if (!estimators.containsKey(path)) {
            estimators.put(path, new LMSEstimator());
        }
        return estimators.get(path);
    }

    private SolrClient getServer(String path, NodeState root) {

        NodeState node = root;
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }

        try {
            if (isSolrIndexNode(node)) {
                if (node.hasChildNode("server")) {
                    SolrServerConfigurationProvider solrServerConfigurationProvider = new NodeStateSolrServerConfigurationProvider(
                            node.getChildNode("server"));
                    return new OakSolrServer(solrServerConfigurationProvider);
                } else {
                    return fallbackSolrServerProvider.getSearchingSolrServer();
                }
            } else if (node.exists()) {
                log.warn("Cannot open Solr Index at path {} as the index is not of type 'solr'", path);
            }
        } catch (Exception e) {
            log.error("Could not access the Solr index at " + path, e);
        }

        return null;
    }

    private AbstractIterator<SolrResultRow> getIterator(final Filter filter, final IndexPlan plan,
                                                        final String parent, final int parentDepth,
                                                        final OakSolrConfiguration configuration, final SolrClient solrServer,
                                                        final LMSEstimator estimator) {
        return new AbstractIterator<SolrResultRow>() {
            public Collection<FacetField> facetFields = new LinkedList<FacetField>();
            private final Set<String> seenPaths = Sets.newHashSet();
            private final Deque<SolrResultRow> queue = Queues.newArrayDeque();
            private int offset = 0;
            private boolean noDocs = false;
            private long numFound = 0;

            @Override
            protected SolrResultRow computeNext() {
                if (!queue.isEmpty() || loadDocs()) {
                    return queue.remove();
                }
                return endOfData();
            }

            private SolrResultRow convertToRow(SolrDocument doc) {
                String path = String.valueOf(doc.getFieldValue(configuration.getPathField()));
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
                return new SolrResultRow(path, score, doc, facetFields);

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
                    SolrQuery query = FilterQueryParser.getQuery(filter, plan, configuration);
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

                    if (log.isDebugEnabled()) {
                        log.debug("getting response {}", queryResponse.getHeader());
                    }

                    SolrDocumentList docs = queryResponse.getResults();

                    if (docs != null) {

                        numFound = docs.getNumFound();

                        estimator.update(filter, docs);

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
                                            doc.addField(QueryConstants.REP_EXCERPT, v);
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

                    // get facets
                    List<FacetField> returnedFieldFacet = queryResponse.getFacetFields();
                    if (returnedFieldFacet != null) {
                        facetFields.addAll(returnedFieldFacet);
                    }

                    // filter facets on doc paths
                    if (!facetFields.isEmpty() && docs != null) {
                        for (SolrDocument doc : docs) {
                            String path = String.valueOf(doc.getFieldValue(configuration.getPathField()));
                            // if facet path doesn't exist for the calling user, filter the facet for this doc
                            for (FacetField ff : facetFields) {
                                if (!filter.isAccessible(path + "/" + ff.getName())) {
                                    filterFacet(doc, ff);
                                }
                            }
                        }
                    }

                    // handle spellcheck
                    SpellCheckResponse spellCheckResponse = queryResponse.getSpellCheckResponse();
                    if (spellCheckResponse != null && spellCheckResponse.getSuggestions() != null &&
                            spellCheckResponse.getSuggestions().size() > 0) {
                        putSpellChecks(spellCheckResponse, queue, filter, configuration, solrServer);
                        noDocs = true;
                    }

                    // handle suggest
                    NamedList<Object> response = queryResponse.getResponse();
                    Map suggest = (Map) response.get("suggest");
                    if (suggest != null) {
                        Set<Map.Entry<String, Object>> suggestEntries = suggest.entrySet();
                        if (!suggestEntries.isEmpty()) {
                            putSuggestions(suggestEntries, queue, filter, configuration, solrServer);
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

    private void filterFacet(SolrDocument doc, FacetField facetField) {
        // facet filtering by value requires that the facet values match the stored values
        // a *_facet field must exist, stored (or /w docValues) to be used for faceting and at filtering time
        if (doc.getFieldNames().contains(facetField.getName())) {
            // decrease facet value
            Collection<Object> docFieldValues = doc.getFieldValues(facetField.getName());
            if (docFieldValues != null) {
                for (Object docFieldValue : docFieldValues) {
                    String valueString = String.valueOf(docFieldValue);
                    List<FacetField.Count> toRemove = new LinkedList<FacetField.Count>();
                    for (FacetField.Count count : facetField.getValues()) {
                        long existingCount = count.getCount();
                        if (valueString.equals(count.getName())) {
                            if (existingCount > 1) {
                                // decrease the count
                                count.setCount(existingCount - 1);
                            } else {
                                // remove the entire entry
                                toRemove.add(count);
                            }
                        }
                    }
                    for (FacetField.Count f : toRemove) {
                        assert facetField.getValues().remove(f);
                    }
                }
            }
        }
    }

    private void putSpellChecks(SpellCheckResponse spellCheckResponse,
                                final Deque<SolrResultRow> queue,
                                Filter filter, OakSolrConfiguration configuration, SolrClient solrServer) throws IOException, SolrServerException {
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
                        queue.add(new SolrResultRow(alternative));
                        break;
                    }
                }
            }
        }
    }

    private void putSuggestions(Set<Map.Entry<String, Object>> suggestEntries, final Deque<SolrResultRow> queue,
                                Filter filter, OakSolrConfiguration configuration, SolrClient solrServer) throws IOException, SolrServerException {
        Collection<SimpleOrderedMap<Object>> retrievedSuggestions = new HashSet<SimpleOrderedMap<Object>>();
        for (Map.Entry<String, Object> suggester : suggestEntries) {
            SimpleOrderedMap<Object> suggestionResponses = ((SimpleOrderedMap) suggester.getValue());
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
                        queue.add(new SolrResultRow(suggestion.get("term").toString(),
                                Double.parseDouble(suggestion.get("weight").toString())));
                        break;
                    }
                }
            }
        }
    }

    static boolean isIgnoredProperty(Filter.PropertyRestriction property, OakSolrConfiguration configuration) {
        if (NATIVE_LUCENE_QUERY.equals(property.propertyName) || NATIVE_SOLR_QUERY.equals(property.propertyName)) {
            return false;
        } else return (!configuration.useForPropertyRestrictions() // Solr index not used for properties
                || (configuration.getUsedProperties().size() > 0 && !configuration.getUsedProperties().contains(property.propertyName)) // not explicitly contained in the used properties
                || property.propertyName.contains("/") // no child-level property restrictions
                || QueryConstants.REP_EXCERPT.equals(property.propertyName) // rep:excerpt is not handled at the property level
                || QueryConstants.OAK_SCORE_EXPLANATION.equals(property.propertyName) // score explain is not handled at the property level
                || QueryConstants.REP_FACET.equals(property.propertyName) // rep:facet is not handled at the property level
                || QueryConstants.RESTRICTION_LOCAL_NAME.equals(property.propertyName)
                || property.propertyName.startsWith(QueryConstants.FUNCTION_RESTRICTION_PREFIX)
                || configuration.getIgnoredProperties().contains(property.propertyName));
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {

        Collection<String> indexPaths = new SolrIndexLookup(rootState).collectIndexNodePaths(filter);
        List<IndexPlan> plans = Lists.newArrayListWithCapacity(indexPaths.size());

        for (String path : indexPaths) {
            OakSolrConfiguration configuration = getConfiguration(path, rootState);
            SolrClient solrServer = getServer(path, rootState);
            // only provide the plan if both valid configuration and server exist
            if (configuration != null && solrServer != null) {
                LMSEstimator estimator = getEstimator(path);
                IndexPlan plan = getIndexPlan(filter, configuration, estimator, sortOrder, path);
                if (plan != null) {
                    plans.add(plan);
                }
            }
        }
        return plans;
    }

    private OakSolrConfiguration getConfiguration(String path, NodeState rootState) {
        NodeState node = rootState;
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }

        try {
            if (isSolrIndexNode(node)) {
                if (node.hasChildNode("server")) {
                    return new OakSolrNodeStateConfiguration(node);
                } else {
                    return fallbackOakSolrConfigurationProvider.getConfiguration();
                }
            } else if (node.exists()) {
                log.warn("Cannot open Solr Index at path {} as the index is not of type 'solr'", path);
            }
        } catch (Exception e) {
            log.error("Could not access the Solr index at " + path, e);
        }

        return null;
    }

    private IndexPlan getIndexPlan(Filter filter, OakSolrConfiguration configuration, LMSEstimator estimator,
                                   List<OrderEntry> sortOrder, String path) {
        if (getMatchingFilterRestrictions(filter, configuration) > 0) {
            return planBuilder(filter)
                    .setEstimatedEntryCount(estimator.estimate(filter))
                    .setSortOrder(sortOrder)
                    .setPlanName(path)
                    .setPathPrefix(getPathPrefix(path))
                    .build();
        } else {
            return null;
        }
    }

    private String getPathPrefix(String indexPath) {
        // 2 = /oak:index/<index name>
        String parentPath = PathUtils.getAncestorPath(indexPath, 2);
        return PathUtils.denotesRoot(parentPath) ? "" : parentPath;
    }

    private IndexPlan.Builder planBuilder(Filter filter) {
        return new IndexPlan.Builder()
                .setCostPerExecution(1.5) // disk I/O + network I/O
                .setCostPerEntry(0.3) // with properly configured SolrCaches ~70% of the doc fetches should hit them
                .setFilter(filter)
                .setFulltextIndex(true)
                .setIncludesNodeData(true) // we currently include node data
                .setDelayed(true); //Solr is most usually async
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        return plan.toString();
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    static class SolrResultRow {
        final String path;
        final double score;
        final SolrDocument doc;
        final Collection<FacetField> facetFields;
        final String suggestion;

        private SolrResultRow(String path, double score, SolrDocument doc, String suggestion, Collection<FacetField> facetFields) {
            this.path = path;
            this.score = score;
            this.doc = doc;
            this.suggestion = suggestion;
            this.facetFields = facetFields;
        }

        SolrResultRow(String suggestion, double score) {
            this("/", score, null, suggestion, null);
        }

        SolrResultRow(String suggestion) {
            this("/", 1.0, null, suggestion, null);
        }

        SolrResultRow(String path, float score, SolrDocument doc, Collection<FacetField> facetFields) {
            this(path, score, doc, null, facetFields);
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
        private final LMSEstimator estimator;
        private final SolrClient solrServer;
        private final OakSolrConfiguration configuration;

        SolrResultRow currentRow;

        SolrRowCursor(final Iterator<SolrResultRow> it, IndexPlan plan, QueryLimits settings,
                      LMSEstimator estimator, SolrClient solrServer, OakSolrConfiguration configuration) {
            this.estimator = estimator;
            this.solrServer = solrServer;
            this.configuration = configuration;
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
            this.pathCursor = new Cursors.PathCursor(pathIterator, false, settings);
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
                public boolean isVirtualRow() {
                    return currentRow.doc == null;
                }

                @Override
                public String getPath() {
                    String sub = pathRow.getPath();
                    if (isVirtualRow()) {
                        return sub;
                    } else if (PathUtils.isAbsolute(sub)) {
                        return plan.getPathPrefix() + sub;
                    } else {
                        return PathUtils.concat(plan.getPathPrefix(), sub);
                    }
                }

                @Override
                public PropertyValue getValue(String columnName) {
                    // overlay the score
                    if (QueryConstants.JCR_SCORE.equals(columnName)) {
                        return PropertyValues.newDouble(currentRow.score);
                    }
                    if (columnName.startsWith(QueryConstants.REP_FACET)) {
                        String facetFieldName = columnName.substring(QueryConstants.REP_FACET.length() + 1, columnName.length() - 1);
                        FacetField facetField = null;
                        for (FacetField ff : currentRow.facetFields) {
                            if (ff.getName().equals(facetFieldName + "_facet")) {
                                facetField = ff;
                                break;
                            }
                        }
                        if (facetField != null) {
                            JsopWriter writer = new JsopBuilder();
                            writer.object();
                            for (FacetField.Count count : facetField.getValues()) {
                                writer.key(count.getName()).value(count.getCount());
                            }
                            writer.endObject();
                            return PropertyValues.newString(writer.toString());
                        } else {
                            return null;
                        }
                    }
                    if (QueryConstants.REP_SPELLCHECK.equals(columnName) || QueryConstants.REP_SUGGEST.equals(columnName)) {
                        return PropertyValues.newString(currentRow.suggestion);
                    }
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
                                return null;
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
                    SolrQuery countQuery = FilterQueryParser.getQuery(plan.getFilter(), null, this.configuration);
                    countQuery.setRows(0);
                    try {
                        estimate = this.solrServer.query(countQuery).getResults().getNumFound();
                    } catch (IOException | SolrServerException e) {
                        log.warn("could not perform count query {}", countQuery);
                    }
                    break;
                case APPROXIMATION:
                    // estimate result size
                    estimate = this.estimator.estimate(plan.getFilter());
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
