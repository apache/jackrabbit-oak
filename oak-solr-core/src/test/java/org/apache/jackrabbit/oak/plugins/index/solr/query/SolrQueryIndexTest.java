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

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.TestUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SolrQueryIndex}
 */
public class SolrQueryIndexTest {

    private NodeState nodeState;

    @Before
    public void setUp() throws Exception {
        NodeState root = EMPTY_NODE;
        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("solr")
                .setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition")
                .setProperty("type", "solr")
                .child("server").setProperty("solrServerType", "embedded");
        nodeState = builder.getNodeState();
    }

    @Test
    public void testNoIndexPlanWithNoRestrictions() throws Exception {

        SelectorImpl selector = mock(SelectorImpl.class);

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "", new QueryEngineSettings());
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }


    @Test
    public void testNoPlanWithPathRestrictions() throws Exception {
        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where isdescendantnode(a, '/test')", new QueryEngineSettings());
        filter.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }

    @Test
    public void testNoPlanWithOnlyPathRestrictionsEnabled() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr").setProperty("pathRestrictions", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where isdescendantnode(a, '/test')", new QueryEngineSettings());
        filter.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }

    @Test
    public void testPlanWithPropertyAndPathRestrictionsEnabled() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr")
                .setProperty("pathRestrictions", true)
                .setProperty("propertyRestrictions", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where isdescendantnode(a, '/test')", new QueryEngineSettings());
        filter.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(1, plans.size());
    }

    @Test
    public void testNoPlanWithPropertyRestrictions() throws Exception {
        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }

    @Test
    public void testPlanWithPropertyRestrictionsEnabled() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr")
                .setProperty("propertyRestrictions", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(1, plans.size());
    }

    @Test
    public void testNoPlanWithPrimaryTypeRestrictions() throws Exception {
        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where jcr:primaryType = 'nt:unstructured')", new QueryEngineSettings());
        filter.restrictProperty("jcr:primaryType", Operator.EQUAL, PropertyValues.newString("nt:unstructured"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }

    @Test
    public void testNoPlanWithOnlyPrimaryTypeRestrictionsEnabled() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr").setProperty("primaryTypes", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where jcr:primaryType = 'nt:unstructured')", new QueryEngineSettings());
        filter.restrictProperty("jcr:primaryType", Operator.EQUAL, PropertyValues.newString("nt:unstructured"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }

    @Test
    public void testPlanWithPropertyAndPrimaryTypeRestrictionsEnabled() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr")
                .setProperty("propertyRestrictions", true)
                .setProperty("primaryTypes", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where jcr:primaryType = 'nt:unstructured')", new QueryEngineSettings());
        filter.restrictProperty("jcr:primaryType", Operator.EQUAL, PropertyValues.newString("nt:unstructured"));
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(1, plans.size());
    }

    @Test
    public void testNoPlanWithPropertyRestrictionsEnabledButPropertyIgnored() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr")
                .setProperty("ignoredProperties", Collections.singleton("name"), Type.STRINGS)
                .setProperty("propertyRestrictions", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size()); // there's no plan matching the filter
    }

    @Test
    public void testNoPlanWithPropertyRestrictionsEnabledButNotUsedProperty() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr")
                .setProperty("usedProperties", Collections.singleton("foo"), Type.STRINGS)
                .setProperty("propertyRestrictions", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }

    @Test
    public void testPlanWithPropertyRestrictionsEnabledAndUsedProperty() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr")
                .setProperty("usedProperties", Collections.singleton("name"), Type.STRINGS)
                .setProperty("propertyRestrictions", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(1, plans.size());
    }

    @Test
    public void testNoPlanWithPropertyNotListedInUsedProperties() throws Exception {
        NodeBuilder builder = nodeState.builder();
        builder.child("oak:index").child("solr")
                .setProperty("usedProperties", Collections.singleton("name"), Type.STRINGS)
                .setProperty("propertyRestrictions", true);
        nodeState = builder.getNodeState();

        SelectorImpl selector = new SelectorImpl(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where foo = 'bar')", new QueryEngineSettings());
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(0, plans.size());
    }

   @Test
    public void testUnion() throws Exception {
       SelectorImpl selector = mock(SelectorImpl.class);

       SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

       String sqlQuery = "select [jcr:path], [jcr:score], [rep:excerpt] from [nt:hierarchyNode] as a where" +
                " isdescendantnode(a, '/content') and contains([jcr:content/*], 'founded') union select [jcr:path]," +
                " [jcr:score], [rep:excerpt] from [nt:hierarchyNode] as a where isdescendantnode(a, '/content') and " +
                "contains([jcr:content/jcr:title], 'founded') union select [jcr:path], [jcr:score], [rep:excerpt]" +
                " from [nt:hierarchyNode] as a where isdescendantnode(a, '/content') and " +
                "contains([jcr:content/jcr:description], 'founded') order by [jcr:score] desc";
        FilterImpl filter = new FilterImpl(selector, sqlQuery, new QueryEngineSettings());
       List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
       List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
       assertEquals(0, plans.size());
    }

    @Ignore
    @Test
    public void testSize() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");
        String sqlQuery = "select [jcr:path], [jcr:score] from [nt:base] as a where" +
                " contains([jcr:content/*], 'founded')";
        SolrServer solrServer = TestUtils.createSolrServer();
        SolrServerProvider solrServerProvider = mock(SolrServerProvider.class);
        OakSolrConfigurationProvider configurationProvider = mock(OakSolrConfigurationProvider.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }
        };
        when(configurationProvider.getConfiguration()).thenReturn(configuration);

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, configurationProvider, solrServerProvider);
        FilterImpl filter = new FilterImpl(selector, sqlQuery, new QueryEngineSettings());
        Cursor cursor = solrQueryIndex.query(filter, root);
        assertNotNull(cursor);
        long sizeExact = cursor.getSize(Result.SizePrecision.EXACT, 100000);
        long sizeApprox = cursor.getSize(Result.SizePrecision.APPROXIMATION, 100000);
        long sizeFastApprox = cursor.getSize(Result.SizePrecision.FAST_APPROXIMATION, 100000);
        assertTrue(Math.abs(sizeExact - sizeApprox) < 10);
        assertTrue(Math.abs(sizeExact - sizeFastApprox) > 10000);
    }

    @Ignore
    @Test
    public void testNoMoreThanThreeSolrRequests() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");
        String sqlQuery = "select [jcr:path], [jcr:score] from [nt:base] as a where" +
                " contains([jcr:content/*], 'founded')";
        SolrServer solrServer = mock(SolrServer.class);
        SolrServerProvider solrServerProvider = mock(SolrServerProvider.class);
        when(solrServerProvider.getSearchingSolrServer()).thenReturn(solrServer);
        OakSolrConfigurationProvider configurationProvider = mock(OakSolrConfigurationProvider.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }

            @Override
            public int getRows() {
                return 10;
            }
        };
        when(configurationProvider.getConfiguration()).thenReturn(configuration);

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, configurationProvider, solrServerProvider);
        FilterImpl filter = new FilterImpl(selector, sqlQuery, new QueryEngineSettings());
        CountingResponse response = new CountingResponse(0);
        when(solrServer.query(any(SolrParams.class))).thenReturn(response);

        Cursor cursor = solrQueryIndex.query(filter, root);
        assertNotNull(cursor);
        while (cursor.hasNext()) {
            IndexRow row = cursor.next();
            assertNotNull(row);
        }
        assertEquals(3, response.getCounter());
    }

    private class CountingResponse extends QueryResponse {

        private int counter;

        public CountingResponse(int counter) {
            this.counter = counter;
        }

        @Override
        public SolrDocumentList getResults() {
            SolrDocumentList results = new SolrDocumentList();
            for (int i = 0; i < 1000; i++) {
                results.add(new SolrDocument());
            }
            results.setNumFound(1000);
            counter++;
            return results;
        }

        public int getCounter() {
            return counter;
        }
    }
}
