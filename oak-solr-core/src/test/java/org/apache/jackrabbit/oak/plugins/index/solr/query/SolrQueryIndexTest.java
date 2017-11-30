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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
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
        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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
        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

        SolrQueryIndex solrQueryIndex = new SolrQueryIndex(null, null, null);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        List<QueryIndex.OrderEntry> sortOrder = new LinkedList<QueryIndex.OrderEntry>();
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, sortOrder, nodeState);
        assertEquals(1, plans.size());
    }

    @Test
    public void testNoPlanWithPrimaryTypeRestrictions() throws Exception {
        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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

        SelectorImpl selector = newSelector(nodeState, "a");

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

    @Test
    public void testSize() throws Exception {
        NodeState root = InitialContent.INITIAL_CONTENT;
        SelectorImpl selector = newSelector(root, "a");
        String sqlQuery = "select [jcr:path], [jcr:score] from [nt:base] as a where" +
                " contains([jcr:content/*], 'founded')";
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
        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, null, root);
        for (QueryIndex.IndexPlan p : plans) {
            Cursor cursor = solrQueryIndex.query(p, root);
            assertNotNull(cursor);
            long sizeExact = cursor.getSize(Result.SizePrecision.EXACT, 100000);
            long sizeApprox = cursor.getSize(Result.SizePrecision.APPROXIMATION, 100000);
            long sizeFastApprox = cursor.getSize(Result.SizePrecision.FAST_APPROXIMATION, 100000);
            assertTrue(Math.abs(sizeExact - sizeApprox) < 10);
            assertTrue(Math.abs(sizeExact - sizeFastApprox) > 10000);
        }
    }

    @Test
    public void testNoMoreThanThreeSolrRequests() throws Exception {
        NodeState root = InitialContent.INITIAL_CONTENT;
        SelectorImpl selector = newSelector(root, "a");
        String sqlQuery = "select [jcr:path], [jcr:score] from [nt:base] as a where" +
                " contains([jcr:content/*], 'founded')";
        SolrClient solrServer = mock(SolrClient.class);
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

        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, null, root);
        for (QueryIndex.IndexPlan p : plans) {
            Cursor cursor = solrQueryIndex.query(p, root);
            assertNotNull(cursor);
            while (cursor.hasNext()) {
                IndexRow row = cursor.next();
                assertNotNull(row);
            }
            assertEquals(3, response.getCounter());
        }
    }

    @Test
    public void testNoNegativeCost() throws Exception {
        NodeState root = InitialContent.INITIAL_CONTENT;
        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("solr")
                .setProperty("usedProperties", Collections.singleton("name"), Type.STRINGS)
                .setProperty("propertyRestrictions", true)
                .setProperty("type", "solr");
        nodeState = builder.getNodeState();
        SelectorImpl selector = newSelector(root, "a");
        String query = "select * from [nt:base] as a where native('solr','select?q=searchKeywords:\"foo\"^20 text:\"foo\"^1 " +
                "description:\"foo\"^8 something:\"foo\"^3 headline:\"foo\"^5 title:\"foo\"^10 &q.op=OR'";
        String sqlQuery = "select * from [nt:base] a where native('solr','" + query + "'";
        SolrClient solrServer = mock(SolrClient.class);
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
        filter.restrictProperty("native*solr", Operator.EQUAL, PropertyValues.newString(query));
        CountingResponse response = new CountingResponse(0);
        when(solrServer.query(any(SolrParams.class))).thenReturn(response);

        List<QueryIndex.IndexPlan> plans = solrQueryIndex.getPlans(filter, null, nodeState);
        for (QueryIndex.IndexPlan p : plans) {
            double costPerEntry = p.getCostPerEntry();
            assertTrue(costPerEntry >= 0);
            double costPerExecution = p.getCostPerExecution();
            assertTrue(costPerExecution >= 0);
            long estimatedEntryCount = p.getEstimatedEntryCount();
            assertTrue(estimatedEntryCount >= 0);

            double c = p.getCostPerExecution() + estimatedEntryCount * p.getCostPerEntry();
            assertTrue(c >= 0);
        }
    }

    private static SelectorImpl newSelector(NodeState root, String name) {
        NodeTypeInfoProvider types = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = types.getNodeTypeInfo("nt:base");
        return new SelectorImpl(type, name);
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
