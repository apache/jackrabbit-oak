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

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex}
 */
public class SolrQueryIndexTest {

    @Test
    public void testDefaultCostWithNoRestrictions() throws Exception {
        NodeState root = mock(NodeState.class);
        SelectorImpl selector = mock(SelectorImpl.class);

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration();
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "", new QueryEngineSettings());
        double cost = solrQueryIndex.getCost(filter, root);
        assertTrue(Double.POSITIVE_INFINITY == cost);
    }

    @Test
    public void testDefaultCostWithPathRestrictions() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration();
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where isdescendantnode(a, '/test')", new QueryEngineSettings());
        filter.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        double cost = solrQueryIndex.getCost(filter, root);
        assertTrue(Double.POSITIVE_INFINITY == cost);
    }

    @Test
    public void testCostWithPathRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration(){
            @Override
            public boolean useForPathRestrictions() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where isdescendantnode(a, '/test')", new QueryEngineSettings());
        filter.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        double cost = solrQueryIndex.getCost(filter, root);
        assertTrue(10 == cost);
    }

    @Test
    public void testDefaultCostWithPropertyRestrictions() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration();
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertTrue(Double.POSITIVE_INFINITY == cost);
    }

    @Test
    public void testCostWithPropertyRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration(){
            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertTrue(10 == cost);
    }

    @Test
    public void testDefaultCostWithPrimaryTypeRestrictions() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration();
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where jcr:primaryType = 'nt:unstructured')", new QueryEngineSettings());
        filter.restrictProperty("jcr:primaryType", Operator.EQUAL, PropertyValues.newString("nt:unstructured"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertTrue(Double.POSITIVE_INFINITY == cost);
    }

    @Test
    public void testCostWithPrimaryTypeRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration(){
            @Override
            public boolean useForPrimaryTypes() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where jcr:primaryType = 'nt:unstructured')", new QueryEngineSettings());
        filter.restrictProperty("jcr:primaryType", Operator.EQUAL, PropertyValues.newString("nt:unstructured"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertTrue(10 == cost);
    }
}
