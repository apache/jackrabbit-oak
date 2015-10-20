/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.solr.TestUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditor}
 */
public class SolrIndexEditorTest {

    @Test
    public void testIndexedProperties() throws Exception {
        NodeBuilder builder = mock(NodeBuilder.class);
        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            OakSolrConfiguration configuration = TestUtils.getTestConfiguration();
            IndexUpdateCallback callback = mock(IndexUpdateCallback.class);
            SolrIndexEditor solrIndexEditor = new SolrIndexEditor(builder, solrServer, configuration, callback);
            NodeState before = mock(NodeState.class);
            NodeState after = mock(NodeState.class);
            Iterable properties = new Iterable<PropertyState>() {
                @Override
                public Iterator<PropertyState> iterator() {
                    return Arrays.asList(PropertyStates.createProperty("foo1", "bar")).iterator();
                }
            };
            when(after.getProperties()).thenReturn(properties);
            solrIndexEditor.leave(before, after);
            QueryResponse queryResponse = solrServer.query(new SolrQuery("foo1:*"));
            assertEquals(1, queryResponse.getResults().getNumFound());
        } finally {
            solrServer.shutdown();
        }
    }

    @Test
    public void testIgnoredPropertiesNotIndexed() throws Exception {
        NodeBuilder builder = mock(NodeBuilder.class);
        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
                @Nonnull
                @Override
                public Collection<String> getIgnoredProperties() {
                    return Arrays.asList("foo2");
                }

                @Nonnull
                @Override
                public CommitPolicy getCommitPolicy() {
                    return CommitPolicy.HARD;
                }
            };
            IndexUpdateCallback callback = mock(IndexUpdateCallback.class);
            SolrIndexEditor solrIndexEditor = new SolrIndexEditor(builder, solrServer, configuration, callback);
            NodeState before = mock(NodeState.class);
            NodeState after = mock(NodeState.class);
            Iterable properties = new Iterable<PropertyState>() {
                @Override
                public Iterator<PropertyState> iterator() {
                    return Arrays.asList(PropertyStates.createProperty("foo2", "bar")).iterator();
                }
            };
            when(after.getProperties()).thenReturn(properties);
            solrIndexEditor.leave(before, after);
            QueryResponse queryResponse = solrServer.query(new SolrQuery("foo2:*"));
            assertEquals(0, queryResponse.getResults().getNumFound());
        } finally {
            solrServer.shutdown();
        }
    }
}
