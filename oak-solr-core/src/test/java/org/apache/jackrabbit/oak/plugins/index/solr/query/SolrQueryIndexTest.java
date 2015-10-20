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

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.plugins.index.solr.TestUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex}
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
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
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
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
    }

    @Test
    public void testCostWithOnlyPathRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPathRestrictions() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where isdescendantnode(a, '/test')", new QueryEngineSettings());
        filter.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
    }

    @Test
    public void testCostWithPropertyAndPathRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPathRestrictions() {
                return true;
            }

            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where isdescendantnode(a, '/test')", new QueryEngineSettings());
        filter.restrictPath("/test", Filter.PathRestriction.ALL_CHILDREN);
        filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(5, cost, 0);
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
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
    }

    @Test
    public void testCostWithPropertyRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(10, cost, 0);
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
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
    }

    @Test
    public void testCostWithOnlyPrimaryTypeRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPrimaryTypes() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where jcr:primaryType = 'nt:unstructured')", new QueryEngineSettings());
        filter.restrictProperty("jcr:primaryType", Operator.EQUAL, PropertyValues.newString("nt:unstructured"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
    }

    @Test
    public void testCostWithPropertyAndPrimaryTypeRestrictionsEnabled() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPrimaryTypes() {
                return true;
            }

            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where jcr:primaryType = 'nt:unstructured')", new QueryEngineSettings());
        filter.restrictProperty("jcr:primaryType", Operator.EQUAL, PropertyValues.newString("nt:unstructured"));
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(5, cost, 0);
    }

    @Test
    public void testCostWithPropertyRestrictionsEnabledButPropertyIgnored() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }

            @Override
            public Collection<String> getIgnoredProperties() {
                return Arrays.asList("name");
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
    }

    @Test
    public void testCostWithPropertyRestrictionsEnabledButNotUsedProperty() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }

            @Nonnull
            @Override
            public Collection<String> getUsedProperties() {
                return Arrays.asList("foo");
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(Double.POSITIVE_INFINITY, cost, 0);
    }

    @Test
    public void testCostWithPropertyRestrictionsEnabledAndUsedProperty() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = mock(SolrServer.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
            @Override
            public boolean useForPropertyRestrictions() {
                return true;
            }

            @Nonnull
            @Override
            public Collection<String> getUsedProperties() {
                return Arrays.asList("name");
            }
        };
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

        FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
        filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
        double cost = solrQueryIndex.getCost(filter, root);
        assertEquals(10, cost, 0);
    }

    @Test
    public void testQueryOnIgnoredExistingProperty() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("path_exact", "/a/b");
            document.addField("name", "hello");
            solrServer.add(document);
            solrServer.commit();
            OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
                @Override
                public boolean useForPropertyRestrictions() {
                    return true;
                }

                @Override
                public Collection<String> getIgnoredProperties() {
                    return Arrays.asList("name");
                }
            };
            SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

            FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
            filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
            String plan = solrQueryIndex.getPlan(filter, root);
            assertNotNull(plan);
            assertTrue(plan.contains("q=*%3A*")); // querying on property name is not possible, then falling back to a match all query
        } finally {
            solrServer.shutdown();
        }
    }

    @Test
    public void testQueryOnExplicitlyUsedProperty() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("path_exact", "/a/b");
            document.addField("name", "hello");
            solrServer.add(document);
            solrServer.commit();
            OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
                @Override
                public boolean useForPropertyRestrictions() {
                    return true;
                }

                @Nonnull
                @Override
                public Collection<String> getUsedProperties() {
                    return Arrays.asList("name");
                }
            };
            SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

            FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
            filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
            String plan = solrQueryIndex.getPlan(filter, root);
            assertNotNull(plan);
            assertTrue(plan.contains("name%3Ahello")); // querying on property name is possible
        } finally {
            solrServer.shutdown();
        }
    }

    @Test
    public void testQueryOnPropertyNotListedInUsedProperties() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("path_exact", "/a/b");
            document.addField("name", "hello");
            solrServer.add(document);
            solrServer.commit();
            OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
                @Override
                public boolean useForPropertyRestrictions() {
                    return true;
                }

                @Nonnull
                @Override
                public Collection<String> getUsedProperties() {
                    return Arrays.asList("name");
                }
            };
            SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

            FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where foo = 'bar')", new QueryEngineSettings());
            filter.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("bar"));
            String plan = solrQueryIndex.getPlan(filter, root);
            assertNotNull(plan);
            assertTrue(plan.contains("*%3A*")); // querying on property foo is not possible, as the only usable property is 'name'
        } finally {
            solrServer.shutdown();
        }
    }

    @Test
    public void testQueryOnExistingProperty() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");

        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            SolrInputDocument document = new SolrInputDocument();
            document.addField("path_exact", "/a/b");
            document.addField("name", "hello");
            solrServer.add(document);
            solrServer.commit();
            OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
                @Override
                public boolean useForPropertyRestrictions() {
                    return true;
                }
            };
            SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);

            FilterImpl filter = new FilterImpl(selector, "select * from [nt:base] as a where name = 'hello')", new QueryEngineSettings());
            filter.restrictProperty("name", Operator.EQUAL, PropertyValues.newString("hello"));
            String plan = solrQueryIndex.getPlan(filter, root);
            assertNotNull(plan);
            assertTrue(plan.contains("q=name%3Ahello")); // query gets converted to a fielded query on name field
        } finally {
            solrServer.shutdown();
        }
    }

    @Test
    public void testUnion() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");
        String sqlQuery = "select [jcr:path], [jcr:score], [rep:excerpt] from [nt:hierarchyNode] as a where" +
                " isdescendantnode(a, '/content') and contains([jcr:content/*], 'founded') union select [jcr:path]," +
                " [jcr:score], [rep:excerpt] from [nt:hierarchyNode] as a where isdescendantnode(a, '/content') and " +
                "contains([jcr:content/jcr:title], 'founded') union select [jcr:path], [jcr:score], [rep:excerpt]" +
                " from [nt:hierarchyNode] as a where isdescendantnode(a, '/content') and " +
                "contains([jcr:content/jcr:description], 'founded') order by [jcr:score] desc";
        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
                @Override
                public boolean useForPropertyRestrictions() {
                    return true;
                }
            };
            SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);
            FilterImpl filter = new FilterImpl(selector, sqlQuery, new QueryEngineSettings());
            Cursor cursor = solrQueryIndex.query(filter, root);
            assertNotNull(cursor);
        } finally {
            solrServer.shutdown();
        }
    }

    @Test
    public void testSize() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");
        String sqlQuery = "select [jcr:path], [jcr:score] from [nt:base] as a where" +
                " contains([jcr:content/*], 'founded')";
        SolrServer solrServer = TestUtils.createSolrServer();
        try {
            OakSolrConfiguration configuration = new DefaultSolrConfiguration() {
                @Override
                public boolean useForPropertyRestrictions() {
                    return true;
                }
            };
            SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);
            FilterImpl filter = new FilterImpl(selector, sqlQuery, new QueryEngineSettings());
            Cursor cursor = solrQueryIndex.query(filter, root);
            assertNotNull(cursor);
            long sizeExact = cursor.getSize(Result.SizePrecision.EXACT, 100000);
            long sizeApprox = cursor.getSize(Result.SizePrecision.APPROXIMATION, 100000);
            long sizeFastApprox = cursor.getSize(Result.SizePrecision.FAST_APPROXIMATION, 100000);
            assertTrue(Math.abs(sizeExact - sizeApprox) < 10);
            assertTrue(Math.abs(sizeExact - sizeFastApprox) > 10000);
        } finally {
            solrServer.shutdown();
        }
    }

    @Test
    public void testNoMoreThanThreeSolrRequests() throws Exception {
        NodeState root = mock(NodeState.class);
        when(root.getNames(any(String.class))).thenReturn(Collections.<String>emptySet());
        SelectorImpl selector = new SelectorImpl(root, "a");
        String sqlQuery = "select [jcr:path], [jcr:score] from [nt:base] as a where" +
                " contains([jcr:content/*], 'founded')";
        SolrServer solrServer = mock(SolrServer.class);
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
        SolrQueryIndex solrQueryIndex = new SolrQueryIndex("solr", solrServer, configuration);
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
