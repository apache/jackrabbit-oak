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

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Testcase for {@link FilterQueryParser}
 */
public class FilterQueryParserTest {

    @Test
    public void testMatchAllConversionWithNoConstraints() throws Exception {
        Filter filter = mock(Filter.class);
        OakSolrConfiguration configuration = mock(OakSolrConfiguration.class);
        QueryIndex.IndexPlan plan = mock(QueryIndex.IndexPlan.class);
        SolrQuery solrQuery = FilterQueryParser.getQuery(filter, plan, configuration);
        assertNotNull(solrQuery);
        assertEquals("*:*", solrQuery.getQuery());
    }

    @Test
    public void testAllChildrenQueryParsing() throws Exception {
        String query = "select [jcr:path], [jcr:score], * from [nt:hierarchy] as a where isdescendantnode(a, '/')";
        Filter filter = mock(Filter.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration(){
            @Override
            public boolean useForPathRestrictions() {
                return true;
            }
        };
        when(filter.getQueryStatement()).thenReturn(query);
        Filter.PathRestriction pathRestriction = Filter.PathRestriction.ALL_CHILDREN;
        when(filter.getPathRestriction()).thenReturn(pathRestriction);
        when(filter.getPath()).thenReturn("/");
        QueryIndex.IndexPlan plan = mock(QueryIndex.IndexPlan.class);
        SolrQuery solrQuery = FilterQueryParser.getQuery(filter, plan, configuration);
        assertNotNull(solrQuery);
        String[] filterQueries = solrQuery.getFilterQueries();
        assertTrue(Arrays.asList(filterQueries).contains(configuration.getFieldForPathRestriction(pathRestriction) + ":\\/"));
        assertEquals("*:*", solrQuery.get("q"));
    }

    @Test
    public void testCollapseJcrContentNodes() throws Exception {
        String query = "select [jcr:path], [jcr:score], * from [nt:hierarchy] as a where isdescendantnode(a, '/')";
        Filter filter = mock(Filter.class);
        OakSolrConfiguration configuration = new DefaultSolrConfiguration(){
            @Override
            public boolean collapseJcrContentNodes() {
                return true;
            }
        };
        when(filter.getQueryStatement()).thenReturn(query);
        QueryIndex.IndexPlan plan = mock(QueryIndex.IndexPlan.class);
        SolrQuery solrQuery = FilterQueryParser.getQuery(filter, plan, configuration);
        assertNotNull(solrQuery);
        String[] filterQueries = solrQuery.getFilterQueries();
        assertTrue(Arrays.asList(filterQueries).contains("{!collapse field=" + configuration.getCollapsedPathField()
                + " min=" + configuration.getPathDepthField() + " hint=top_fc nullPolicy=expand}"));
        assertEquals("*:*", solrQuery.get("q"));
    }

}
