/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.query.index;

import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexPlan;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IndexSelectionTest extends AbstractQueryTest {
    private TestIndexProvider testIndexProvider = new TestIndexProvider();
    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(testIndexProvider)
                .createContentRepository();
    }

    @Test
    public void uuidIndexQuery() throws Exception{
        NodeUtil node = new NodeUtil(root.getTree("/"));
        String uuid = UUID.randomUUID().toString();
        node.setString(JcrConstants.JCR_UUID, uuid);
        root.commit();

        assertQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] = '"+uuid+"' ",
                ImmutableList.of("/"));
        assertEquals("Test index plan should not be invoked", 
                0, testIndexProvider.index.invocationCount);
    }
    
    @Test
    public void uuidIndexNotNullQuery() throws Exception{
        NodeUtil node = new NodeUtil(root.getTree("/"));
        String uuid = UUID.randomUUID().toString();
        node.setString(JcrConstants.JCR_UUID, uuid);
        root.commit();

        assertQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] is not null",
                ImmutableList.of("/"));
        assertEquals("Test index plan should be invoked", 
                1, testIndexProvider.index.invocationCount);
    }

    @Test
    public void uuidIndexInListQuery() throws Exception{
        NodeUtil node = new NodeUtil(root.getTree("/"));
        String uuid = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        node.setString(JcrConstants.JCR_UUID, uuid);
        root.commit();

        assertQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] in('" + uuid + "', '" + uuid2 + "')",
                ImmutableList.of("/"));
        assertEquals("Test index plan should be invoked", 
                1, testIndexProvider.index.invocationCount);
    }

    private static class TestIndexProvider implements QueryIndexProvider {
        TestIndex index = new TestIndex();
        @Nonnull
        @Override
        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return ImmutableList.<QueryIndex>of(index);
        }
    }

    private static class TestIndex implements QueryIndex {
        int invocationCount = 0;
        @Override
        public double getMinimumCost() {
            return PropertyIndexPlan.COST_OVERHEAD + 0.1;
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            invocationCount++;
            return Double.POSITIVE_INFINITY;
        }

        @Override
        public Cursor query(Filter filter, NodeState rootState) {
            return null;
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) {
            return null;
        }

        @Override
        public String getIndexName() {
            return "test-index";
        }
    }
}