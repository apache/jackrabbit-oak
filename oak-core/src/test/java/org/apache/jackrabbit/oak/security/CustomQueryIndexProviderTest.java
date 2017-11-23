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

package org.apache.jackrabbit.oak.security;

import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * This test validates if Oak initialization works fine with custom QueryIndexProvider
 * and none of the initializers rely on some hard coded index types
 */
public class CustomQueryIndexProviderTest extends AbstractSecurityTest {

    public static final String TEST_INDEX = "test-index";

    @Override
    protected Oak withEditors(Oak oak) {
        oak.with(new UUIDIndexReplacementInitializer());
        oak.with(new TestIndexEditor());
        oak.with(new TestQueryProvider());
        return oak;
    }

    @Test
    public void initWentFine() throws Exception{
        assertNotNull(root);
    }

    private class UUIDIndexReplacementInitializer implements RepositoryInitializer {
        @Override
        public void initialize(@Nonnull NodeBuilder builder) {
            builder.child("oak:index").child("uuid").setProperty("type", TEST_INDEX);
        }
    }

    private static class TestIndexEditor implements IndexEditorProvider {
        @CheckForNull
        @Override
        public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                                     @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
                throws CommitFailedException {
            if (TEST_INDEX.equals(type)) {
                PropertyIndexEditorProvider piep = new PropertyIndexEditorProvider();
                return piep.getIndexEditor(PropertyIndexEditorProvider.TYPE, definition, root, callback);
            }
            return null;
        }
    }

    private static class TestQueryProvider implements QueryIndexProvider {
        @Nonnull
        @Override
        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
            return Collections.singletonList(new TestQueryIndex());
        }
    }

    private static class TestQueryIndex implements QueryIndex {

        @Override
        public double getMinimumCost() {
            return 1;
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            if (filter.getPropertyRestriction("jcr:uuid") != null) {
                return 1;
            }
            return Double.MAX_VALUE;
        }

        @Override
        public Cursor query(Filter filter, NodeState rootState) {
            Filter.PropertyRestriction pr = filter.getPropertyRestriction("jcr:uuid");
            if (pr != null) {
                NodeBuilder nb = rootState.builder();
                //Fake the index type by reverting to "property" for final evaluation
                nb.child("oak:index").child("uuid").setProperty("type", PropertyIndexEditorProvider.TYPE);
                rootState = nb.getNodeState();
                PropertyIndexLookup pil = new PropertyIndexLookup(rootState);
                return Cursors.newPathCursor(pil.query(filter, "jcr:uuid", pr.first), new QueryEngineSettings());
            }
            return null;
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) {
            return "Test";
        }

        @Override
        public String getIndexName() {
            return "TestIndex";
        }
    }
}
