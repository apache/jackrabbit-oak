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
package org.apache.jackrabbit.oak.plugins.index.search.spi.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.SizeEstimator;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextPathCursor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.IteratorRewoundStateProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class FulltextIndexTest {

    @Test
    public void testParseFacetField() {
        String field = FulltextIndex.parseFacetField("rep:facet(text)");
        assertNotNull(field);
        assertEquals("text", field);
        field = FulltextIndex.parseFacetField("rep:facet(jcr:title)");
        assertNotNull(field);
        assertEquals("jcr:title", field);
        field = FulltextIndex.parseFacetField("rep:facet(jcr:primaryType)");
        assertNotNull(field);
        assertEquals("jcr:primaryType", field);
    }

    /**
     * Test that we can read the rows first, and then read the data from the rows.
     */
    @Test
    public void fulltextPathCursorTest() {
        ArrayList<FulltextResultRow> rowList = new ArrayList<>();
        String s1 = "s1", s2 = "s2";
        rowList.add(new FulltextResultRow(s1, 1));
        rowList.add(new FulltextResultRow(s2, 2));
        FulltextPathCursor fulltextPathCursor = new TestFulltextPathCursor(rowList.iterator());
        IndexRow row1 = fulltextPathCursor.next();
        IndexRow row2 = fulltextPathCursor.next();
        String suggest1 = row1.getValue(QueryConstants.REP_SUGGEST).getValue(Type.STRING);
        String suggest2 = row2.getValue(QueryConstants.REP_SUGGEST).getValue(Type.STRING);
        assertEquals(s1, suggest1);
        assertEquals(s2, suggest2);

    }

    static class TestFulltextPathCursor extends FulltextPathCursor {
        public TestFulltextPathCursor(Iterator<FulltextResultRow> it) {
            super(it, new IteratorRewoundStateProvider() {

                @Override
                public int rewoundCount() {
                    return 0;
                }

            }, new TestIndexPlan(), new QueryEngineSettings(), new SizeEstimator() {

                @Override
                public long getSize() {
                    return 0;
                }

            });
        }

    }

    static class TestIndexPlan implements IndexPlan {

        @Override
        public double getCostPerExecution() {
            return 0;
        }

        @Override
        public double getCostPerEntry() {
            return 0;
        }

        @Override
        public long getEstimatedEntryCount() {
            return 0;
        }

        @Override
        public Filter getFilter() {
            return null;
        }

        @Override
        public void setFilter(Filter filter) {
        }

        @Override
        public boolean isDelayed() {
            return false;
        }

        @Override
        public boolean isFulltextIndex() {
            return false;
        }

        @Override
        public boolean includesNodeData() {
            return false;
        }

        @Override
        public List<OrderEntry> getSortOrder() {
            return null;
        }

        @Override
        public NodeState getDefinition() {
            return null;
        }

        @Override
        public String getPathPrefix() {
            return null;
        }

        @Override
        public boolean getSupportsPathRestriction() {
            return false;
        }

        @Override
        public @Nullable PropertyRestriction getPropertyRestriction() {
            return null;
        }

        @Override
        public IndexPlan copy() {
            return null;
        }

        @Override
        public @Nullable Object getAttribute(String name) {
            if (name.equals(FulltextIndex.ATTR_PLAN_RESULT)) {
                IndexDefinition indexDef = new IndexDefinition(EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE,
                        "");
                PlanResult pr = new PlanResult("test", indexDef, null);
                pr.disableUniquePaths();
                return pr;
            }
            return null;
        }

        @Override
        public @Nullable String getPlanName() {
            return null;
        }

        @Override
        public boolean isDeprecated() {
            return false;
        }

    }

}
