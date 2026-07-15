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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.junit.Test;

/**
 * Test the advanced query index feature.
 */
public class AdvancedIndexTest {

    @Test
    public void builder() {
        IndexPlan.Builder b = new IndexPlan.Builder();
        IndexPlan plan = b.setEstimatedEntryCount(10).build();
        assertEquals(10, plan.getEstimatedEntryCount());
        b.setEstimatedEntryCount(20);
        assertEquals(10, plan.getEstimatedEntryCount());
    }

    @Test
    public void copy() throws Exception{
        Filter f = new FilterImpl(null, "SELECT * FROM [nt:file]", new QueryEngineSettings());
        IndexPlan.Builder b = new IndexPlan.Builder();
        IndexPlan plan1 = b.setEstimatedEntryCount(10).setFilter(f).setDelayed(true).build();

        IndexPlan plan2 = plan1.copy();
        plan2.setFilter(new FilterImpl(null, "SELECT * FROM [oak:Unstructured]", new QueryEngineSettings()));

        assertEquals(plan1.getEstimatedEntryCount(), 10);
        assertEquals(plan2.getEstimatedEntryCount(), 10);
        assertTrue(plan1.isDelayed());
        assertTrue(plan2.isDelayed());
        assertEquals(plan1.getFilter().getQueryStatement(), "SELECT * FROM [nt:file]");
        assertEquals(plan2.getFilter().getQueryStatement(), "SELECT * FROM [oak:Unstructured]");
    }

    @Test
    public void attribute() throws Exception{
        IndexPlan plan = new IndexPlan.Builder().setAttribute("foo", "bar").build();
        assertEquals("bar", plan.getAttribute("foo"));
    }
    
}
