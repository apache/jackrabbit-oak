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
package org.apache.jackrabbit.oak.query;

import static com.google.common.collect.ImmutableList.of;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.jackrabbit.oak.query.ast.AndImpl;
import org.apache.jackrabbit.oak.query.ast.ComparisonImpl;
import org.apache.jackrabbit.oak.query.ast.ConstraintImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeImpl;
import org.apache.jackrabbit.oak.query.ast.FullTextSearchImpl;
import org.apache.jackrabbit.oak.query.ast.OrImpl;
import org.junit.Test;

public class QueryCostOverheadTest {
    @Test
    public void getCostOverhead() {
        final double allowedDelta = 10;
        QueryImpl query;
        UnionQueryImpl union;
        ConstraintImpl c, c1, c2, c3, c4, c5;
        
        union = new UnionQueryImpl(false, null, null, null);
        assertEquals("we always expect 0 from a `UnionQueryImpl`", 0, union.getCostOverhead(),
            allowedDelta);
        
        c = mock(OrImpl.class);
        c1 = mock(ComparisonImpl.class);
        c2 = mock(FullTextSearchImpl.class);
        when(c.getConstraints()).thenReturn(of(c1, c2));
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(Double.MAX_VALUE, query.getCostOverhead(), allowedDelta);

        c = mock(OrImpl.class);
        c1 = mock(ComparisonImpl.class);
        c2 = mock(FullTextSearchImpl.class);
        c3 = mock(FullTextSearchImpl.class);
        when(c.getConstraints()).thenReturn(of(c1, c2, c3));
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(Double.MAX_VALUE, query.getCostOverhead(), allowedDelta);
        
        c1 = mock(OrImpl.class);
        c2 = mock(FullTextSearchImpl.class);
        c3 = mock(FullTextSearchImpl.class);
        c4 = mock(ComparisonImpl.class);
        when(c1.getConstraints()).thenReturn(of(c2, c3, c4));
        c = mock(AndImpl.class);
        c5 = mock(DescendantNodeImpl.class);
        when(c.getConstraints()).thenReturn(of(c1, c5));
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(Double.MAX_VALUE, query.getCostOverhead(), allowedDelta);
        
        c = mock(FullTextSearchImpl.class);
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(0, query.getCostOverhead(), allowedDelta);

        c = mock(OrImpl.class);
        c1 = mock(FullTextSearchImpl.class);
        c2 = mock(FullTextSearchImpl.class);
        c3 = mock(FullTextSearchImpl.class);
        when(c.getConstraints()).thenReturn(of(c1, c2, c3));
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(0, query.getCostOverhead(), allowedDelta);
        
        c = mock(AndImpl.class);
        c1 = mock(ComparisonImpl.class);
        c2 = mock(FullTextSearchImpl.class);
        c3 = mock(FullTextSearchImpl.class);
        when(c.getConstraints()).thenReturn(of(c1, c2, c3));
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(0, query.getCostOverhead(), allowedDelta);

        c = mock(AndImpl.class);
        c1 = mock(ComparisonImpl.class);
        c2 = mock(ComparisonImpl.class);
        when(c.getConstraints()).thenReturn(of(c1, c2, c3));
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(0, query.getCostOverhead(), allowedDelta);

        c2 = mock(ComparisonImpl.class);
        query = new QueryImpl(null, null, c, null, null, null);
        assertEquals(0, query.getCostOverhead(), allowedDelta);
    }
}
