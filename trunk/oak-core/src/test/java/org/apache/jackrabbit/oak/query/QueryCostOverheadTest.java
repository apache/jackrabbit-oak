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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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
        QueryImpl query;
        UnionQueryImpl union;
        ConstraintImpl c, c1, c2, c3, c4, c5;
        
        c1 = new ComparisonImpl(null, null, null);
        c2 = new FullTextSearchImpl(null, null, null);
        union = new UnionQueryImpl(false,
                createQuery(c1),
                createQuery(c2),
                null);
        assertFalse("we always expect false from a `UnionQueryImpl`", 
                union.containsUnfilteredFullTextCondition());
        
        c1 = new ComparisonImpl(null, null, null);
        c2 = new FullTextSearchImpl(null, null, null);
        c = new OrImpl(c1, c2);
        query = createQuery(c);
        assertTrue(query.containsUnfilteredFullTextCondition());

        c1 = new ComparisonImpl(null, null, null);
        c2 = new FullTextSearchImpl(null, null, null);
        c3 = new FullTextSearchImpl(null, null, null);
        c = new OrImpl(of(c1, c2, c3));
        query = createQuery(c);
        assertTrue(query.containsUnfilteredFullTextCondition());
        
        c2 = new FullTextSearchImpl(null, null, null);
        c3 = new FullTextSearchImpl(null, null, null);
        c4 = new ComparisonImpl(null, null, null);
        c1 = new OrImpl(of(c2, c3, c4));
        c5 = mock(DescendantNodeImpl.class);
        c = new AndImpl(c1, c5);
        query = createQuery(c);
        assertTrue(query.containsUnfilteredFullTextCondition());
        
        c = new FullTextSearchImpl(null, null, null);
        query = createQuery(c);
        assertFalse(query.containsUnfilteredFullTextCondition());

        c1 = new FullTextSearchImpl(null, null, null);
        c2 = new FullTextSearchImpl(null, null, null);
        c3 = new FullTextSearchImpl(null, null, null);
        c = new OrImpl(of(c1, c2, c3));
        query = createQuery(c);
        assertFalse(query.containsUnfilteredFullTextCondition());
        
        c1 = new ComparisonImpl(null, null, null);
        c2 = new FullTextSearchImpl(null, null, null);
        c3 = new FullTextSearchImpl(null, null, null);
        c = new AndImpl(of(c1, c2, c3));
        query = createQuery(c);
        assertFalse(query.containsUnfilteredFullTextCondition());

        c1 = new ComparisonImpl(null, null, null);
        c2 = new ComparisonImpl(null, null, null);
        c = new AndImpl(of(c1, c2, c3));
        query = createQuery(c);
        assertFalse(query.containsUnfilteredFullTextCondition());
    }
    
    QueryImpl createQuery(ConstraintImpl c) {
        return new QueryImpl(null, null, c, null, null, null, null);
    }
}
