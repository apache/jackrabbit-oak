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
package org.apache.jackrabbit.oak.query.facet;

import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FacetResult}
 */
public class FacetResultTest {

    @Test
    public void testResult() throws Exception {
        QueryResult queryResult = mock(QueryResult.class);
        when(queryResult.getColumnNames()).thenReturn(new String[]{"rep:facet(text)", "jcr:path", "rep:facet(jcr:title)"});
        RowIterator rows = mock(RowIterator.class);
        when(rows.hasNext()).thenReturn(true);
        Row row = mock(Row.class);
        Value value = mock(Value.class);
        when(value.getString()).thenReturn("{}");
        when(row.getValue("rep:facet(text)")).thenReturn(value);
        Value value2 = mock(Value.class);
        when(value2.getString()).thenReturn("{\"a\" : 2, \"b\" : 1}");
        when(row.getValue("rep:facet(jcr:title)")).thenReturn(value2);
        when(rows.nextRow()).thenReturn(row);
        when(queryResult.getRows()).thenReturn(rows);

        FacetResult facetResult = new FacetResult(queryResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(2, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains("text"));
        assertTrue(facetResult.getDimensions().contains("jcr:title"));
        assertNotNull(facetResult.getFacets("text"));
        assertTrue(facetResult.getFacets("text").isEmpty());
        assertNotNull(facetResult.getFacets("jcr:title"));
        assertEquals(2, facetResult.getFacets("jcr:title").size());
        assertEquals("a", facetResult.getFacets("jcr:title").get(0).getLabel());
        assertEquals(2, facetResult.getFacets("jcr:title").get(0).getCount(), 0);
        assertEquals("b", facetResult.getFacets("jcr:title").get(1).getLabel());
        assertEquals(1, facetResult.getFacets("jcr:title").get(1).getCount(), 0);
    }

}