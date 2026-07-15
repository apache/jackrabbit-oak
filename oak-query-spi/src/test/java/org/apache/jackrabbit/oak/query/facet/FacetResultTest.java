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

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.query.facet.FacetResult.Facet;
import org.apache.jackrabbit.oak.query.facet.FacetResult.FacetResultRow;
import org.junit.Test;

import javax.jcr.Value;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.query.QueryConstants.REP_FACET;
import static org.junit.Assert.*;
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

    @Test
    public void simpleMergeFacets() {
        String r1c1Facet = json(f("l1", 2), f("l2", 1));
        String r2c1Facet = json(f("l2", 4), f("l1", 1));

        FacetResult merged = facet(new FacetColumn("x", r1c1Facet, r2c1Facet));

        FacetResult expected = facet(new FacetColumn("x", json(f("l2", 5), f("l1", 3))));

        verify(expected, merged);
    }

    @Test
    public void uniqueLabelsMergeFacets() {
        String r1c1Facet = json(f("l1", 1));
        String r2c1Facet = json(f("l2", 2));

        FacetResult merged = facet(new FacetColumn("x", r1c1Facet, r2c1Facet));

        FacetResult expected = facet(new FacetColumn("x", json(f("l2", 2), f("l1", 1))));

        verify(expected, merged);
    }

    @Test
    public void multipleColumns() {
        String r1c1Facet = json(f("l1", 1));
        String r2c1Facet = json(f("l2", 2));

        String r1c2Facet = json(f("m1", 2));
        String r2c2Facet = json(f("m2", 1));

        FacetResult merged = facet(
                new FacetColumn("x", r1c1Facet, r2c1Facet),
                new FacetColumn("y", r1c2Facet, r2c2Facet)
        );

        FacetResult expected = facet(
                new FacetColumn("x", json(f("l2", 2), f("l1", 1))),
                new FacetColumn("y", json(f("m1", 2), f("m2", 1)))
        );

        verify(expected, merged);
    }

    @Test
    public void multipleColumnsWithNullColumns() {
        String r2c1Facet = json(f("l1", 1));
        String r1c2Facet = json(f("m1", 1));

        FacetResult merged = facet(
                new FacetColumn("x", null, r2c1Facet),
                new FacetColumn("y", r1c2Facet, null)
        );

        FacetResult expected = facet(
                new FacetColumn("x", json(f("l1", 1))),
                new FacetColumn("y", json(f("m1", 1)))
        );

        verify(expected, merged);
    }

    private FacetResult facet(FacetColumn ... facetColumns) {
        String[] colNames = new String[facetColumns.length];
        colNames[0] = facetColumns[0].colName;

        int numRows = facetColumns[0].facets.length;

        for (int i = 1; i < facetColumns.length; i++) {
            assertEquals("numRows for col num " + i + " wasn't same as first", numRows, facetColumns[i].facets.length);

            colNames[i] = facetColumns[i].colName;
        }

        FacetResultRow[] facetResultRows = new FacetResultRow[numRows];

        for (int i = 0; i < numRows; i++) {
            Map<String, String> columns = new HashMap<>();

            for (FacetColumn col : facetColumns) {
                columns.put(col.colName, col.facets[i]);
            }

            facetResultRows[i] = new FacetResultRow() {
                final Map<String, String> cols = columns;
                @Override
                public String getValue(String columnName) {
                    return cols.get(columnName);
                }
            };
        }

        return new FacetResult(colNames, facetResultRows);
    }

    private static String json(Facet ... facets) {
        JsopBuilder builder = new JsopBuilder();
        builder.object();
        for (Facet facet : facets) {
            builder.key(facet.getLabel());
            builder.value(facet.getCount());
        }
        builder.endObject();

        return builder.toString();
    }

    private static class FacetColumn {
        final String colName;
        final String[] facets;

        FacetColumn(String colName, String ... facets) {
            this.colName = REP_FACET + "(" + colName + ")";
            this.facets = facets;
        }
    }

    private static Facet f(String label, int count) {
        return new Facet(label, count);
    }

    private static void verify(FacetResult expected, FacetResult result) {
        assertEquals("Dimension mismatch", expected.getDimensions(), result.getDimensions());

        for (String dim : expected.getDimensions()) {
            List<Facet> expectedFacets = expected.getFacets(dim);
            List<Facet> resultFacets = result.getFacets(dim);

            for (int i = 0; i < expectedFacets.size(); i++) {
                Facet expectedFacet = expectedFacets.get(i);
                Facet resultFacet = resultFacets.get(i);

                assertEquals("label mismatch for dim " + dim, expectedFacet.getLabel(), resultFacet.getLabel());
                assertEquals("count mismatch for dim " + dim, expectedFacet.getCount(), resultFacet.getCount());
            }
        }
    }
}