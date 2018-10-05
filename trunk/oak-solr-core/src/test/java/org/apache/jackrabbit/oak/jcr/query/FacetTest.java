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
package org.apache.jackrabbit.oak.jcr.query;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import java.util.List;

import org.apache.jackrabbit.core.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.facet.FacetResult;

/**
 * Test for faceting capabilities via JCR API
 */
public class FacetTest extends AbstractQueryTest {

    public void testFacetRetrieval() throws Exception {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "oh hallo");
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String sql2 = "select [jcr:path], [rep:facet(text)] from [nt:base] " +
                "where contains([text], 'hello OR hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(1, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains("text"));
        List<FacetResult.Facet> facets = facetResult.getFacets("text");
        assertNotNull(facets);
        assertEquals("hallo", facets.get(0).getLabel());
        assertEquals(1, facets.get(0).getCount(), 0);
        assertEquals("hello", facets.get(1).getLabel());
        assertEquals(1, facets.get(1).getCount(), 0);
        assertEquals("oh hallo", facets.get(2).getLabel());
        assertEquals(1, facets.get(2).getCount(), 0);

        NodeIterator nodes = result.getNodes();
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertFalse(nodes.hasNext());
    }

    public void testFacetRetrievalMV() throws Exception {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "apache jackrabbit oak");
        n1.setProperty("tags", new String[]{"software", "repository", "apache"});
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "oak furniture");
        n2.setProperty("tags", "furniture");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("jcr:title", "oak cosmetics");
        n3.setProperty("tags", "cosmetics");
        Node n4 = testRootNode.addNode("node4");
        n4.setProperty("jcr:title", "oak and aem");
        n4.setProperty("tags", new String[]{"software", "repository", "aem"});
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String sql2 = "select [jcr:path], [rep:facet(tags)] from [nt:base] " +
                "where contains([jcr:title], 'oak') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(1, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains("tags"));
        List<FacetResult.Facet> facets = facetResult.getFacets("tags");
        assertNotNull(facets);
        assertEquals("repository", facets.get(0).getLabel());
        assertEquals(2, facets.get(0).getCount(), 0);
        assertEquals("software", facets.get(1).getLabel());
        assertEquals(2, facets.get(1).getCount(), 0);
        assertEquals("aem", facets.get(2).getLabel());
        assertEquals(1, facets.get(2).getCount(), 0);
        assertEquals("apache", facets.get(3).getLabel());
        assertEquals(1, facets.get(3).getCount(), 0);
        assertEquals("cosmetics", facets.get(4).getLabel());
        assertEquals(1, facets.get(4).getCount(), 0);
        assertEquals("furniture", facets.get(5).getLabel());
        assertEquals(1, facets.get(5).getCount(), 0);


        NodeIterator nodes = result.getNodes();
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertFalse(nodes.hasNext());
    }

    public void testFacetRetrievalWithAnonymousUser() throws Exception {
        Session session = superuser;

        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "oh hallo");
        session.save();

        session = getHelper().getReadOnlySession();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String sql2 = "select [jcr:path], [rep:facet(text)] from [nt:base] " +
                "where contains([text], 'hello OR hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(1, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains("text"));
        List<FacetResult.Facet> facets = facetResult.getFacets("text");
        assertNotNull(facets);
        assertEquals("hallo", facets.get(0).getLabel());
        assertEquals(1, facets.get(0).getCount(), 0);
        assertEquals("hello", facets.get(1).getLabel());
        assertEquals(1, facets.get(1).getCount(), 0);
        assertEquals("oh hallo", facets.get(2).getLabel());
        assertEquals(1, facets.get(2).getCount(), 0);

        NodeIterator nodes = result.getNodes();
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertFalse(nodes.hasNext());
    }

    public void testFacetRetrieval2() throws Exception {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        String pn = "jcr:title";
        n1.setProperty(pn, "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty(pn, "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty(pn, "oh hallo");
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String sql2 = "select [jcr:path], [rep:facet(" + pn + ")] from [nt:base] " +
                "where contains([" + pn + "], 'hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(1, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains(pn));
        List<FacetResult.Facet> facets = facetResult.getFacets(pn);
        assertNotNull(facets);
        assertEquals("hallo", facets.get(0).getLabel());
        assertEquals(1, facets.get(0).getCount(), 0);
        assertEquals("oh hallo", facets.get(1).getLabel());
        assertEquals(1, facets.get(1).getCount(), 0);

        NodeIterator nodes = result.getNodes();
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertFalse(nodes.hasNext());
    }

    public void testMultipleFacetsRetrieval() throws Exception {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        String pn = "jcr:title";
        String pn2 = "jcr:description";
        n1.setProperty(pn, "hello");
        n1.setProperty(pn2, "a");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty(pn, "hallo");
        n2.setProperty(pn2, "b");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty(pn, "oh hallo");
        n3.setProperty(pn2, "a");
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String sql2 = "select [jcr:path], [rep:facet(" + pn + ")], [rep:facet(" + pn2 + ")] from [nt:base] " +
                "where contains([" + pn + "], 'hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(2, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains(pn));
        assertTrue(facetResult.getDimensions().contains(pn2));
        List<FacetResult.Facet> facets = facetResult.getFacets(pn);
        assertNotNull(facets);
        assertEquals("hallo", facets.get(0).getLabel());
        assertEquals(1, facets.get(0).getCount(), 0);
        assertEquals("oh hallo", facets.get(1).getLabel());
        assertEquals(1, facets.get(1).getCount(), 0);
        List<FacetResult.Facet> facets1 = facetResult.getFacets(pn2);
        assertNotNull(facets1);
        assertEquals("a", facets1.get(0).getLabel());
        assertEquals(1, facets1.get(0).getCount(), 0);
        assertEquals("b", facets1.get(1).getLabel());
        assertEquals(1, facets1.get(1).getCount(), 0);

        NodeIterator nodes = result.getNodes();
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertTrue(nodes.hasNext());
        assertNotNull(nodes.nextNode());
        assertFalse(nodes.hasNext());
    }

}