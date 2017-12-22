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
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.util.List;

import org.apache.jackrabbit.core.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.junit.After;
import org.junit.Before;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;

/**
 * Test for faceting capabilities via JCR API
 */
public class FacetTest extends AbstractQueryTest {

    public static final String FACET_CONFING_PROP_PATH = "/oak:index/luceneGlobal/indexRules/nt:base/properties/allProps/facets";
    public static final String FACET_CONFING_NODE_PATH = "/oak:index/luceneGlobal/facets";
    public static final String INDEX_CONFING_NODE_PATH = "/oak:index/luceneGlobal";

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        if (superuser.itemExists(FACET_CONFING_PROP_PATH)) {
            superuser.getItem(FACET_CONFING_PROP_PATH).remove();
        }

        Node props = superuser.getNode("/oak:index/luceneGlobal/indexRules/nt:base/properties");
        if (props.hasNode("relative")) {
            props.getNode("relative").remove();
        }

        Node node = props.addNode("relative");
        node.setProperty("name", "jc/text");
        node.setProperty(LuceneIndexConstants.PROP_FACETS, true);
        node.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        node = props.getNode("allProps");
        node.setProperty(LuceneIndexConstants.PROP_FACETS, true);
        markIndexForReindex();
        superuser.save();
        superuser.refresh(true);

        if (!superuser.nodeExists(FACET_CONFING_NODE_PATH)) {
            node = superuser.getNode(INDEX_CONFING_NODE_PATH);
            node.addNode(LuceneIndexConstants.FACETS);
            markIndexForReindex();
            superuser.save();
            superuser.refresh(true);
        }
    }

    @After
    protected void tearDown() throws Exception {
        assertTrue(superuser.nodeExists("/oak:index/luceneGlobal/facets"));

        if (superuser.nodeExists(FACET_CONFING_PROP_PATH)) {
            superuser.getProperty(LuceneIndexConstants.PROP_FACETS).remove();
            superuser.save();
            superuser.refresh(true);
        }

        if (superuser.nodeExists("/oak:index/luceneGlobal/indexRules/nt:base/properties/relative")) {
            superuser.removeItem("/oak:index/luceneGlobal/indexRules/nt:base/properties/relative");
            superuser.save();
            superuser.refresh(true);
        }

        if (superuser.nodeExists(FACET_CONFING_NODE_PATH)) {
            superuser.getNode(FACET_CONFING_NODE_PATH).remove();
            superuser.save();
            superuser.refresh(true);
        }

        super.tearDown();
    }

    public void testFacetsNA() throws Exception {
        if (superuser.itemExists(FACET_CONFING_PROP_PATH)) {
            superuser.getItem(FACET_CONFING_PROP_PATH).remove();
            markIndexForReindex();
            superuser.save();
        }
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "foo");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "bar");
        session.save();

        String sql2 = "select [jcr:path], [rep:facet(text)] from [nt:base] " +
                "where contains([text], 'foo OR bar')";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertTrue(facetResult.getDimensions().isEmpty());
    }

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

    public void testFacetRetrievalXPath() throws Exception {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "oh hallo");
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String xpath = "//*[jcr:contains(@text, 'hello OR hallo')]/(rep:facet(text)) order by jcr:path";
        Query q = qm.createQuery(xpath, Query.XPATH);
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

    public void testFacetRetrievalRelativeProperty() throws Exception {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        n1.addNode("jc").setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.addNode("jc").setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.addNode("jc").setProperty("text", "oh hallo");
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String sql2 = "select [jcr:path], [rep:facet(jc/text)] from [nt:base] " +
                "where contains([jc/text], 'hello OR hallo') order by [jcr:path]";
        Query q = qm.createQuery(sql2, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(1, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains("jc/text"));
        List<FacetResult.Facet> facets = facetResult.getFacets("jc/text");
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

    public void testFacetRetrievalRelativePropertyXPath() throws Exception {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        n1.addNode("jc").setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.addNode("jc").setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.addNode("jc").setProperty("text", "oh hallo");
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        String xpath = "//*[jcr:contains(jc/@text, 'hello OR hallo')]/(rep:facet(jc/text)) order by jcr:path";
        Query q = qm.createQuery(xpath, Query.XPATH);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertNotNull(facetResult.getDimensions());
        assertEquals(1, facetResult.getDimensions().size());
        assertTrue(facetResult.getDimensions().contains("jc/text"));
        List<FacetResult.Facet> facets = facetResult.getFacets("jc/text");
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

    public void testFacetRetrievalDefaultNumberOfFacets() throws RepositoryException {
        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        String pn = "jcr:title";
        n1.setProperty(pn, "hello 1");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty(pn, "hallo 2");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty(pn, "hallo 3");
        Node n4 = testRootNode.addNode("node4");
        n4.setProperty(pn, "hallo 4");
        Node n5 = testRootNode.addNode("node5");
        n5.setProperty(pn, "hallo 5");
        Node n6 = testRootNode.addNode("node6");
        n6.setProperty(pn, "hallo 6");
        Node n7 = testRootNode.addNode("node7");
        n7.setProperty(pn, "hallo 7");
        Node n8 = testRootNode.addNode("node8");
        n8.setProperty(pn, "hallo 8");
        Node n9 = testRootNode.addNode("node9");
        n9.setProperty(pn, "hallo 9");
        Node n10 = testRootNode.addNode("node10");
        n10.setProperty(pn, "hallo 10");
        Node n11 = testRootNode.addNode("node11");
        n11.setProperty(pn, "hallo 11");
        Node n12 = testRootNode.addNode("node12");
        n12.setProperty(pn, "hallo 12");
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
        assertEquals(10, facets.size());
    }

    public void testFacetRetrievalNumberOfFacetsConfiguredHigherThanDefault() throws RepositoryException {

        Node facetsConfig = superuser.getNode(FACET_CONFING_NODE_PATH);
        facetsConfig.setProperty(LuceneIndexConstants.PROP_FACETS_TOP_CHILDREN, 11);
        markIndexForReindex();
        superuser.save();
        superuser.refresh(true);

        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        String pn = "jcr:title";
        n1.setProperty(pn, "hello 1");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty(pn, "hallo 2");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty(pn, "hallo 3");
        Node n4 = testRootNode.addNode("node4");
        n4.setProperty(pn, "hallo 4");
        Node n5 = testRootNode.addNode("node5");
        n5.setProperty(pn, "hallo 5");
        Node n6 = testRootNode.addNode("node6");
        n6.setProperty(pn, "hallo 6");
        Node n7 = testRootNode.addNode("node7");
        n7.setProperty(pn, "hallo 7");
        Node n8 = testRootNode.addNode("node8");
        n8.setProperty(pn, "hallo 8");
        Node n9 = testRootNode.addNode("node9");
        n9.setProperty(pn, "hallo 9");
        Node n10 = testRootNode.addNode("node10");
        n10.setProperty(pn, "hallo 10");
        Node n11 = testRootNode.addNode("node11");
        n11.setProperty(pn, "hallo 11");
        Node n12 = testRootNode.addNode("node12");
        n12.setProperty(pn, "hallo 12");
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
        assertEquals(11, facets.size());
    }

    public void testFacetRetrievalNumberOfFacetsConfiguredLowerThanDefault() throws RepositoryException {

        Node facetsConfig = superuser.getNode(FACET_CONFING_NODE_PATH);
        facetsConfig.setProperty(LuceneIndexConstants.PROP_FACETS_TOP_CHILDREN, 7);
        markIndexForReindex();
        superuser.save();
        superuser.refresh(true);

        Session session = superuser;
        Node n1 = testRootNode.addNode("node1");
        String pn = "jcr:title";
        n1.setProperty(pn, "hello 1");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty(pn, "hallo 2");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty(pn, "hallo 3");
        Node n4 = testRootNode.addNode("node4");
        n4.setProperty(pn, "hallo 4");
        Node n5 = testRootNode.addNode("node5");
        n5.setProperty(pn, "hallo 5");
        Node n6 = testRootNode.addNode("node6");
        n6.setProperty(pn, "hallo 6");
        Node n7 = testRootNode.addNode("node7");
        n7.setProperty(pn, "hallo 7");
        Node n8 = testRootNode.addNode("node8");
        n8.setProperty(pn, "hallo 8");
        Node n9 = testRootNode.addNode("node9");
        n9.setProperty(pn, "hallo 9");
        Node n10 = testRootNode.addNode("node10");
        n10.setProperty(pn, "hallo 10");
        Node n11 = testRootNode.addNode("node11");
        n11.setProperty(pn, "hallo 11");
        Node n12 = testRootNode.addNode("node12");
        n12.setProperty(pn, "hallo 12");
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
        assertEquals(7, facets.size());
    }

    // OAK-7078
    public void testFacetsOfResultSetThatDoesntContainDim() throws Exception {
        Node content = testRootNode.addNode("absentDimFacets");

        // create a document with a simple/tags property
        Node foo = content.addNode("foo");
        Node fooSimple = foo.addNode("jc");
        foo.setProperty("text", "lorem lorem");
        fooSimple.setProperty("text", new String[]{"tag1", "tag2"});
        // now create a document without simple/tags property
        Node bar = content.addNode("bar");
        bar.setProperty("text", "lorem ipsum");

        superuser.save();

        String query = "select [rep:facet(jc/text)] from [nt:base] where contains(*, 'ipsum')";

        Query q = qm.createQuery(query, Query.JCR_SQL2);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);
        assertNotNull(facetResult);
        assertTrue(facetResult.getDimensions().isEmpty());

        RowIterator rows = result.getRows();
        assertTrue(rows.hasNext());
        assertEquals(bar.getPath(), rows.nextRow().getPath());
        assertFalse(rows.hasNext());
    }

    private void markIndexForReindex() throws RepositoryException {
        superuser.getNode("/oak:index/luceneGlobal").setProperty(REINDEX_PROPERTY_NAME, true);
    }
}