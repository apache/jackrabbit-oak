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
import javax.jcr.security.Privilege;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.core.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.junit.After;
import org.junit.Before;

import static com.google.common.collect.Sets.newHashSet;
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
        node.setProperty(FulltextIndexConstants.PROP_FACETS, true);
        node.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        node = props.getNode("allProps");
        node.setProperty(FulltextIndexConstants.PROP_FACETS, true);
        markIndexForReindex();
        superuser.save();
        superuser.refresh(true);

        if (!superuser.nodeExists(FACET_CONFING_NODE_PATH)) {
            node = superuser.getNode(INDEX_CONFING_NODE_PATH);
            node.addNode(FulltextIndexConstants.FACETS);
            markIndexForReindex();
            superuser.save();
            superuser.refresh(true);
        }
    }

    @After
    protected void tearDown() throws Exception {
        assertTrue(superuser.nodeExists("/oak:index/luceneGlobal/facets"));

        if (superuser.nodeExists(FACET_CONFING_PROP_PATH)) {
            superuser.getProperty(FulltextIndexConstants.PROP_FACETS).remove();
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
        facetsConfig.setProperty(FulltextIndexConstants.PROP_FACETS_TOP_CHILDREN, 11);
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
        facetsConfig.setProperty(FulltextIndexConstants.PROP_FACETS_TOP_CHILDREN, 7);
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

    // OAK-7975
    public void testFacetWithNoIndexedValues() throws Exception {
        Node content = testRootNode.addNode("absentDimFacets");

        content.addNode("bar").setProperty("text", "lorem ipsum");

        superuser.save();

        String query;
        FacetResult facetResult;
        List<FacetResult.Facet> facets;

        // test with single facet column which has no indexed value yet
        query = "select [rep:facet(jc/text)] from [nt:base] where contains(*, 'ipsum')";

        facetResult = new FacetResult(qm.createQuery(query, Query.JCR_SQL2).execute());

        assertNotNull(facetResult);
        assertTrue(facetResult.getDimensions().isEmpty());

        // test with requesting multiple facet columns - one would get facets other won't
        query = "select [rep:facet(text)], [rep:facet(jc/text)] from [nt:base] where contains(*, 'ipsum')";

        facetResult = new FacetResult(qm.createQuery(query, Query.JCR_SQL2).execute());

        assertNotNull(facetResult);
        assertEquals(newHashSet("text"), facetResult.getDimensions());

        facets = facetResult.getFacets("text");
        assertEquals(1, facets.size());

        assertEquals("lorem ipsum", facets.get(0).getLabel());
        assertEquals(1, facets.get(0).getCount());
    }

    public void testNoFacetsIfNoAccess() throws Exception {
        deny(testRootNode.addNode("test1")).setProperty("jcr:title", "test1");
        deny(testRootNode.addNode("test2")).addNode("child").setProperty("jcr:title", "test2");
        deny(testRootNode.addNode("test3").addNode("child")).setProperty("jcr:title", "test3");
        superuser.save();

        Session anonUser = getHelper().getReadOnlySession();
        QueryManager queryManager = anonUser.getWorkspace().getQueryManager();
        Query q = queryManager.createQuery("//*[@jcr:title]/(rep:facet(jcr:title))", Query.XPATH);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);

        assertNotNull("facetResult is null", facetResult);
        assertTrue(facetResult.getDimensions().isEmpty());
    }

    public void testOnlyAllowedFacetLabelsShowUp() throws Exception {
        deny(testRootNode.addNode("test1")).setProperty("jcr:title", "test1");
        deny(testRootNode.addNode("test2")).addNode("child").setProperty("jcr:title", "test2");
        testRootNode.addNode("test3").addNode("child").setProperty("jcr:title", "test3");
        superuser.save();

        Session anonUser = getHelper().getReadOnlySession();
        QueryManager queryManager = anonUser.getWorkspace().getQueryManager();
        Query q = queryManager.createQuery("//*[@jcr:title]/(rep:facet(jcr:title))", Query.XPATH);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);

        assertNotNull("facetResult is null", facetResult);
        assertEquals("Unexpected number of dimension", 1, facetResult.getFacets("jcr:title").size());
        FacetResult.Facet facet = facetResult.getFacets("jcr:title").get(0);
        assertEquals("Unexpected facet label", "test3", facet.getLabel());
        assertEquals("Unexpected facet count", 1, facet.getCount());
    }

    public void testInaccessibleFacetCounts() throws Exception {
        deny(testRootNode.addNode("test1")).setProperty("jcr:title", "test");
        deny(testRootNode.addNode("test2")).addNode("child").setProperty("jcr:title", "test");
        testRootNode.addNode("test3").addNode("child").setProperty("jcr:title", "test");
        testRootNode.addNode("test4").addNode("child").setProperty("jcr:title", "another-test");
        superuser.save();

        Session anonUser = getHelper().getReadOnlySession();
        QueryManager queryManager = anonUser.getWorkspace().getQueryManager();
        Query q = queryManager.createQuery("//*[@jcr:title]/(rep:facet(jcr:title))", Query.XPATH);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);

        assertNotNull("facetResult is null", facetResult);
        assertEquals("Unexpected number of labels", 2, facetResult.getFacets("jcr:title").size());
        Map<String, Integer> facets = facetResult.getFacets("jcr:title")
                .stream().collect(Collectors.toMap(FacetResult.Facet::getLabel, FacetResult.Facet::getCount));
        assertEquals("Unexpected facet count for jcr:title", 1, (int)facets.get("test"));
        assertEquals("Unexpected facet count for jcr:title", 1, (int)facets.get("another-test"));
    }

    public void testAllowedSubNodeFacet() throws Exception {
        allow(
            deny(testRootNode.addNode("parent")).addNode("child")
        ).setProperty("jcr:title", "test");
        superuser.save();

        Session anonUser = getHelper().getReadOnlySession();
        QueryManager queryManager = anonUser.getWorkspace().getQueryManager();
        Query q = queryManager.createQuery("//*[@jcr:title]/(rep:facet(jcr:title))", Query.XPATH);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);

        assertNotNull("facetResult is null", facetResult);
        assertEquals("Unexpected number of labels", 1, facetResult.getFacets("jcr:title").size());
        FacetResult.Facet facet = facetResult.getFacets("jcr:title").get(0);
        assertEquals("Unexpected facet label", "test", facet.getLabel());
        assertEquals("Unexpected facet count", 1, facet.getCount());
    }

    public void testNoIndexedFacetedQuery() throws Exception {
        Query q = qm.createQuery("//*[@jcr:title]/(rep:facet(non-indexed/jcr:title))", Query.XPATH);
        QueryResult result = q.execute();

        try {
            new FacetResult(result);

            fail("Facet evaluation must fail if the index doesn't support the required faceted properties");
        } catch (RuntimeException iae) {
            if (iae.getCause() instanceof IllegalArgumentException) {
                // expected and hence ignored
            } else {
                throw iae;
            }
        }

        q = qm.createQuery("//*[@jcr:title]/(rep:facet(non-indexed1/jcr:title) | rep:facet(non-indexed2/jcr:title))", Query.XPATH);
        result = q.execute();

        try {
            new FacetResult(result);

            fail("Facet evaluation must fail if the index doesn't support any of the required faceted properties");
        } catch (RuntimeException iae) {
            if (iae.getCause() instanceof IllegalArgumentException) {
                // expected and hence ignored
            } else {
                throw iae;
            }
        }
    }

    public void testSomeNonIndexedFacetedQuery() throws Exception {
        Query q = qm.createQuery("//*[@jcr:title]/(rep:facet(non-indexed/jcr:title) | rep:facet(jcr:title))", Query.XPATH);
        QueryResult result = q.execute();

        try {
            new FacetResult(result);

            fail("Facet evaluation must fail if the index doesn't support some of the required faceted properties");
        } catch (RuntimeException iae) {
            if (iae.getCause() instanceof IllegalArgumentException) {
                // expected and hence ignored
            } else {
                throw iae;
            }
        }
    }

    public void testAcRelativeFacetsAccessControl() throws Exception {
        deny(testRootNode.addNode("test1")).addNode("jc").setProperty("text", "test_1");
        deny(testRootNode.addNode("test2").addNode("jc")).setProperty("text", "test_2");
        testRootNode.addNode("test3").addNode("jc").setProperty("text", "test_3");
        superuser.save();

        Session anonUser = getHelper().getReadOnlySession();
        QueryManager queryManager = anonUser.getWorkspace().getQueryManager();
        Query q = queryManager.createQuery("//*[jcr:contains(jc/@text, 'test')]/(rep:facet(jc/text))", Query.XPATH);
        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);

        assertNotNull("facetResult is null", facetResult);
        assertEquals("Unexpected number of dimension", 1, facetResult.getFacets("jc/text").size());
        FacetResult.Facet facet = facetResult.getFacets("jc/text").get(0);
        assertEquals("Unexpected facet label", "test_3", facet.getLabel());
        assertEquals("Unexpected facet count", 1, facet.getCount());
    }

    // OAK-7605
    public void testDistinctUnionWithDifferentFacetsOnSubQueries() throws Exception {
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "t1");
        n1.setProperty("name","Node1");
        // make sure that facet values from both ends of OR clause are different
        // the test is essentially that facet columns don't define uniqueness of a row
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "t1");
        n3.setProperty("name","Node3");
        superuser.save();

        String xpath = "//*[@text = 't1' or @name = 'Node1']/(rep:facet(text))";
        Query q = qm.createQuery(xpath, Query.XPATH);
        QueryResult result = q.execute();
        RowIterator rows=result.getRows();

        assertEquals(2, rows.getSize());
    }

    public void testMergedFacetsOverUnionUniqueLabels() throws Exception {
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "t1");
        n1.setProperty("x", "x1");
        n1.setProperty("name","Node1");

        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "t2");
        n2.setProperty("x", "x2");
        n2.setProperty("name","Node2");

        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "t3");
        n3.setProperty("x", "x3");
        n3.setProperty("name","Node3");
        superuser.save();

        String xpath = "//*[@name = 'Node1' or @text = 't2' or @x = 'x3']/(rep:facet(text))";

        Query q = qm.createQuery(xpath, Query.XPATH);

        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);

        assertEquals("Unexpected dimensions", newHashSet("text"), facetResult.getDimensions());

        List<FacetResult.Facet> facets = facetResult.getFacets("text");

        Set<String> facetLabels = newHashSet();
        for (FacetResult.Facet facet : facets) {
            assertEquals("Unexpected facet count for " + facet.getLabel(), 1, facet.getCount());
            facetLabels.add(facet.getLabel());
        }

        assertEquals("Unexpected facet labels", newHashSet("t1", "t2", "t3"), facetLabels);
    }

    public void testMergedFacetsOverUnionSummingCount() throws Exception {
        // the distribution of nodes with t1 and t2 are intentionally across first and second set (below)
        // put such that second condition turns facet count around

        // first set of nodes matching first condition (x1 = v1)
        Node n11 = testRootNode.addNode("node11");
        n11.setProperty("text", "t1");
        n11.setProperty("x1","v1");
        Node n12 = testRootNode.addNode("node12");
        n12.setProperty("text", "t1");
        n12.setProperty("x1","v1");
        Node n13 = testRootNode.addNode("node13");
        n13.setProperty("text", "t2");
        n13.setProperty("x1","v1");

        // second set of nodes matching second condition (x2 = v2)
        Node n21 = testRootNode.addNode("node21");
        n21.setProperty("text", "t2");
        n21.setProperty("x2","v2");
        Node n22 = testRootNode.addNode("node22");
        n22.setProperty("text", "t1");
        n22.setProperty("x2","v2");
        Node n23 = testRootNode.addNode("node23");
        n23.setProperty("text", "t1");
        n23.setProperty("x2","v2");
        Node n24 = testRootNode.addNode("node24");
        n24.setProperty("text", "t1");
        n24.setProperty("x2","v2");

        superuser.save();

        String xpath = "//*[@x1 = 'v1' or @x2 = 'v2']/(rep:facet(text))";

        Query q = qm.createQuery(xpath, Query.XPATH);

        QueryResult result = q.execute();
        FacetResult facetResult = new FacetResult(result);

        assertEquals("Unexpected dimensions", newHashSet("text"), facetResult.getDimensions());

        List<FacetResult.Facet> facets = facetResult.getFacets("text");
        assertEquals("Incorrect facet label list size", 2, facets.size());

        FacetResult.Facet facet = facets.get(0);
        assertEquals("t1", facet.getLabel());
        assertEquals(5, facet.getCount());

        facet = facets.get(1);
        assertEquals("t2", facet.getLabel());
        assertEquals(2, facet.getCount());
    }

    public Node deny(Node node) throws RepositoryException {
        AccessControlUtils.deny(node, "anonymous", Privilege.JCR_ALL);
        return node;
    }

    public Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    private void markIndexForReindex() throws RepositoryException {
        superuser.getNode("/oak:index/luceneGlobal").setProperty(REINDEX_PROPERTY_NAME, true);
    }
}