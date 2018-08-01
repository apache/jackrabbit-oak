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
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.UUID;

import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class ValidNamesTest extends AbstractRepositoryTest {

    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;
    private static final Map<NodeStoreFixture, NodeStore> STORES = Maps.newConcurrentMap();

    private Node testNode;

    private String unmappedNsPrefix;
    private String testPrefix;
    private String testNsUri;

    public ValidNamesTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Repository repo = createRepository(fixture);
        Session session = repo.login(getAdminCredentials());
        Node root = session.getRootNode();
        testNode = root.addNode(TEST_NODE);
        session.save();

        StringBuilder t = new StringBuilder();
        for (String prefix : session.getNamespacePrefixes()) {
            int l = t.length();
            if (prefix.length() > l) {
                t.append((char) (prefix.charAt(l) ^ 1));
            } else {
                t.append('x');
            }
        }
        unmappedNsPrefix = t.toString();

        for (String p : testNode.getSession().getNamespacePrefixes()) {
            if (p.length() != 0) {
                String u = testNode.getSession().getNamespaceURI(p);
                if (u.contains(":")) {
                    testPrefix = p;
                    testNsUri = u;
                }
            }
        }
        assertNotNull(testPrefix);
        assertNotNull(testNsUri);
    }

    @After
    public void tearDown() throws RepositoryException {
        Session s = testNode.getSession();
        s.removeItem(TEST_PATH);
        s.save();
        Repository r = s.getRepository();
        s.logout();
        dispose(r);
    }

    @AfterClass
    public static void disposeStores() throws Exception {
        for (Map.Entry<NodeStoreFixture, NodeStore> e : STORES.entrySet()) {
            e.getKey().dispose(e.getValue());
        }
        STORES.clear();
    }

    @Test
    public void testSimple() throws RepositoryException {
        nameTest("foo");
    }

    // TODO: questionable exception
    @Test(expected = ItemExistsException.class)
    public void testDot() throws RepositoryException {
        nameTest(".");
    }

    @Test
    public void testDotFoo() throws RepositoryException {
        nameTest(".foo");
    }

    // TODO: questionable exception
    @Test(expected = ItemExistsException.class)
    public void testDotDot() throws RepositoryException {
        nameTest("..");
    }

    @Test
    public void testDotDotFoo() throws RepositoryException {
        nameTest("..foo");
    }

    @Test
    public void testTrailingDot() throws RepositoryException {
        nameTest("foo.");
    }

    // TODO: questionable exception
    @Test(expected = RepositoryException.class)
    public void testLeadingBlank() throws RepositoryException {
        nameTest(" foo");
    }

    // TODO: questionable exception
    @Test(expected = RepositoryException.class)
    public void testTrailingBlank() throws RepositoryException {
        nameTest("foo ");
    }

    // TODO: questionable exception
    @Test(expected = PathNotFoundException.class)
    public void testEnclosedSlash() throws RepositoryException {
        nameTest("foo/bar");
    }

    // TODO: questionable exception
    @Test(expected = PathNotFoundException.class)
    public void testEnclosedPipe() throws RepositoryException {
        nameTest("foo|bar");
    }

    // TODO: questionable exception
    @Test(expected = PathNotFoundException.class)
    public void testEnclosedStar() throws RepositoryException {
        nameTest("foo*bar");
    }

    // TODO: questionable exception
    @Test(expected = PathNotFoundException.class)
    public void testEnclosedOpenBracket() throws RepositoryException {
        nameTest("foo[bar");
    }

    // TODO: questionable exception
    @Test(expected = PathNotFoundException.class)
    public void testEnclosedCloseBracket() throws RepositoryException {
        nameTest("foo]bar");
    }

    // TODO: questionable exception
    @Test(expected = RepositoryException.class)
    public void testLeadingColon() throws RepositoryException {
        nameTest(":foo");
    }

    // TODO: questionable exception
    @Test(expected = RepositoryException.class)
    public void testEnclosedUnmappedNsColon() throws RepositoryException {
        nameTest(unmappedNsPrefix + ":bar");
    }

    // TODO seems to be a bug
    @Test
    public void testEmptyNameInCurlys() throws RepositoryException {
        Node n = nameTest("{}foo");
        assertEquals("foo", n.getName());
    }

    @Test
    public void testSingleEnclosedOpenCurly() throws RepositoryException {
        nameTest("foo{bar");
    }

    @Test
    public void testSingleEnclosedCloseCurly() throws RepositoryException {
        nameTest("foo}bar");
    }

    @Test
    public void testValidLocalNameInCurlys() throws RepositoryException {
        Node n = nameTest("{foo}bar");
        assertEquals("{foo}bar", n.getName());
    }

    // TODO: questionable exception
    @Test(expected = RepositoryException.class)
    public void testNonUriInCurlys() throws RepositoryException {
        nameTest("{/}bar");
    }

    @Test
    public void testValidNamespaceUriInCurlys() throws RepositoryException {
        Node n = nameTest("{" + testNsUri + "}foo");
        assertEquals(testPrefix + ":foo", n.getName());
    }

    // TODO: questionable exception
    @Test(expected = RepositoryException.class)
    public void testValidNamespaceUriInCurlysWrongPlace() throws RepositoryException {
        nameTest("x{" + testNsUri + "}foo");
    }

    // TODO: questionable exception
    @Test(expected = RepositoryException.class)
    public void testValidNamespaceUriInCurlysNoLocalName() throws RepositoryException {
        nameTest("{" + testNsUri + "}");
    }

    // TODO better exception - or maybe this should pass?
    @Test(expected = RepositoryException.class)
    public void testQualifiedNameWithUnmappedNsUri() throws RepositoryException {
        String ns = "urn:uuid:" + UUID.randomUUID().toString();
        Node n = nameTest("{" + ns + "}foo");
        String pref = n.getSession().getNamespacePrefix(ns);
        assertEquals(pref + ":foo", n.getName());
    }

    @Test
    public void testEnclosedPercent() throws RepositoryException {
        nameTest("foo%bar");
    }

    @Test
    public void testEnclosedBlank() throws RepositoryException {
        nameTest("foo bar");
    }

    @Test(expected = RepositoryException.class)
    public void testEnclosedTab() throws RepositoryException {
        nameTest("foo\tbar");
    }

    @Test(expected = RepositoryException.class)
    public void testEnclosedLf() throws RepositoryException {
        nameTest("foo\nbar");
    }

    @Test(expected = RepositoryException.class)
    public void testEnclosedCr() throws RepositoryException {
        nameTest("foo\rbar");
    }

    @Test
    public void testEnclosedNonBreakingSpace() throws RepositoryException {
        nameTest("foo\u00a0bar");
    }

    // OAK-4587
    @Test(expected = RepositoryException.class)
    public void testEnclosedIdeographicSpace() throws RepositoryException {
        nameTest("foo\u3000bar");
    }

    @Test
    public void testUnpairedSurrogate() throws RepositoryException {
        // see OAK-5506
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("segment"));
        nameTest("foo\ud800");
    }

    @Test
    public void testSurrogate() throws RepositoryException {
        nameTest("foo\uD83D\uDCA9");
    }

    private Node nameTest(String nodeName) throws RepositoryException {
        Node n = testNode.addNode(nodeName);
        testNode.getSession().save();
        try {
            return testNode.getSession().getNode(n.getPath());
        } catch (RepositoryException ex) {
            fail(ex.getMessage());
            return null;
        }
    }

    private Repository createRepository(NodeStoreFixture fixture)
            throws RepositoryException {
        NodeStore ns = null;
        for (Map.Entry<NodeStoreFixture, NodeStore> e : STORES.entrySet()) {
            if (e.getKey().getClass().equals(fixture.getClass())) {
                ns = e.getValue();
            }
        }
        if (ns == null) {
            ns = createNodeStore(fixture);
            STORES.put(fixture, ns);
        }
        return createRepository(ns);
    }
}