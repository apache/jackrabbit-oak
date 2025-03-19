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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.jcr.ItemExistsException;
import javax.jcr.NamespaceException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.util.KnownIssuesIgnoreRule;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ValidNamesTest extends AbstractRepositoryTest {

    @Rule
    public KnownIssuesIgnoreRule customIgnoreRule = new KnownIssuesIgnoreRule();
    
    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;
    private static final Map<NodeStoreFixture, NodeStore> STORES = new ConcurrentHashMap<>();

    private Repository repo;
    private Session session;
    private Node testNode;

    private String unmappedNsPrefix;
    private String testPrefix;
    private String testNsUri;

    private static char[] SURROGATE_PAIR = Character.toChars(0x1f4a9);

    public ValidNamesTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws NamespaceException, RepositoryException {
        repo = createRepository(fixture);
        session = repo.login(getAdminCredentials());
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
        if (session != null) {
            session.removeItem(TEST_PATH);
            session.save();
            session.logout();
        }

        if (repo != null) {
            dispose(repo);
        }
    }

    @AfterClass
    public static void disposeStores() throws Exception {
        for (Map.Entry<NodeStoreFixture, NodeStore> e : STORES.entrySet()) {
            e.getKey().dispose(e.getValue());
        }
        STORES.clear();
    }

    @Test
    public void testSimple() {
        nameTest("foo");
    }

    // TODO: questionable exception
    @Test
    public void testDot() {
        unsupportedNameTest(".", ItemExistsException.class);
    }

    @Test
    public void testDotFoo() {
        nameTest(".foo");
    }

    // TODO: questionable exception
    @Test
    public void testDotDot() {
        unsupportedNameTest("..", ItemExistsException.class);
    }

    @Test
    public void testDotDotFoo() {
        nameTest("..foo");
    }

    @Test
    public void testTrailingDot() {
        nameTest("foo.");
    }

    // TODO: questionable exception
    @Test
    public void testLeadingBlank() {
        unsupportedNameTest(" foo", RepositoryException.class);
    }

    // TODO: questionable exception
    @Test
    public void testTrailingBlank() {
        unsupportedNameTest("foo ", RepositoryException.class);
    }

    // TODO: questionable exception
    @Test
    public void testEnclosedSlash() {
        unsupportedNameTest("foo/bar", PathNotFoundException.class);
    }

    @Test
    public void testEnclosedPipe() {
        unsupportedNameTest("foo|bar", RepositoryException.class);
    }

    @Test
    public void testEnclosedStar() {
        unsupportedNameTest("foo*bar", RepositoryException.class);
    }

    @Test
    public void testEnclosedOpenBracket() {
        unsupportedNameTest("foo[bar", RepositoryException.class);
    }

    @Test
    public void testEnclosedCloseBracket() {
        unsupportedNameTest("foo]bar", RepositoryException.class);
    }

    // TODO: questionable exception
    @Test
    public void testLeadingColon() {
        unsupportedNameTest(":foo", RepositoryException.class);
    }

    // TODO: questionable exception
    @Test
    public void testEnclosedUnmappedNsColon() {
        unsupportedNameTest(unmappedNsPrefix + ":bar", RepositoryException.class);
    }

    // TODO seems to be a bug
    @Test
    public void testEmptyNameInCurlys() throws RepositoryException {
        Node n = nameTest("{}foo");
        assertEquals("foo", n.getName());
    }

    @Test
    public void testSingleEnclosedOpenCurly() {
        nameTest("foo{bar");
    }

    @Test
    public void testSingleEnclosedCloseCurly() {
        nameTest("foo}bar");
    }

    @Test
    public void testValidLocalNameInCurlys() throws RepositoryException {
        Node n = nameTest("{foo}bar");
        assertEquals("{foo}bar", n.getName());
    }

    // TODO: questionable exception
    @Test
    public void testNonUriInCurlys() {
        unsupportedNameTest("{/}bar", RepositoryException.class);
    }

    @Test
    public void testValidNamespaceUriInCurlys() throws RepositoryException {
        Node n = nameTest("{" + testNsUri + "}foo");
        assertEquals(testPrefix + ":foo", n.getName());
    }

    // OAK-74 and OAK-9584
    @Test
    public void testRepNamespaceUri() throws RepositoryException {
        JackrabbitSession jrSession = (JackrabbitSession)session;
        UserManager userManager = jrSession.getUserManager();
        User user = userManager.createUser("test", "test");

        session.save();
        Node n = session.getNode(user.getPath());

        String repNamespaceUri = session.getNamespaceURI("rep");
        assertTrue(n.hasProperty("rep:authorizableId"));
        assertTrue(n.hasProperty("{"+repNamespaceUri+"}authorizableId"));
    }
 
    // TODO: questionable exception
    @Test
    public void testValidNamespaceUriInCurlysWrongPlace() {
        unsupportedNameTest("x{" + testNsUri + "}foo", RepositoryException.class);
    }

    // TODO: questionable exception
    @Test
    public void testValidNamespaceUriInCurlysNoLocalName() {
        unsupportedNameTest("{" + testNsUri + "}", RepositoryException.class);
    }

    // TODO this should actually pass
    @Test
    public void testQualifiedNameWithUnmappedNsUri() {
        String ns = "urn:uuid:" + UUID.randomUUID().toString();
        unsupportedNameTest("{" + ns + "}foo", RepositoryException.class);
    }

    @Test
    public void testEnclosedPercent() {
        nameTest("foo%bar");
    }

    @Test
    public void testEnclosedBlank() {
        nameTest("foo bar");
    }

    @Test
    public void testEnclosedTab() {
        unsupportedNameTest("foo\tbar", RepositoryException.class);
    }

    @Test
    public void testEnclosedLf() {
        unsupportedNameTest("foo\nbar", RepositoryException.class);
    }

    @Test
    public void testEnclosedCr() {
        unsupportedNameTest("foo\rbar", RepositoryException.class);
    }

    @Test
    public void testEnclosedNonXMLChars() {
        // see OAK-7270
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("rdb"));
        // see https://www.w3.org/TR/xml/#NT-Char
        for (int c = 0; c < 32; c++) {
            if (!(c == 0x9 || c == 0xa || c == 0xd)) {
                unsupportedNameTest("foo" + ((char) c) + "bar", RepositoryException.class);
            }
        }
    }

    @Test
    public void testEnclosedNonBreakingSpace() {
        nameTest("foo\u00a0bar");
    }

    @Test
    public void testEnclosedIdeographicSpace() {
        nameTest("foo\u3000bar");
    }

    @Test
    public void testUnpairedHighSurrogateEnd() {
        // see OAK-5506
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("segment"));
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("rdb"));
        nameTest("foo" + SURROGATE_PAIR[0]);
    }

    @Test
    public void testUnpairedLowSurrogateStart() {
        // see OAK-5506
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("segment"));
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("rdb"));
        nameTest(SURROGATE_PAIR[1] + "foo");
    }

    @Test
    public void testUnpairedSurrogateInside() {
        // see OAK-5506
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("segment"));
        // see OAK-7270
        org.junit.Assume.assumeFalse(super.fixture.toString().toLowerCase().contains("rdb"));
        nameTest("foo" + SURROGATE_PAIR[0] + "bar");
        nameTest("foo" + SURROGATE_PAIR[1] + "bar");
    }

    @Test
    public void testSurrogate() {
        nameTest("foo" + new String(SURROGATE_PAIR));
    }

    private Node nameTest(String nodeName) {
        try {
            Node n = testNode.addNode(nodeName);
            testNode.getSession().save();
            Node p = testNode.getSession().getNode(n.getPath());
            assertTrue("nodes should be the same", p.isSame(n));
            assertEquals("paths should be equal", p.getPath(), n.getPath());
            return p;
        } catch (RepositoryException ex) {
            fail(ex.getMessage());
            return null;
        }
    }

    private void unsupportedNameTest(String nodeName, Class<? extends RepositoryException> clazz) {
        try {
            testNode.addNode(nodeName);
            testNode.getSession().save();
            fail("should have failed with " + clazz);
        }
        catch (RepositoryException ex) {
            assertTrue("should have failed with " + clazz + ", but got " + ex.getClass(), clazz.isAssignableFrom(ex.getClass()));
        }
    }

    private Repository createRepository(NodeStoreFixture fixture) throws RepositoryException
            {
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