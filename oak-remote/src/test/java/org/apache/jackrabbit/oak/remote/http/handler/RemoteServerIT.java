/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.http.handler;

import static com.mashape.unirest.http.Unirest.get;
import static com.mashape.unirest.http.Unirest.head;
import static com.mashape.unirest.http.Unirest.patch;
import static com.mashape.unirest.http.Unirest.post;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.SimpleCredentials;

import com.google.common.base.Charsets;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.remote.RemoteRepository;
import org.apache.jackrabbit.oak.remote.content.ContentRemoteRepository;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("OAK-5171")
public class RemoteServerIT extends OakBaseTest {

    private ContentRepository contentRepository;

    private ContentSession contentSession;

    private RemoteServer remoteServer;

    private int port;

    public RemoteServerIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    private InputStream asStream(String s) {
        return new ByteArrayInputStream(s.getBytes());
    }

    private String asString(Calendar calendar) {
        return ISO8601.format(calendar);
    }

    private RemoteServer getRemoteServer(RemoteRepository repository, String host, int port) {
        return new RemoteServer(repository, host, port);
    }

    private RemoteRepository getRemoteRepository(ContentRepository repository) {
        return new ContentRemoteRepository(repository);
    }

    private ContentRepository getContentRepository() {
        return new Jcr(new Oak(store)).createContentRepository();
    }

    private ContentSession getContentSession(ContentRepository repository) throws Exception {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
    }

    private int getRandomPort() throws Exception {
        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(0);
            return serverSocket.getLocalPort();
        } finally {
            if (serverSocket != null) {
                serverSocket.close();
            }
        }
    }

    private String resource(String path) {
        return "http://localhost:" + port + path;
    }

    private String load(String name) throws Exception {
        InputStream is = null;
        try {
            is = getClass().getResource(name).openStream();
            return IOUtils.toString(is, Charsets.UTF_8);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    @Before
    public void setUp() throws Exception {
        port = getRandomPort();
        contentRepository = getContentRepository();
        contentSession = getContentSession(contentRepository);
        RemoteRepository remoteRepository = getRemoteRepository(contentRepository);
        remoteServer = getRemoteServer(remoteRepository, "localhost", port);
        remoteServer.start();
    }

    @After
    public void tearDown() throws Exception {
        remoteServer.stop();
        contentSession.close();
        if (contentRepository instanceof Closeable) {
            IOUtils.closeQuietly((Closeable) contentRepository);
        }
    }

    @Test
    public void testReadLastRevision() throws Exception {
        HttpResponse<JsonNode> response = get(resource("/revisions/last")).basicAuth("admin", "admin").asJson();
        assertEquals(200, response.getStatus());
        assertNotNull(getRevision(response));
    }

    @Test
    public void testReadLastRevisionWithoutAuthentication() throws Exception {
        assertEquals(401, get(resource("/revisions/last")).asJson().getStatus());
    }

    @Test
    public void testReadLastRevisionTree() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testReadLastRevisionTreeWithoutAuthentication() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).asJson();
        assertEquals(401, response.getStatus());
    }

    @Test
    public void testReadLastRevisionTreeRevision() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertFalse(response.getHeaders().getFirst("oak-revision").isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeWithNotExistingTree() throws Exception {
        assertEquals(404, get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getStatus());
    }

    @Test
    public void testReadLastRevisionTreeHasMoreChildren() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertFalse(getHasMoreChildren(response));
    }

    @Test
    public void testReadLastRevisionTreeChildren() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree child = node.addChild("child");
        child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));
    }

    @Test
    public void testReadLastRevisionTreeStringProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "a", Type.STRING);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("string", getPropertyType(response, "property"));
        assertEquals("a", getStringPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiStringProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("a", "b"), Type.STRINGS);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("strings", getPropertyType(response, "property"));
        assertEquals("a", getStringPropertyValue(response, "property", 0));
        assertEquals("b", getStringPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeBinaryProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(root.createBlob(asStream("a")), root.createBlob(asStream("b"))), Type.BINARIES);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("binaryIds", getPropertyType(response, "property"));
        assertFalse(getStringPropertyValue(response, "property", 0).isEmpty());
        assertFalse(getStringPropertyValue(response, "property", 1).isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeMultiBinaryProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", root.createBlob(asStream("a")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("binaryId", getPropertyType(response, "property"));
        assertFalse(getStringPropertyValue(response, "property").isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeLongProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", 42L, Type.LONG);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("long", getPropertyType(response, "property"));
        assertEquals(42L, getLongPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiLongProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(4L, 2L), Type.LONGS);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("longs", getPropertyType(response, "property"));
        assertEquals(4L, getLongPropertyValue(response, "property", 0));
        assertEquals(2L, getLongPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeDoubleProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", 4.2, Type.DOUBLE);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("double", getPropertyType(response, "property"));
        assertEquals(4.2, getDoublePropertyValue(response, "property"), 1e-10);
    }

    @Test
    public void testReadLastRevisionTreeMultiDoubleProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(4.2, 2.4), Type.DOUBLES);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("doubles", getPropertyType(response, "property"));
        assertEquals(4.2, getDoublePropertyValue(response, "property", 0), 1e-10);
        assertEquals(2.4, getDoublePropertyValue(response, "property", 1), 1e-10);
    }

    @Test
    public void testReadLastRevisionTreeDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asString(calendar), Type.DATE);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("date", getPropertyType(response, "property"));
        assertEquals(calendar.getTimeInMillis(), getLongPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(asString(calendar), asString(calendar)), Type.DATES);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("dates", getPropertyType(response, "property"));
        assertEquals(calendar.getTimeInMillis(), getLongPropertyValue(response, "property", 0));
        assertEquals(calendar.getTimeInMillis(), getLongPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeBooleanProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", true, Type.BOOLEAN);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("boolean", getPropertyType(response, "property"));
        assertEquals(true, getBooleanPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiBooleanProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(true, false), Type.BOOLEANS);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("booleans", getPropertyType(response, "property"));
        assertEquals(true, getBooleanPropertyValue(response, "property", 0));
        assertEquals(false, getBooleanPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeNameProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("name", getPropertyType(response, "property"));
        assertEquals("value", getStringPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiNameProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.NAMES);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("names", getPropertyType(response, "property"));
        assertEquals("first", getStringPropertyValue(response, "property", 0));
        assertEquals("second", getStringPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreePathProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "/value", Type.PATH);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("path", getPropertyType(response, "property"));
        assertEquals("/value", getStringPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiPathProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("/first", "/second"), Type.PATHS);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("paths", getPropertyType(response, "property"));
        assertEquals("/first", getStringPropertyValue(response, "property", 0));
        assertEquals("/second", getStringPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.REFERENCE);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("reference", getPropertyType(response, "property"));
        assertEquals("value", getStringPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.REFERENCES);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("references", getPropertyType(response, "property"));
        assertEquals("first", getStringPropertyValue(response, "property", 0));
        assertEquals("second", getStringPropertyValue(response, "property", 0));
    }

    @Test
    public void testReadLastRevisionTreeWeakReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.WEAKREFERENCE);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("weakReference", getPropertyType(response, "property"));
        assertEquals("value", getStringPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiWeakReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.WEAKREFERENCES);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("weakReferences", getPropertyType(response, "property"));
        assertEquals("first", getStringPropertyValue(response, "property", 0));
        assertEquals("second", getStringPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeUriProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "http://acme.org", Type.URI);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("uri", getPropertyType(response, "property"));
        assertEquals("http://acme.org", getStringPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiUriProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("http://acme.org", "http://acme.com"), Type.URIS);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("uris", getPropertyType(response, "property"));
        assertEquals("http://acme.org", getStringPropertyValue(response, "property", 0));
        assertEquals("http://acme.com", getStringPropertyValue(response, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeDecimalProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", BigDecimal.ZERO, Type.DECIMAL);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("decimal", getPropertyType(response, "property"));
        assertEquals("0", getStringPropertyValue(response, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiDecimalProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(BigDecimal.ZERO, BigDecimal.ONE), Type.DECIMALS);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        assertTrue(hasNullChild(response, "child"));

        assertEquals("decimals", getPropertyType(response, "property"));
        assertEquals("0", getStringPropertyValue(response, "property", 0));
        assertEquals("1", getStringPropertyValue(response, "property", 0));
    }

    @Test
    public void testReadLastRevisionTreeWithDepth() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree child = node.addChild("child");
        child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree grandChild = child.addChild("grandChild");
        grandChild.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").queryString("depth", 1).asJson();
        assertEquals(200, response.getStatus());

        assertFalse(hasNullChild(response, "child"));
        assertTrue(hasNullGrandChild(response, "child", "grandChild"));
    }

    @Test
    public void testReadLastRevisionTreeWithWithPropertyFilters() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("foo", "foo", Type.STRING);
        node.setProperty("bar", "bar", Type.STRING);
        node.setProperty("baz", "baz", Type.STRING);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").queryString("properties", "ba*").queryString("properties", "-baz").asJson();
        assertEquals(200, response.getStatus());

        assertFalse(hasProperty(response, "foo"));
        assertTrue(hasProperty(response, "bar"));
        assertFalse(hasProperty(response, "baz"));
    }

    @Test
    public void testReadLastRevisionTreeWithWithNodeFilters() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree foo = node.addChild("foo");
        foo.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree bar = node.addChild("bar");
        bar.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree baz = node.addChild("baz");
        baz.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").queryString("children", "ba*").queryString("children", "-baz").asJson();
        assertEquals(200, response.getStatus());

        assertFalse(hasChild(response, "foo"));
        assertTrue(hasChild(response, "bar"));
        assertFalse(hasChild(response, "baz"));
    }

    @Test
    public void testReadTreeAtRevision() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        String revision = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getHeaders().getFirst("oak-revision");

        HttpResponse<JsonNode> response = get(resource("/revisions/{revision}/tree/node")).basicAuth("admin", "admin").routeParam("revision", revision).asJson();
        assertEquals(200, response.getStatus());
        assertEquals(revision, response.getHeaders().getFirst("oak-revision"));
    }

    @Test
    public void testReadTreeAtRevisionWithInvalidRevision() throws Exception {
        assertEquals(410, get(resource("/revisions/any/tree/node")).basicAuth("admin", "admin").asJson().getStatus());
    }

    @Test
    public void testReadBinary() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("test")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        String binaryId = getStringPropertyValue(response, "binary");

        HttpResponse<String> binaryResponse = get(resource("/binaries/{binaryId}")).basicAuth("admin", "admin").routeParam("binaryId", binaryId).asString();
        assertEquals(200, binaryResponse.getStatus());
        assertEquals("test", binaryResponse.getBody());
        assertEquals("4", binaryResponse.getHeaders().getFirst("content-length"));
    }

    @Test
    public void testReadBinaryWithoutAuthentication() throws Exception {
        HttpResponse<String> response = get(resource("/binaries/any")).asString();
        assertEquals(401, response.getStatus());
    }

    @Test
    public void testReadBinaryWithInvalidId() throws Exception {
        HttpResponse<String> response = get(resource("/binaries/any")).basicAuth("admin", "admin").asString();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testReadBinaryRangeOffsetOnly() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        String binaryId = getStringPropertyValue(response, "binary");

        // Offset = 0
        HttpResponse<String> binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", binaryResponse.getBody());

        // Offset = 3
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes = 3")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*3\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("7", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("3456789", binaryResponse.getBody());

        // Offset = 9 (last)
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=9")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*9\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("1", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("9", binaryResponse.getBody());
    }

    @Test
    public void testReadBinaryRangeSuffix() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        String binaryId = getStringPropertyValue(response, "binary");

        // Last 10 bytes (full body)
        HttpResponse<String> binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=-10")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", binaryResponse.getBody());

        // Last 3 bytes
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=-3")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*7\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("3", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("789", binaryResponse.getBody());

        // Last 1 byte
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=-1")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*9\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("1", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("9", binaryResponse.getBody());
    }

    @Test
    public void testReadBinarySingleRange() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        String binaryId = getStringPropertyValue(response, "binary");

        // Range 0-9 (full body)
        HttpResponse<String> binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0-9")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", binaryResponse.getBody());

        // Range 0- (full body)
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0-")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", binaryResponse.getBody());

        // Range 3-6
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=3- 6")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*3\\s*\\-\\s*6\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("4", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("3456", binaryResponse.getBody());

        // Range 9-9
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes= 9 - 9")
                .asString();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*9\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("1", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("9", binaryResponse.getBody());
    }

    @Test
    public void testReadBinaryMultipleRanges() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        String binaryId = getStringPropertyValue(response, "binary");

        HttpResponse<String> binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0,-1 , 2-2, 3-6")
                .asString();
        assertEquals(206, binaryResponse.getStatus());

        String contentType = binaryResponse.getHeaders().getFirst("content-type");
        assertNotNull(contentType);

        Pattern multipartDelimiterPattern = Pattern.compile("^\\s*multipart/byteranges;\\s*boundary=(.+)");
        Matcher matcher = multipartDelimiterPattern.matcher(contentType);
        assertTrue(matcher.matches());

        //TODO: Validate response body
    }

    @Test
    public void testReadBinaryInvalidRanges() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        String binaryId = getStringPropertyValue(response, "binary");

        // Unknown range unit = elephant
        HttpResponse<String> binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "elephant=0-9")
                .asString();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Invalid range header
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "this is not correct")
                .asString();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Missing range unit
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "0")
                .asString();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Range limit greater than file size
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0-1000")
                .asString();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Range start greater than end
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=9-8")
                .asString();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // One bad range will "break" all ranges
        binaryResponse = get(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0, -1, 10000")
                .asString();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());
    }

    @Test
    public void testExistsBinary() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson();
        String binaryId = getStringPropertyValue(response, "binary");

        // Offset = 0
        HttpResponse<String> binaryResponse = head(resource("/binaries/{binaryId}"))
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .asString();
        assertEquals(200, binaryResponse.getStatus());
        assertEquals("bytes", binaryResponse.getHeaders().getFirst("accept-ranges"));
    }

    @Test
    public void testExistsBinaryWithInvalidId() throws Exception {
        HttpResponse<String> response = head(resource("/binaries/any")).basicAuth("admin", "admin").asString();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testExistsBinaryWithoutAuthentication() throws Exception {
        HttpResponse<String> response = head(resource("/binaries/any")).asString();
        assertEquals(401, response.getStatus());
    }

    @Test
    public void testCreateBinary() throws Exception {
        HttpResponse<JsonNode> response = post(resource("/binaries")).basicAuth("admin", "admin").body("body").asJson();
        assertEquals(201, response.getStatus());

        String binaryId = getBinaryId(response);
        assertFalse(binaryId.isEmpty());

        HttpResponse<String> binaryResponse = get(resource("/binaries/{binaryId}")).basicAuth("admin", "admin").routeParam("binaryId", binaryId).asString();
        assertEquals(200, binaryResponse.getStatus());
        assertEquals("body", binaryResponse.getBody());
    }

    @Test
    public void testCreateBinaryWithoutAuthentication() throws Exception {
        assertEquals(401, post(resource("/binaries")).body("body").asJson().getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeBinaryProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeBinaryProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeMultiBinaryProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiBinaryProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeBooleanProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeBooleanProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeMultiBooleanProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiBooleanProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeDateProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeDateProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeMultiDateProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiDateProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddDecimalProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeDecimalProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiDecimalProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiDecimalProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddDoubleProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeDoubleProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiDoubleProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiDoubleProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddLongProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeLongProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiLongProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiLongProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNameProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeNameProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiNameProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiNameProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddPathProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodePathProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiPathProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiPathProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddReferenceProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeReferenceProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiReferenceProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiReferenceProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddStringProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeStringProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiStringProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiStringProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddUriProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeUriProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiUriProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiUriProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddWeakReferenceProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeWeakReferenceProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiWeakReferenceProperty() throws Exception {
        HttpResponse<JsonNode> response = patch(resource("/revisions/last/tree")).basicAuth("admin", "admin").body(load("addNodeMultiWeakReferenceProperty.json")).asJson();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testSearchLastRevision() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree n1 = test.addChild("one");
        n1.setProperty("name", "one", Type.STRING);
        n1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree n2 = test.addChild("two");
        n2.setProperty("name", "two", Type.STRING);
        n2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree"))
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asJson();

        assertEquals(200, response.getStatus());

        assertTrue(getTotal(response) > 0);

        List<String> columns = getStringArray(response, "columns");
        assertTrue(columns.contains("name"));
        assertTrue(columns.contains("jcr:path"));

        List<String> selectors = getStringArray(response, "selectors");
        assertTrue(selectors.contains("node"));
    }

    @Test
    public void testSearchLastRevisionWithoutAuthentication() throws Exception {
        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree"))
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asJson();

        assertEquals(401, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutQuery() throws Exception {
        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree"))
                .basicAuth("admin", "admin")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asJson();

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutLanguage() throws Exception {
        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree"))
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asJson();

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutOffset() throws Exception {
        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree"))
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("limit", 10)
                .asJson();

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutLimit() throws Exception {
        HttpResponse<JsonNode> response = get(resource("/revisions/last/tree"))
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .asJson();

        assertEquals(400, response.getStatus());
    }

    private String getRevision(HttpResponse<JsonNode> response) {
        // return response.revision
        throw new UnsupportedOperationException("not implemented");
    }

    private boolean getHasMoreChildren(HttpResponse<JsonNode> response) {
        // return response.hasMoreChildren
        throw new UnsupportedOperationException("not implemented");
    }

    private boolean hasChild(HttpResponse<JsonNode> response, String name) {
        // return name in response.children
        throw new UnsupportedOperationException("not implemented");
    }

    private boolean hasNullChild(HttpResponse<JsonNode> response, String name) {
        // return response.children[name] === null
        throw new UnsupportedOperationException("not implemented");
    }

    private boolean hasNullGrandChild(HttpResponse<JsonNode> response, String child, String grandChild) {
        // return response.children[child].children[grandChild] == null
        throw new UnsupportedOperationException("not implemented");
    }

    private boolean hasProperty(HttpResponse<JsonNode> response, String name) {
        // return name in response.properties
        throw new UnsupportedOperationException("not implemented");
    }

    private String getPropertyType(HttpResponse<JsonNode> response, String name) {
        // return response.properties[name].type
        throw new UnsupportedOperationException("not implemented");
    }

    private String getStringPropertyValue(HttpResponse<JsonNode> response, String name) {
        // return response.properties[name].value
        throw new UnsupportedOperationException("not implemented");
    }

    private String getStringPropertyValue(HttpResponse<JsonNode> response, String name, int index) {
        // return response.properties[name].value[index]
        throw new UnsupportedOperationException("not implemented");
    }

    private long getLongPropertyValue(HttpResponse<JsonNode> response, String name) {
        // return response.properties[name].type
        throw new UnsupportedOperationException("not implemented");
    }

    private long getLongPropertyValue(HttpResponse<JsonNode> response, String name, int index) {
        // return response.properties[name].value[index]
        throw new UnsupportedOperationException("not implemented");
    }

    private double getDoublePropertyValue(HttpResponse<JsonNode> response, String name) {
        // return response.properties[name].type
        throw new UnsupportedOperationException("not implemented");
    }

    private double getDoublePropertyValue(HttpResponse<JsonNode> response, String name, int index) {
        // return response.properties[name].value[index]
        throw new UnsupportedOperationException("not implemented");
    }

    private boolean getBooleanPropertyValue(HttpResponse<JsonNode> response, String name) {
        // return response.properties[name].type
        throw new UnsupportedOperationException("not implemented");
    }

    private boolean getBooleanPropertyValue(HttpResponse<JsonNode> response, String name, int index) {
        // return response.properties[name].value[index]
        throw new UnsupportedOperationException("not implemented");
    }

    private String getBinaryId(HttpResponse<JsonNode> response) {
        // return response.binaryId
        throw new UnsupportedOperationException("not implemented");
    }

    private long getTotal(HttpResponse<JsonNode> response) {
        // return response.total
        throw new UnsupportedOperationException("not implemented");
    }

    private List<String> getStringArray(HttpResponse<JsonNode> response, String name) {
        // return response[name]
        throw new UnsupportedOperationException("not implemented");
    }


}
