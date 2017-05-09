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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.SimpleCredentials;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.GetRequest;
import com.mashape.unirest.request.HttpRequestWithBody;
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
import org.junit.Test;

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
        HttpResponse<InputStream> response = get("/revisions/last").basicAuth("admin", "admin").asBinary();
        assertEquals(200, response.getStatus());
        assertNotNull(getRevision(json(response)));
    }

    @Test
    public void testReadLastRevisionWithoutAuthentication() throws Exception {
        assertEquals(401, get("/revisions/last").asString().getStatus());
    }

    @Test
    public void testReadLastRevisionTree() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testReadLastRevisionTreeWithoutAuthentication() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").asBinary();
        assertEquals(401, response.getStatus());
    }

    @Test
    public void testReadLastRevisionTreeRevision() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        assertFalse(response.getHeaders().getFirst("oak-revision").isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeWithNotExistingTree() throws Exception {
        assertEquals(404, get("/revisions/last/tree/node").basicAuth("admin", "admin").asString().getStatus());
    }

    @Test
    public void testReadLastRevisionTreeHasMoreChildren() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        assertFalse(getHasMoreChildren(json(response)));
    }

    @Test
    public void testReadLastRevisionTreeChildren() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree child = node.addChild("child");
        child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        assertTrue(hasNullChild(json(response), "child"));
    }

    @Test
    public void testReadLastRevisionTreeStringProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "a", Type.STRING);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("string", getPropertyType(body, "property"));
        assertEquals("a", getStringPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiStringProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("a", "b"), Type.STRINGS);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("strings", getPropertyType(body, "property"));
        assertEquals("a", getStringPropertyValue(body, "property", 0));
        assertEquals("b", getStringPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeBinaryProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(root.createBlob(asStream("a")), root.createBlob(asStream("b"))), Type.BINARIES);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("binaryIds", getPropertyType(body, "property"));
        assertFalse(getStringPropertyValue(body, "property", 0).isEmpty());
        assertFalse(getStringPropertyValue(body, "property", 1).isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeMultiBinaryProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", root.createBlob(asStream("a")), Type.BINARY);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("binaryId", getPropertyType(body, "property"));
        assertFalse(getStringPropertyValue(body, "property").isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeLongProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", 42L, Type.LONG);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("long", getPropertyType(body, "property"));
        assertEquals(42L, getLongPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiLongProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(4L, 2L), Type.LONGS);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("longs", getPropertyType(body, "property"));
        assertEquals(4L, getLongPropertyValue(body, "property", 0));
        assertEquals(2L, getLongPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeDoubleProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", 4.2, Type.DOUBLE);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("double", getPropertyType(body, "property"));
        assertEquals(4.2, getDoublePropertyValue(body, "property"), 1e-10);
    }

    @Test
    public void testReadLastRevisionTreeMultiDoubleProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(4.2, 2.4), Type.DOUBLES);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("doubles", getPropertyType(body, "property"));
        assertEquals(4.2, getDoublePropertyValue(body, "property", 0), 1e-10);
        assertEquals(2.4, getDoublePropertyValue(body, "property", 1), 1e-10);
    }

    @Test
    public void testReadLastRevisionTreeDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asString(calendar), Type.DATE);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("date", getPropertyType(body, "property"));
        assertEquals(calendar.getTimeInMillis(), getLongPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(asString(calendar), asString(calendar)), Type.DATES);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("dates", getPropertyType(body, "property"));
        assertEquals(calendar.getTimeInMillis(), getLongPropertyValue(body, "property", 0));
        assertEquals(calendar.getTimeInMillis(), getLongPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeBooleanProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", true, Type.BOOLEAN);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("boolean", getPropertyType(body, "property"));
        assertEquals(true, getBooleanPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiBooleanProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(true, false), Type.BOOLEANS);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("booleans", getPropertyType(body, "property"));
        assertEquals(true, getBooleanPropertyValue(body, "property", 0));
        assertEquals(false, getBooleanPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeNameProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.NAME);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("name", getPropertyType(body, "property"));
        assertEquals("value", getStringPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiNameProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.NAMES);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("names", getPropertyType(body, "property"));
        assertEquals("first", getStringPropertyValue(body, "property", 0));
        assertEquals("second", getStringPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreePathProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "/value", Type.PATH);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("path", getPropertyType(body, "property"));
        assertEquals("/value", getStringPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiPathProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("/first", "/second"), Type.PATHS);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("paths", getPropertyType(body, "property"));
        assertEquals("/first", getStringPropertyValue(body, "property", 0));
        assertEquals("/second", getStringPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.REFERENCE);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("reference", getPropertyType(body, "property"));
        assertEquals("value", getStringPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.REFERENCES);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("references", getPropertyType(body, "property"));
        assertEquals("first", getStringPropertyValue(body, "property", 0));
        assertEquals("second", getStringPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeWeakReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.WEAKREFERENCE);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("weakReference", getPropertyType(body, "property"));
        assertEquals("value", getStringPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiWeakReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.WEAKREFERENCES);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("weakReferences", getPropertyType(body, "property"));
        assertEquals("first", getStringPropertyValue(body, "property", 0));
        assertEquals("second", getStringPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeUriProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "http://acme.org", Type.URI);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("uri", getPropertyType(body, "property"));
        assertEquals("http://acme.org", getStringPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiUriProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("http://acme.org", "http://acme.com"), Type.URIS);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("uris", getPropertyType(body, "property"));
        assertEquals("http://acme.org", getStringPropertyValue(body, "property", 0));
        assertEquals("http://acme.com", getStringPropertyValue(body, "property", 1));
    }

    @Test
    public void testReadLastRevisionTreeDecimalProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", BigDecimal.ZERO, Type.DECIMAL);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("decimal", getPropertyType(body, "property"));
        assertEquals("0", getStringPropertyValue(body, "property"));
    }

    @Test
    public void testReadLastRevisionTreeMultiDecimalProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(BigDecimal.ZERO, BigDecimal.ONE), Type.DECIMALS);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        JsonNode body = json(response);
        assertEquals("decimals", getPropertyType(body, "property"));
        assertEquals("0", getStringPropertyValue(body, "property", 0));
        assertEquals("1", getStringPropertyValue(body, "property", 1));
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

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").queryString("depth", 1).asBinary();
        assertEquals(200, response.getStatus());
        JsonNode body = json(response);
        assertFalse(hasNullChild(body, "child"));
        assertTrue(hasNullGrandChild(body, "child", "grandChild"));
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

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").queryString("properties", "ba*").queryString("properties", "-baz").asBinary();
        assertEquals(200, response.getStatus());
        JsonNode body = json(response);
        assertFalse(hasProperty(body, "foo"));
        assertTrue(hasProperty(body, "bar"));
        assertFalse(hasProperty(body, "baz"));
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

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").queryString("children", "ba*").queryString("children", "-baz").asBinary();
        assertEquals(200, response.getStatus());
        JsonNode body = json(response);
        assertFalse(hasChild(body, "foo"));
        assertTrue(hasChild(body, "bar"));
        assertFalse(hasChild(body, "baz"));
    }

    @Test
    public void testReadTreeAtRevision() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        String revision = get("/revisions/last/tree/node").basicAuth("admin", "admin").asString().getHeaders().getFirst("oak-revision");

        HttpResponse<InputStream> response = get("/revisions/{revision}/tree/node").basicAuth("admin", "admin").routeParam("revision", revision).asBinary();
        assertEquals(200, response.getStatus());
        assertEquals(revision, response.getHeaders().getFirst("oak-revision"));
    }

    @Test
    public void testReadTreeAtRevisionWithInvalidRevision() throws Exception {
        assertEquals(410, get("/revisions/any/tree/node").basicAuth("admin", "admin").asString().getStatus());
    }

    @Test
    public void testReadBinary() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("test")), Type.BINARY);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        String binaryId = getStringPropertyValue(json(response), "binary");

        HttpResponse<InputStream> binaryResponse = get("/binaries/{binaryId}").basicAuth("admin", "admin").routeParam("binaryId", binaryId).asBinary();
        assertEquals(200, binaryResponse.getStatus());
        assertEquals("test", string(binaryResponse));
        assertEquals("4", binaryResponse.getHeaders().getFirst("content-length"));
    }

    @Test
    public void testReadBinaryWithoutAuthentication() throws Exception {
        HttpResponse<InputStream> response = get("/binaries/any").asBinary();
        assertEquals(401, response.getStatus());
    }

    @Test
    public void testReadBinaryWithInvalidId() throws Exception {
        HttpResponse<InputStream> response = get("/binaries/any").basicAuth("admin", "admin").asBinary();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testReadBinaryRangeOffsetOnly() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        String binaryId = getStringPropertyValue(json(response), "binary");

        // Offset = 0
        HttpResponse<InputStream> binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", string(binaryResponse));

        // Offset = 3
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes = 3")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*3\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("7", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("3456789", string(binaryResponse));

        // Offset = 9 (last)
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=9")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*9\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("1", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("9", string(binaryResponse));
    }

    @Test
    public void testReadBinaryRangeSuffix() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        String binaryId = getStringPropertyValue(json(response), "binary");

        // Last 10 bytes (full body)
        HttpResponse<InputStream> binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=-10")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", string(binaryResponse));

        // Last 3 bytes
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=-3")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*7\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("3", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("789", string(binaryResponse));

        // Last 1 byte
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=-1")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*9\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("1", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("9", string(binaryResponse));
    }

    @Test
    public void testReadBinarySingleRange() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        String binaryId = getStringPropertyValue(json(response), "binary");

        // Range 0-9 (full body)
        HttpResponse<InputStream> binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0-9")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", string(binaryResponse));

        // Range 0- (full body)
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0-")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*0\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("10", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("0123456789", string(binaryResponse));

        // Range 3-6
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=3- 6")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*3\\s*\\-\\s*6\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("4", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("3456", string(binaryResponse));

        // Range 9-9
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes= 9 - 9")
                .asBinary();
        assertEquals(206, binaryResponse.getStatus());
        assertTrue(binaryResponse.getHeaders().containsKey("content-range"));
        assertTrue(binaryResponse.getHeaders().getFirst("content-range").matches("^\\s*9\\s*\\-\\s*9\\s*/\\s*(10|\\*)\\s*$"));
        assertEquals("1", binaryResponse.getHeaders().getFirst("content-length"));
        assertEquals("9", string(binaryResponse));
    }

    @Test
    public void testReadBinaryMultipleRanges() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("binary", root.createBlob(asStream("0123456789")), Type.BINARY);

        root.commit();

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        String binaryId = getStringPropertyValue(json(response), "binary");

        HttpResponse<InputStream> binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0,-1 , 2-2, 3-6")
                .asBinary();
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

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        String binaryId = getStringPropertyValue(json(response), "binary");

        // Unknown range unit = elephant
        HttpResponse<InputStream> binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "elephant=0-9")
                .asBinary();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Invalid range header
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "this is not correct")
                .asBinary();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Missing range unit
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "0")
                .asBinary();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Range limit greater than file size
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0-1000")
                .asBinary();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // Range start greater than end
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=9-8")
                .asBinary();
        assertNotEquals(206, binaryResponse.getStatus());
        assertNotEquals(500, binaryResponse.getStatus());

        // One bad range will "break" all ranges
        binaryResponse = get("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .header("Range", "bytes=0, -1, 10000")
                .asBinary();
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

        HttpResponse<InputStream> response = get("/revisions/last/tree/node").basicAuth("admin", "admin").asBinary();
        String binaryId = getStringPropertyValue(json(response), "binary");

        // Offset = 0
        HttpResponse<InputStream> binaryResponse = head("/binaries/{binaryId}")
                .basicAuth("admin", "admin")
                .routeParam("binaryId", binaryId)
                .asBinary();
        assertEquals(200, binaryResponse.getStatus());
        assertEquals("bytes", binaryResponse.getHeaders().getFirst("accept-ranges"));
    }

    @Test
    public void testExistsBinaryWithInvalidId() throws Exception {
        HttpResponse<InputStream> response = head("/binaries/any").basicAuth("admin", "admin").asBinary();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testExistsBinaryWithoutAuthentication() throws Exception {
        HttpResponse<InputStream> response = head("/binaries/any").asBinary();
        assertEquals(401, response.getStatus());
    }

    @Test
    public void testCreateBinary() throws Exception {
        HttpResponse<InputStream> response = post("/binaries").basicAuth("admin", "admin").body("body").asBinary();
        assertEquals(201, response.getStatus());

        String binaryId = getBinaryId(json(response));
        assertFalse(binaryId.isEmpty());

        HttpResponse<InputStream> binaryResponse = get("/binaries/{binaryId}").basicAuth("admin", "admin").routeParam("binaryId", binaryId).asBinary();
        assertEquals(200, binaryResponse.getStatus());
        assertEquals("body", string(binaryResponse));
    }

    @Test
    public void testCreateBinaryWithoutAuthentication() throws Exception {
        assertEquals(401, post("/binaries").body("body").asString().getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeBinaryProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeBinaryProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeMultiBinaryProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiBinaryProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeBooleanProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeBooleanProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeMultiBooleanProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiBooleanProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeDateProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeDateProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNodeMultiDateProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiDateProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddDecimalProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeDecimalProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiDecimalProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiDecimalProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddDoubleProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeDoubleProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiDoubleProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiDoubleProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddLongProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeLongProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiLongProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiLongProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddNameProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeNameProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiNameProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiNameProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddPathProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodePathProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiPathProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiPathProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddReferenceProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeReferenceProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiReferenceProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiReferenceProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddStringProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeStringProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiStringProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiStringProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddUriProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeUriProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiUriProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiUriProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddWeakReferenceProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeWeakReferenceProperty.json")).asBinary();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testPatchLastRevisionAddMultiWeakReferenceProperty() throws Exception {
        HttpResponse<InputStream> response = patch("/revisions/last/tree").basicAuth("admin", "admin").body(load("addNodeMultiWeakReferenceProperty.json")).asBinary();
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

        HttpResponse<InputStream> response = get("/revisions/last/tree")
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asBinary();

        assertEquals(200, response.getStatus());
        JsonNode body = json(response);
        assertTrue(hasTotal(body));
        List<String> columns = getStringArray(body, "columns");
        assertTrue(columns.contains("name"));
        assertTrue(columns.contains("jcr:path"));
        List<String> selectors = getStringArray(body, "selectors");
        assertTrue(selectors.contains("node"));
    }

    @Test
    public void testSearchLastRevisionWithoutAuthentication() throws Exception {
        HttpResponse<InputStream> response = get("/revisions/last/tree")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asBinary();

        assertEquals(401, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutQuery() throws Exception {
        HttpResponse<InputStream> response = get("/revisions/last/tree")
                .basicAuth("admin", "admin")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asBinary();

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutLanguage() throws Exception {
        HttpResponse<InputStream> response = get("/revisions/last/tree")
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("offset", 0)
                .queryString("limit", 10)
                .asBinary();

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutOffset() throws Exception {
        HttpResponse<InputStream> response = get("/revisions/last/tree")
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("limit", 10)
                .asBinary();

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testSearchLastRevisionWithoutLimit() throws Exception {
        HttpResponse<InputStream> response = get("/revisions/last/tree")
                .basicAuth("admin", "admin")
                .queryString("query", "select name from nt:unstructured as node where jcr:path like '/test/%'")
                .queryString("language", "sql")
                .queryString("offset", 0)
                .asBinary();

        assertEquals(400, response.getStatus());
    }

    private String resource(String path) {
        return "http://localhost:" + port + path;
    }

    private GetRequest get(String path) {
        return Unirest.get(resource(path));
    }

    private GetRequest head(String path) {
        return Unirest.head(resource(path));
    }

    private HttpRequestWithBody patch(String path) {
        return Unirest.patch(resource(path));
    }

    private HttpRequestWithBody post(String path) {
        return Unirest.post(resource(path));
    }

    private JsonNode json(HttpResponse<?> response) throws Exception {
        return new ObjectMapper().readTree(response.getRawBody());
    }

    private String string(HttpResponse<?> response) throws Exception {
        return new String(ByteStreams.toByteArray(response.getRawBody()), Charsets.UTF_8);
    }

    private String getRevision(JsonNode response) throws Exception {
        return response.get("revision").asText();
    }

    private boolean getHasMoreChildren(JsonNode response) throws Exception {
        return response.get("hasMoreChildren").asBoolean();
    }

    private boolean hasChild(JsonNode response, String name) throws Exception {
        return response.get("children").has(name);
    }

    private boolean hasNullChild(JsonNode response, String name) throws Exception {
        return response.get("children").get(name).isNull();
    }

    private boolean hasNullGrandChild(JsonNode response, String child, String grandChild) throws Exception {
        return response.get("children").get(child).get("children").get(grandChild).isNull();
    }

    private boolean hasProperty(JsonNode response, String name) throws Exception {
        return response.get("properties").has(name);
    }

    private String getPropertyType(JsonNode response, String name) throws Exception {
        return response.get("properties").get(name).get("type").asText();
    }

    private JsonNode getPropertyValue(JsonNode response, String name) throws Exception {
        return response.get("properties").get(name).get("value");
    }

    private JsonNode getPropertyValue(JsonNode response, String name, int index) throws Exception {
        return response.get("properties").get(name).get("value").get(index);
    }

    private String getStringPropertyValue(JsonNode response, String name) throws Exception {
        return getPropertyValue(response, name).asText();
    }

    private String getStringPropertyValue(JsonNode response, String name, int index) throws Exception {
        return getPropertyValue(response, name, index).asText();
    }

    private long getLongPropertyValue(JsonNode response, String name) throws Exception {
        return getPropertyValue(response, name).asLong();
    }

    private long getLongPropertyValue(JsonNode response, String name, int index) throws Exception {
        return getPropertyValue(response, name, index).asLong();
    }

    private double getDoublePropertyValue(JsonNode response, String name) throws Exception {
        return getPropertyValue(response, name).asDouble();
    }

    private double getDoublePropertyValue(JsonNode response, String name, int index) throws Exception {
        return getPropertyValue(response, name, index).asDouble();
    }

    private boolean getBooleanPropertyValue(JsonNode response, String name) throws Exception {
        return getPropertyValue(response, name).asBoolean();
    }

    private boolean getBooleanPropertyValue(JsonNode response, String name, int index) throws Exception {
        return getPropertyValue(response, name, index).asBoolean();
    }

    private String getBinaryId(JsonNode response) throws Exception {
        return response.get("binaryId").asText();
    }

    private boolean hasTotal(JsonNode response) throws Exception {
        return response.has("total");
    }

    private List<String> getStringArray(JsonNode response, String name) throws Exception {
        List<String> result = new ArrayList<>();
        JsonNode field = response.get(name);
        for (int i = 0; i < field.size(); i++) {
            result.add(field.get(i).asText());
        }
        return result;
    }

}
