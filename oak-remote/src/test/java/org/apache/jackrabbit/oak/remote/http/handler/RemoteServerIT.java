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
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.SimpleCredentials;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;
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

public class RemoteServerIT extends OakBaseTest {

    private ContentRepository contentRepository;

    private ContentSession contentSession;

    private RemoteRepository remoteRepository;

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
        remoteRepository = getRemoteRepository(contentRepository);
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

        JSONObject payload = response.getBody().getObject();
        assertNotNull(payload.getString("revision"));
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

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertFalse(body.getBoolean("hasMoreChildren"));
    }

    @Test
    public void testReadLastRevisionTreeChildren() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree child = node.addChild("child");
        child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));
    }

    @Test
    public void testReadLastRevisionTreeStringProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "a", Type.STRING);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("string", property.getString("type"));
        assertEquals("a", property.getString("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiStringProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("a", "b"), Type.STRINGS);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("strings", property.getString("type"));
        assertEquals("a", property.getJSONArray("value").getString(0));
        assertEquals("b", property.getJSONArray("value").getString(1));
    }

    @Test
    public void testReadLastRevisionTreeBinaryProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(root.createBlob(asStream("a")), root.createBlob(asStream("b"))), Type.BINARIES);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("binaryIds", property.getString("type"));
        assertFalse(property.getJSONArray("value").getString(0).isEmpty());
        assertFalse(property.getJSONArray("value").getString(1).isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeMultiBinaryProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", root.createBlob(asStream("a")), Type.BINARY);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("binaryId", property.getString("type"));
        assertFalse(property.getString("value").isEmpty());
    }

    @Test
    public void testReadLastRevisionTreeLongProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", 42L, Type.LONG);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("long", property.getString("type"));
        assertEquals(42L, property.getLong("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiLongProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(4L, 2L), Type.LONGS);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("longs", property.getString("type"));
        assertEquals(4L, property.getJSONArray("value").getLong(0));
        assertEquals(2L, property.getJSONArray("value").getLong(1));
    }

    @Test
    public void testReadLastRevisionTreeDoubleProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", 4.2, Type.DOUBLE);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("double", property.getString("type"));
        assertEquals(4.2, property.getDouble("value"), 1e-10);
    }

    @Test
    public void testReadLastRevisionTreeMultiDoubleProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(4.2, 2.4), Type.DOUBLES);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("doubles", property.getString("type"));
        assertEquals(4.2, property.getJSONArray("value").getDouble(0), 1e-10);
        assertEquals(2.4, property.getJSONArray("value").getDouble(1), 1e-10);
    }

    @Test
    public void testReadLastRevisionTreeDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asString(calendar), Type.DATE);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("date", property.getString("type"));
        assertEquals(calendar.getTimeInMillis(), property.getLong("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(asString(calendar), asString(calendar)), Type.DATES);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("dates", property.getString("type"));
        assertEquals(calendar.getTimeInMillis(), property.getJSONArray("value").getLong(0));
        assertEquals(calendar.getTimeInMillis(), property.getJSONArray("value").getLong(1));
    }

    @Test
    public void testReadLastRevisionTreeBooleanProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", true, Type.BOOLEAN);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("boolean", property.getString("type"));
        assertEquals(true, property.getBoolean("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiBooleanProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(true, false), Type.BOOLEANS);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("booleans", property.getString("type"));
        assertEquals(true, property.getJSONArray("value").getBoolean(0));
        assertEquals(false, property.getJSONArray("value").getBoolean(1));
    }

    @Test
    public void testReadLastRevisionTreeNameProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.NAME);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("name", property.getString("type"));
        assertEquals("value", property.getString("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiNameProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.NAMES);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("names", property.getString("type"));
        assertEquals("first", property.getJSONArray("value").getString(0));
        assertEquals("second", property.getJSONArray("value").getString(1));
    }

    @Test
    public void testReadLastRevisionTreePathProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "/value", Type.PATH);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("path", property.getString("type"));
        assertEquals("/value", property.getString("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiPathProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("/first", "/second"), Type.PATHS);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("paths", property.getString("type"));
        assertEquals("/first", property.getJSONArray("value").getString(0));
        assertEquals("/second", property.getJSONArray("value").getString(1));
    }

    @Test
    public void testReadLastRevisionTreeReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.REFERENCE);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("reference", property.getString("type"));
        assertEquals("value", property.getString("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.REFERENCES);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("references", property.getString("type"));
        assertEquals("first", property.getJSONArray("value").getString(0));
        assertEquals("second", property.getJSONArray("value").getString(1));
    }

    @Test
    public void testReadLastRevisionTreeWeakReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "value", Type.WEAKREFERENCE);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("weakReference", property.getString("type"));
        assertEquals("value", property.getString("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiWeakReferenceProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("first", "second"), Type.WEAKREFERENCES);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("weakReferences", property.getString("type"));
        assertEquals("first", property.getJSONArray("value").getString(0));
        assertEquals("second", property.getJSONArray("value").getString(1));
    }

    @Test
    public void testReadLastRevisionTreeUriProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", "http://acme.org", Type.URI);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("uri", property.getString("type"));
        assertEquals("http://acme.org", property.getString("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiUriProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList("http://acme.org", "http://acme.com"), Type.URIS);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("uris", property.getString("type"));
        assertEquals("http://acme.org", property.getJSONArray("value").getString(0));
        assertEquals("http://acme.com", property.getJSONArray("value").getString(1));
    }

    @Test
    public void testReadLastRevisionTreeDecimalProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", BigDecimal.ZERO, Type.DECIMAL);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("decimal", property.getString("type"));
        assertEquals("0", property.getString("value"));
    }

    @Test
    public void testReadLastRevisionTreeMultiDecimalProperty() throws Exception {
        Root root = contentSession.getLatestRoot();

        Tree node = root.getTree("/").addChild("node");
        node.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        node.setProperty("property", asList(BigDecimal.ZERO, BigDecimal.ONE), Type.DECIMALS);

        root.commit();

        JSONObject body = get(resource("/revisions/last/tree/node")).basicAuth("admin", "admin").asJson().getBody().getObject();
        assertTrue(body.getJSONObject("children").isNull("child"));

        JSONObject property = body.getJSONObject("properties").getJSONObject("property");
        assertEquals("decimals", property.getString("type"));
        assertEquals("0", property.getJSONArray("value").getString(0));
        assertEquals("1", property.getJSONArray("value").getString(1));
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

        JSONObject body = response.getBody().getObject();
        assertFalse(body.getJSONObject("children").isNull("child"));
        assertTrue(body.getJSONObject("children").getJSONObject("child").getJSONObject("children").isNull("grandChild"));
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

        JSONObject body = response.getBody().getObject();
        assertFalse(body.getJSONObject("properties").has("foo"));
        assertTrue(body.getJSONObject("properties").has("bar"));
        assertFalse(body.getJSONObject("properties").has("baz"));
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

        JSONObject body = response.getBody().getObject();
        assertFalse(body.getJSONObject("children").has("foo"));
        assertTrue(body.getJSONObject("children").has("bar"));
        assertFalse(body.getJSONObject("children").has("baz"));
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
        String binaryId = response.getBody().getObject().getJSONObject("properties").getJSONObject("binary").getString("value");

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
        String binaryId = response.getBody().getObject().getJSONObject("properties").getJSONObject("binary").getString("value");

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
        String binaryId = response.getBody().getObject().getJSONObject("properties").getJSONObject("binary").getString("value");

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
        String binaryId = response.getBody().getObject().getJSONObject("properties").getJSONObject("binary").getString("value");

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
        String binaryId = response.getBody().getObject().getJSONObject("properties").getJSONObject("binary").getString("value");

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
        String binaryId = response.getBody().getObject().getJSONObject("properties").getJSONObject("binary").getString("value");

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
        String binaryId = response.getBody().getObject().getJSONObject("properties").getJSONObject("binary").getString("value");

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

        String binaryId = response.getBody().getObject().getString("binaryId");
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

        JSONObject results = response.getBody().getObject();
        assertNotNull(results.getLong("total"));

        List<String> columns = getStringArray(results, "columns");
        assertTrue(columns.contains("name"));
        assertTrue(columns.contains("jcr:path"));

        List<String> selectors = getStringArray(results, "selectors");
        assertTrue(selectors.contains("node"));
    }

    private List<String> getStringArray(JSONObject parent, String name) {
        List<String> result = newArrayList();

        JSONArray array = parent.getJSONArray(name);

        for (int i = 0; i < array.length(); i++) {
            result.add(array.getString(i));
        }

        return result;
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

}
