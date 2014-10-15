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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;
import javax.jcr.retention.RetentionPolicy;
import javax.jcr.security.Privilege;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

public class TestContentLoader {

    /**
     * The encoding of the test resources.
     */
    private static final String ENCODING = "UTF-8";

    public void loadTestContent(Session session) throws RepositoryException, IOException, ParseException {
        session.getWorkspace().getNamespaceRegistry().registerNamespace(
                "test", "http://www.apache.org/jackrabbit/test");

        registerTestNodeTypes(session);

        Node root = session.getRootNode();
        Node data = getOrAddNode(root, "testdata");
        addPropertyTestData(getOrAddNode(data, "property"));
        addQueryTestData(getOrAddNode(data, "query"));
        addNodeTestData(getOrAddNode(data, "node"));
        if (session.getRepository().getDescriptorValue(Repository.OPTION_LIFECYCLE_SUPPORTED).getBoolean()) {
            addLifecycleTestData(getOrAddNode(data, "lifecycle"));
        }
        addExportTestData(getOrAddNode(data, "docViewTest"));

        if (session.getRepository().getDescriptorValue(Repository.OPTION_RETENTION_SUPPORTED).getBoolean()) {
            Node conf = getOrAddNode(session.getRootNode(), "testconf");
            addRetentionTestData(getOrAddNode(conf, "retentionTest"));
        }

        AccessControlUtils.addAccessControlEntry(session, "/", EveryonePrincipal.getInstance(), new String[]{Privilege.JCR_READ}, true);
        AccessControlUtils.addAccessControlEntry(session, "/jcr:system", EveryonePrincipal.getInstance(), new String[]{Privilege.JCR_READ}, false);
        session.save();
    }

    private static void registerTestNodeTypes(Session session) throws RepositoryException, ParseException, IOException {
        InputStream stream = TestContentLoader.class.getResourceAsStream("test_nodetypes.cnd");
        try {
            CndImporter.registerNodeTypes(
                    new InputStreamReader(stream, Charsets.UTF_8), session);
        } finally {
            stream.close();
        }
    }

    private static Node getOrAddNode(Node node, String name) throws RepositoryException {
        try {
            return node.getNode(name);
        } catch (PathNotFoundException e) {
            return node.addNode(name);
        }
    }

    /**
     * Creates a boolean, double, long, calendar and a path property at the
     * given node.
     */
    private static void addPropertyTestData(Node node) throws RepositoryException {
        node.setProperty("boolean", true);
        node.setProperty("double", Math.PI);
        node.setProperty("long", 90834953485278298L);
        Calendar c = Calendar.getInstance();
        c.set(2005, 6, 18, 17, 30);
        node.setProperty("calendar", c);
        ValueFactory factory = node.getSession().getValueFactory();
        node.setProperty("path", factory.createValue("/", PropertyType.PATH));
        node.setProperty("multi", new String[] { "one", "two", "three" });
    }

    /**
     * Creates a node with a RetentionPolicy
     */
    private  void addRetentionTestData(Node node) throws RepositoryException {
        RetentionPolicy rp = createRetentionPolicy("testRetentionPolicy", node.getSession());
        node.getSession().getRetentionManager().setRetentionPolicy(node.getPath(), rp);
    }

    private RetentionPolicy createRetentionPolicy(String testRetentionPolicy, Session session) {
        throw new UnsupportedOperationException("Retention Management not yet implemented");
    }

    /**
     * Creates four nodes under the given node. Each node has a String property
     * named "prop1" with some content set.
     */
    private static void addQueryTestData(Node node) throws RepositoryException {
        while (node.hasNode("node1")) {
            node.getNode("node1").remove();
        }
        getOrAddNode(node, "node1").setProperty("prop1", "You can have it good, cheap, or fast. Any two.");
        getOrAddNode(node, "node1").setProperty("prop1", "foo bar");
        getOrAddNode(node, "node1").setProperty("prop1", "Hello world!");
        getOrAddNode(node, "node2").setProperty("prop1", "Apache Jackrabbit");
    }

    /**
     * Creates three nodes under the given node: one of type nt:resource and the
     * other nodes referencing it.
     */
    private static void addNodeTestData(Node node) throws RepositoryException, IOException {
        if (node.hasNode("multiReference")) {
            node.getNode("multiReference").remove();
        }
        if (node.hasNode("resReference")) {
            node.getNode("resReference").remove();
        }
        if (node.hasNode("myResource")) {
            node.getNode("myResource").remove();
        }

        Node resource = node.addNode("myResource", "nt:resource");
        // nt:resource not longer referenceable since JCR 2.0
        resource.addMixin("mix:referenceable");
        resource.setProperty("jcr:encoding", ENCODING);
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty("jcr:data", "Hello w\u00F6rld.", PropertyType.BINARY);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());

        Node resReference = getOrAddNode(node, "reference");
        resReference.setProperty("ref", resource);
        // make this node itself referenceable
        resReference.addMixin("mix:referenceable");

        Node multiReference = node.addNode("multiReference");
        ValueFactory factory = node.getSession().getValueFactory();
        multiReference.setProperty("ref", new Value[] {
            factory.createValue(resource),
            factory.createValue(resReference)
        });

        // NodeDefTest requires a test node with a mandatory child node
        JcrUtils.putFile(node, "testFile", "text/plain", new ByteArrayInputStream("Hello, World!".getBytes("UTF-8")));
    }

    /**
     * Creates a lifecycle policy node and another node with a lifecycle
     * referencing that policy.
     */
    private  void addLifecycleTestData(Node node) throws RepositoryException {
        Node policy = getOrAddNode(node, "policy");
        policy.addMixin(NodeType.MIX_REFERENCEABLE);
        Node transitions = getOrAddNode(policy, "transitions");
        Node transition = getOrAddNode(transitions, "identity");
        transition.setProperty("from", "identity");
        transition.setProperty("to", "identity");
        Node lifecycle = getOrAddNode(node, "node");
        assignLifecyclePolicy(lifecycle, policy, "identity");
    }

    private void assignLifecyclePolicy(Node lifecycle, Node policy, String identity) {
        throw new UnsupportedOperationException("Lifecycle Management is not yet implemented");

    }

    private static void addExportTestData(Node node) throws RepositoryException, IOException {
        getOrAddNode(node, "invalidXmlName").setProperty("propName", "some text");

        // three nodes which should be serialized as xml text in docView export
        // separated with spaces
        getOrAddNode(node, "jcr:xmltext").setProperty("jcr:xmlcharacters", "A text without any special character.");
        getOrAddNode(node, "some-element");
        getOrAddNode(node, "jcr:xmltext").setProperty("jcr:xmlcharacters",
                " The entity reference characters: <, ', ,&, >,  \" should" + " be escaped in xml export. ");
        getOrAddNode(node, "some-element");
        getOrAddNode(node, "jcr:xmltext").setProperty("jcr:xmlcharacters", "A text without any special character.");

        Node big = getOrAddNode(node, "bigNode");
        big.setProperty("propName0", "SGVsbG8gd8O2cmxkLg==;SGVsbG8gd8O2cmxkLg==".split(";"), PropertyType.BINARY);
        big.setProperty("propName1", "text 1");
        big.setProperty("propName2", "multival text 1;multival text 2;multival text 3".split(";"));
        big.setProperty("propName3", "text 1");

        addExportValues(node, "propName");
        addExportValues(node, "Prop<>prop");
    }

    /**
     * create nodes with following properties binary & single binary & multival
     * notbinary & single notbinary & multival
     */
    private static void addExportValues(Node node, String name) throws RepositoryException, IOException {
        String prefix = "valid";
        if (name.indexOf('<') != -1) {
            prefix = "invalid";
        }
        node = getOrAddNode(node, prefix + "Names");

        String[] texts = new String[] { "multival text 1", "multival text 2", "multival text 3" };
        getOrAddNode(node, prefix + "MultiNoBin").setProperty(name, texts);

        Node resource = getOrAddNode(node, prefix + "MultiBin");
        resource.setProperty("jcr:encoding", ENCODING);
        resource.setProperty("jcr:mimeType", "text/plain");
        String[] values = new String[] { "SGVsbG8gd8O2cmxkLg==", "SGVsbG8gd8O2cmxkLg==" };
        resource.setProperty(name, values, PropertyType.BINARY);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());

        getOrAddNode(node, prefix + "NoBin").setProperty(name, "text 1");

        resource = getOrAddNode(node, "invalidBin");
        resource.setProperty("jcr:encoding", ENCODING);
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(name, "Hello w\u00F6rld.", PropertyType.BINARY);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
    }
}
