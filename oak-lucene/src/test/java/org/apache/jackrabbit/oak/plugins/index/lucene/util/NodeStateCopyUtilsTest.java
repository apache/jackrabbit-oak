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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.io.InputStream;
import java.util.Calendar;
import java.util.Iterator;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.After;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.*;

public class NodeStateCopyUtilsTest {
    private NodeBuilder builder = EMPTY_NODE.builder();
    private Repository repository;

    @After
    public void cleanup(){
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
    }

    @Test
    public void copyUnordered() throws Exception{
        builder.setProperty("foo", "x");
        builder.child("a").setProperty("foo", "y");
        builder.child("b").setProperty("foo", "z");
        builder.child("a").child("c").setProperty("foo", "acx");

        Tree tree = TreeFactory.createTree(EMPTY_NODE.builder());
        NodeStateCopyUtils.copyToTree(builder.getNodeState(), tree);

        assertEquals("x", tree.getProperty("foo").getValue(Type.STRING));
        assertEquals(2, tree.getChildrenCount(100));
        assertEquals("y", tree.getChild("a").getProperty("foo").getValue(Type.STRING));
        assertEquals("z", tree.getChild("b").getProperty("foo").getValue(Type.STRING));
        assertEquals("acx", tree.getChild("a").getChild("c").getProperty("foo").getValue(Type.STRING));
    }

    @Test
    public void copyOrdered() throws Exception{
        NodeBuilder testBuilder = EMPTY_NODE.builder();
        Tree srcTree = TreeFactory.createTree(testBuilder);
        srcTree.setOrderableChildren(true);
        srcTree.setProperty("foo", "x");

        srcTree.addChild("a").setOrderableChildren(true);
        srcTree.addChild("a").setProperty("foo", "y");

        srcTree.addChild("b").setOrderableChildren(true);
        srcTree.addChild("b").setProperty("foo", "z");

        Tree tree = TreeFactory.createTree(EMPTY_NODE.builder());
        NodeStateCopyUtils.copyToTree(testBuilder.getNodeState(), tree);

        assertEquals("x", tree.getProperty("foo").getValue(Type.STRING));
        assertEquals(2, tree.getChildrenCount(100));
        assertEquals("y", tree.getChild("a").getProperty("foo").getValue(Type.STRING));
        assertEquals("z", tree.getChild("b").getProperty("foo").getValue(Type.STRING));

        //Assert the order
        Iterator<Tree> children = tree.getChildren().iterator();
        assertEquals("a", children.next().getName());
        assertEquals("b", children.next().getName());
    }

    @Test
    public void copyToJcr() throws Exception{
        repository = new Jcr().with(new OpenSecurityProvider()).createRepository();

        Tree srcTree = TreeFactory.createTree(builder);
        srcTree.setOrderableChildren(true);
        srcTree.setProperty("foo", "x");
        srcTree.setProperty("foo", "x");
        srcTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);

        srcTree.addChild("a").setOrderableChildren(true);
        srcTree.addChild("a").setProperty("foo", "y");
        srcTree.addChild("a").setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);

        srcTree.addChild("b").setOrderableChildren(true);
        srcTree.addChild("b").setProperty("foo", "z");
        srcTree.addChild("b").setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);

        Session session = repository.login(null, null);
        Node node = session.getRootNode();
        Node test = node.addNode("test", NT_OAK_UNSTRUCTURED);

        NodeStateCopyUtils.copyToNode(builder.getNodeState(), test);
        session.save();

        test = session.getNode("/test");
        assertEquals("y", test.getProperty("a/foo").getString());
        assertEquals("z", test.getProperty("b/foo").getString());
    }

    @Test
    public void copyToJcrAndHiddenProps() throws Exception{
        repository = new Jcr().with(new OpenSecurityProvider()).createRepository();

        Tree srcTree = TreeFactory.createTree(builder);
        srcTree.addChild("a").setProperty("foo", "y");
        srcTree.addChild("a").setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);
        builder.child(":hidden-node").setProperty("x", "y");
        builder.setProperty(":hidden-prop", "y");

        Session session = repository.login(null, null);
        Node node = session.getRootNode();
        Node test = node.addNode("test", NT_OAK_UNSTRUCTURED);

        NodeStateCopyUtils.copyToNode(builder.getNodeState(), test);
        session.save();

        test = session.getNode("/test");
        assertEquals("y", test.getProperty("a/foo").getString());
    }

    @Test
    public void copyToJcrVariousProps() throws Exception{
        repository = new Jcr().with(new OpenSecurityProvider()).createRepository();

        Calendar cal = ISO8601.parse(ISO8601.format(Calendar.getInstance()));
        Tree srcTree = TreeFactory.createTree(builder);
        srcTree.setOrderableChildren(true);
        srcTree.setProperty("fooString", "x");
        srcTree.setProperty("fooLong", 1L, Type.LONG);
        srcTree.setProperty("fooPath", "/fooNode", Type.PATH);
        srcTree.setProperty("fooName", "mix:title", Type.NAME);
        srcTree.setProperty("fooDouble", 1.0, Type.DOUBLE);
        srcTree.setProperty("fooDate", ISO8601.format(cal), Type.DATE);
        srcTree.setProperty("fooBoolean", true, Type.BOOLEAN);
        srcTree.setProperty("fooStrings", asList("a", "b"), Type.STRINGS);
        srcTree.setProperty("fooBlob", new ArrayBasedBlob("foo".getBytes()), Type.BINARY);
        srcTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);

        srcTree.setProperty(JcrConstants.JCR_MIXINTYPES, asList("mix:mimeType", "mix:title"), Type.NAMES);

        Session session = repository.login(null, null);
        Node node = session.getRootNode();
        Node test = node.addNode("test", NT_OAK_UNSTRUCTURED);
        Node fooNode = node.addNode("fooNode", NT_OAK_UNSTRUCTURED);

        NodeStateCopyUtils.copyToNode(builder.getNodeState(), test);
        session.save();

        test = session.getNode("/test");
        assertEquals("x", test.getProperty("fooString").getString());
        assertEquals("/fooNode", test.getProperty("fooPath").getNode().getPath());
        assertEquals("mix:title", test.getProperty("fooName").getString());
        assertEquals(1, test.getProperty("fooLong").getLong());

        assertEquals(cal, test.getProperty("fooDate").getDate());
        assertEquals("a", test.getProperty("fooStrings").getValues()[0].getString());
        assertEquals("b", test.getProperty("fooStrings").getValues()[1].getString());

        InputStream is = test.getProperty("fooBlob").getBinary().getStream();
        String streamVal = IOUtils.toString(is, "UTF-8");
        assertEquals("foo", streamVal);
    }

}