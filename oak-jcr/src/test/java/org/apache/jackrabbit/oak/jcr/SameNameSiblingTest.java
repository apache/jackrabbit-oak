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

package org.apache.jackrabbit.oak.jcr;

import static com.google.common.collect.Lists.asList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitNode;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class SameNameSiblingTest extends AbstractRepositoryTest {
    private static final String SIBLING = "sibling";

    private static final String[] SIBLINGS = new String[] {
            SIBLING + "[1]", SIBLING+ "[2]", SIBLING + "[3]", SIBLING + "[4]"};

    private Session session;
    private Node sns;

    public SameNameSiblingTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        session = getAdminSession();
        sns = session.getNode("/sns");
    }

    @Override
    protected NodeStore createNodeStore(NodeStoreFixture fixture) throws RepositoryException {
        try {
            NodeStore nodeStore = super.createNodeStore(fixture);

            NodeBuilder root = nodeStore.getRoot().builder();
            NodeBuilder sns = root.setChildNode("sns");
            sns.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
            for (String sibling : asList(SIBLING, SIBLINGS)) {
                if (!sibling.endsWith("[1]")) {
                    sns.setChildNode(sibling).setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
                }
            }
            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            return nodeStore;
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @Test
    public void iterateSiblings() throws RepositoryException {
        Set<String> expected = newHashSet(SIBLINGS);
        expected.add(SIBLING);
        expected.remove(SIBLINGS[0]);

        NodeIterator siblings = sns.getNodes();
        while (siblings.hasNext()) {
            Node sib = siblings.nextNode();
            assertTrue("Unexpected node: " + sib.getPath(), expected.remove(sib.getName()));
        }

        assertTrue("Missing nodes: " + expected, expected.isEmpty());
    }

    @Test
    public void getSiblings() throws RepositoryException {
        for (String name : asList(SIBLING, SIBLINGS)) {
            assertTrue(sns.hasNode(name));
            Node sib = sns.getNode(name);
            session.getNode(sns.getPath() + '/' + name);
        }
    }

    @Test
    public void modifySiblings() throws RepositoryException {
        for (String name : SIBLINGS) {
            Node sib = sns.getNode(name);
            sib.addNode("node");
            sib.setProperty("prop", 42);
        }
        session.save();

        for (String name : asList(SIBLING, SIBLINGS)) {
            Node sib = sns.getNode(name);
            assertTrue(sib.hasNode("node"));
            assertEquals(42L, sib.getProperty("prop").getLong());
        }
    }

    private static String rename(String name) {
        return name.replace('[', '_').replace(']', '_');
    }

    @Test
    public void moveSiblings() throws RepositoryException {
        Node target = session.getRootNode().addNode("target");
        session.save();

        for (String name : SIBLINGS) {
            session.move(sns.getPath() + '/' + name, target.getPath() + '/' + rename(name));
        }
        session.save();

        for (String name : SIBLINGS) {
            assertFalse(sns.hasNode(name));
            assertTrue(target.hasNode(rename(name)));
        }
    }

    @Test
    public void renameSiblings() throws RepositoryException {
        for (String name : SIBLINGS) {
            JackrabbitNode sib = (JackrabbitNode) sns.getNode(name);
            sib.rename(rename(name));
        }
        session.save();

        for (String name : SIBLINGS) {
            assertFalse(sns.hasNode(name));
            assertTrue(sns.hasNode(rename(name)));
        }
    }
}
