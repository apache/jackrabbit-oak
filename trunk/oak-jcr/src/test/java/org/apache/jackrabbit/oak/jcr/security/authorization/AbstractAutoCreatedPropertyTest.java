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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.security.Privilege;

import org.junit.Test;

/**
 * Testing removing and re-adding node which defines autocreated protected properties.
 */
public abstract class AbstractAutoCreatedPropertyTest extends AbstractEvaluationTest {

    Node targetNode;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Node leaf = superuser.getNode(childNPath);
        targetNode = leaf.addNode(getNodeName());
        targetNode.addMixin(getMixinName());

        superuser.save();
    }

    abstract String getNodeName();

    abstract String getMixinName();

    @Test
    public void testReplaceNode() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_MODIFY_PROPERTIES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        testSession.removeItem(targetNode.getPath());
        Node newNode = testSession.getNode(childNPath).addNode(targetNode.getName(), targetNode.getPrimaryNodeType().getName());
        newNode.addMixin(getMixinName());
        try {
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            testSession.refresh(false);
        }
    }

    @Test
    public void testReplaceNode2() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        testSession.removeItem(targetNode.getPath());
        Node newNode = testSession.getNode(childNPath).addNode(targetNode.getName(), targetNode.getPrimaryNodeType().getName());
        newNode.addMixin(getMixinName());
        try {
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            testSession.refresh(false);
        }
    }

    @Test
    public void testReplaceNode3() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_REMOVE_CHILD_NODES, Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        testSession.removeItem(targetNode.getPath());
        Node newNode = testSession.getNode(childNPath).addNode(targetNode.getName(), targetNode.getPrimaryNodeType().getName());
        newNode.addMixin(getMixinName());
        try {
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            testSession.refresh(false);
        }
    }

    @Test
    public void testReplaceNode4() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        testSession.removeItem(targetNode.getPath());
        Node newNode = testSession.getNode(childNPath).addNode(targetNode.getName(), targetNode.getPrimaryNodeType().getName());
        newNode.addMixin(getMixinName());
        testSession.save();
    }

    @Test
    public void testRemoveReAddMixin() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES}));

        try {
            Node refNode = testSession.getNode(targetNode.getPath());
            refNode.removeMixin(getMixinName());
            refNode.addMixin(getMixinName());
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }
}
