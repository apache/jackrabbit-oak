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
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

/**
 * Permission evaluation tests for move operations.
 */
public class SessionMoveTest extends AbstractMoveTest {

    protected void move(String source, String dest) throws RepositoryException {
        move(source, dest, testSession);
    }

    @Override
    protected void move(String source, String dest, Session session) throws RepositoryException {
        session.move(source, dest);
        session.save();
    }

    @Test
    public void testMoveAndRemoveSubTree() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(siblingPath, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        testSession.move(childNPath, siblingDestPath);

        Node moved = testSession.getNode(siblingDestPath);
        Node child = moved.getNode(nodeName3);

        try {
            child.remove();
            testSession.save();
            fail("Removing subtree after move requires 'jcr:removeNode' privilege on the target");
        } catch (AccessDeniedException e) {
            // success

        }
    }

    @Test
    public void testMoveAndRemoveSubTree2() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_REMOVE_NODE}));
        allow(siblingPath, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        deny(testSession.getNode(nodePath3).getPath(), privilegesFromName(Privilege.JCR_REMOVE_NODE));

        try {
            testSession.move(childNPath, siblingDestPath);

            Node moved = testSession.getNode(siblingDestPath);
            Node child = moved.getNode(nodeName3);

            child.remove();
            testSession.save();
            fail("Removing subtree after move requires 'jcr:removeNode' on the removed child.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndRemoveSubTree3() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        allow(siblingPath, privilegesFromName(PrivilegeConstants.REP_WRITE));

        testSession.move(childNPath, siblingDestPath);

        Node moved = testSession.getNode(siblingDestPath);
        Node child = moved.getNode(nodeName3);
        child.remove();

        testSession.save();
    }

    @Test
    public void testMoveAndRemoveProperty() throws Exception {
        // TODO
    }

    @Test
    public void testMoveAndAddReplacementAtSource() throws Exception {
        // TODO
    }

    @Test
    public void testMoveAndAddSubTree() throws Exception {
        // TODO
    }

    @Test
    public void testMoveAndAddProperty() throws Exception {
        // TODO
    }

    @Test
    public void testMoveAndRemoveDestination() throws Exception {
        // TODO
    }

    @Test
    public void testMoveAndMoveSubTreeBack() throws Exception {
        // TODO
    }
}
