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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Permission evaluation tests for multiple move operations.
 */
public class MultipleMoveTest extends AbstractEvaluationTest {

    protected String nodePath3;
    protected String siblingDestPath;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        Node node3 = superuser.getNode(childNPath).addNode(nodeName3);
        node3.setProperty("movedProp", "val");
        nodePath3 = node3.getPath();
        superuser.save();
        testSession.refresh(false);

        siblingDestPath = siblingPath + "/destination";
    }

    private void move(String source, String dest) throws RepositoryException {
        move(source, dest, testSession);
    }

    private void move(String source, String dest, Session session) throws RepositoryException {
        session.move(source, dest);
    }

    private void setupMovePermissions(String source, String dest) throws Exception {
        allow(source, privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES
        }));
        allow(dest, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

    }

    @Test
    public void testMoveSubTreeBack() throws Exception {
        setupMovePermissions(path, siblingPath);

        try {
            // first move must succeed
            move(childNPath, siblingDestPath);
            // moving child back must fail due to missing privileges
            move(siblingDestPath + '/' + nodeName3, path + "/subtreeBack");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveSubTreeBack2() throws Exception {
        allow(testRootNode.getPath(), privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT
        }));

        //first move must succeed
        move(childNPath, siblingDestPath);
        //moving child back must fail due to missing privileges
        move(siblingDestPath + '/' + nodeName3, path + "/subtreeBack");
        testSession.save();
    }

    @Test
    public void testMoveSubTreeBack3() throws Exception {
        setupMovePermissions(path, siblingPath);

        try {
            // first move must succeed
            move(childNPath, siblingDestPath);
            // moving child back must fail due to missing privileges
            move(siblingDestPath + '/' + nodeName3, childNPath);
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Ignore("Known Limitation of OAK-710")
    @Test
    public void testMoveSubTreeBack4() throws Exception {
        allow(testRootNode.getPath(), privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT
        }));

        //first move must succeed
        move(childNPath, siblingDestPath);
        //moving child back must fail due to missing privileges
        move(siblingDestPath + '/' + nodeName3, childNPath);
        testSession.save();
    }

    @Test
    public void testMoveDestParent() throws Exception {
        setupMovePermissions(path, siblingPath);

        try {
            // first move must succeed
            move(childNPath, siblingDestPath);
            // moving dest parent must fail due to missing privileges
            move(siblingPath, path + "/parentMove");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Ignore("Known Limitation of OAK-710")
    @Test
    public void testMoveDestParent2() throws Exception {
        allow(testRootNode.getPath(), privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT
        }));

        //first move must succeed
        move(childNPath, siblingDestPath);
        //moving dest parent to original source location
        move(siblingPath, path + "/parentMove");
        testSession.save();
    }

    @Test
    public void testMoveDestParent3() throws Exception {
        setupMovePermissions(path, siblingPath);

        try {
            // first move must succeed
            move(childNPath, siblingDestPath);
            // moving dest parent to location of originally moved node must fail due to missing privileges
            move(siblingPath, childNPath);
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Ignore("Known Limitation of OAK-710")
    @Test
    public void testMoveDestParent4() throws Exception {
        allow(testRootNode.getPath(), privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT
        }));

        //first move must succeed
        move(childNPath, siblingDestPath);
        //moving dest parent to original source location
        move(siblingPath, childNPath);
        testSession.save();
    }
}
