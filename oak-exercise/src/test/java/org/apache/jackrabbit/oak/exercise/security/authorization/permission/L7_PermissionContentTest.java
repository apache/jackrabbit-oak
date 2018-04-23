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
package org.apache.jackrabbit.oak.exercise.security.authorization.permission;

import javax.jcr.GuestCredentials;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Representation of Permissions in the Repository
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how the default implementation represents permissions in the repository.
 * This advanced exercise aims to provide you with some insights to the permission
 * store and how permissions are being evaluated from the content stored
 * therein.
 *
 * Exercises:
 *
 * - Overview
 *   Look {@code org/apache/jackrabbit/oak/plugins/nodetype/write/builtin_nodetypes.cnd}
 *   and try to identify the built in node types used to store permission
 *   content.
 *
 *   Question: Can explain the meaning of all types?
 *   Question: Why are most item definitions protected?
 *   Question: Can you identify node types that are not used? Can you explain why?
 *
 * - {@link #testAdministrativeAccessOnly()}
 *   The permission store is hidden from regular users and can only be
 *   accessed using an administrative session. Use to test to find out how this
 *   is enforced by the implementation.
 *
 *   Question: Can you imagine why the permission store should only be accessible
 *   to administrative sessions?
 *   Question: Can you identify the class(es) that actually enforce this?
 *   Question: Can you explain why the permission store is not 'hidden' like the
 *   index content? Compare these different approaches and discuss your findings.
 *
 * - {@link #testReadOnly()}
 *   The permission store is a system maintained structure and cannot be edited
 *   using JCR or Oak API calls. This test aims to illustrate this behavior.
 *
 *   Question: Can you explain why the permission store is read-only?
 *   Question: Can you identify the class(es) responsible for enforcing the read-only nature?
 *
 * </pre>
 */
public class L7_PermissionContentTest extends AbstractSecurityTest {

    String permissionStorePath = null; // EXERCISE: specify the path to the permission store root node

    @Test
    public void testAdministrativeAccessOnly() {
        Root root = adminSession.getLatestRoot();
        Tree permissionStoreTree = root.getTree(permissionStorePath);
        assertTrue(permissionStoreTree.exists());

        // EXERCISE : explain the content structure of the permission store
        Tree wspTree = permissionStoreTree.getChild(adminSession.getWorkspaceName());
        for (Tree t : wspTree.getChildren()) {
            System.out.println(t.getName());
        }

        // EXERCISE : pick one child tree above and inspect the subtree
        //            - what does the name of the child stand for?
        //            - explain the structure
        String name = null; // EXERCISE
        Tree child = wspTree.getChild(name);

        // EXERCISE: walk through the tree structure and look at the properties
    }

    @Test
    public void testReadOnly() throws Exception {
        ContentSession guestSession = login(new GuestCredentials());

        Root root = guestSession.getLatestRoot();
        Tree permissionStoreTree = root.getTree(permissionStorePath);

        // EXERCISE: explain the fact that the tree does not exist
        assertFalse(permissionStoreTree.exists());
    }
}