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

import java.security.Principal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Introduction
 * -----------------------------------------------------------------------------
 *
 * Become familiar with the way to verify permissions using JCR API.
 * Get a basic understanding how permission evaluation is used and exposed in Oak
 * and finally gain insight into some details of the default implementation.
 *
 * Exercises:
 *
 * - Overview and Usages of Permission Evaluation
 *   Search and list for permission related methods in the JCR API and recap what
 *   the specification states about permissions compared to access control
 *   management.
 *
 *   Question: What are the areas in JCR that deal with permissions?
 *   Question: Who is the expected API consumer?
 *   Question: Can you elaborate when it actually makes sense to use this API?
 *   Question: Can you think about potential drawbacks of doing so?
 *
 * - Permission Evaluation in Oak
 *   In a second step try to become more familiar with the nature of the
 *   permission evaluation in Oak.
 *
 *   Question: What is the nature of the public SPI?
 *   Question: Can you identify the main entry point for permission evaluation?
 *   Question: Can you identify to exact location(s) in Oak where read-access is being enforced?
 *   Question: Can you identify the exact location(s) in Oak where all kind of write access is being enforced?
 *
 * - Configuration
 *   Look at the default implementation(s) of the {@link org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration}
 *   and try to identify the configurable parts with respect to permission evaluation.
 *   Compare your results with the Oak documentation.
 *
 *   Question: Can you provide a list of configuration options for the permission evaluation?
 *   Question: Can you identify where these configuration options are being evaluated?
 *   Question: Which options also affect the access control management?
 *
 * - Pluggability
 *   Become familar with the pluggable parts of the permission evaluation
 *
 *   Question: What means does Oak provide to change or extend the permission evaluation?
 *   Question: Can you identify the interfaces that you needed to implement?
 *   Question: Would it be possible to only replace the implementation of {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider}?
 *             How could you achieve this?
 *             And what would be the consequences for the whole authorization module?
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Read Permission Walkthrough
 *   Use {@link #testReadPermissionWalkThrough()} to become familiar with the
 *   very internals of the permission evaluation. Walk through the item read
 *   methods (for simplicity reduced to oak-core only) and be aware of
 *   permission-evaluation related parts.
 *
 *   Question: Can you list the relevant steps wrt permission evalution?
 *   Question: What is the nature of the returned tree objects?
 *   Question: How can you verify if the editing test session can actually read those trees without using the permission-evaluation code?
 *   Question: How can you verify if the properties are accessible.
 *
 * - Write Permission Walkthrough
 *   Use {@link #testWritePermissionWalkThrough()} to become familiar with the
 *   internals of the permission evaluation with respect to writing. Walk through
 *   the item write methods (for simplicity reduced to oak-core only) and the
 *   subsequent {@link org.apache.jackrabbit.oak.api.Root#commit()} and be aware
 *   of permission-evaluation related parts.
 *
 *   Question: Can you list the relevant steps wrt permission evalution?
 *   Question: What can you say about write permissions for special (protected) items?
 *
 * - Extending {@link #testReadPermissionWalkThrough()} and {@link #testWritePermissionWalkThrough()}
 *   Use the two test-cases and play with additional access/writes and or
 *   additional (more complex) permission setup.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L2_PermissionDiscoveryTest}
 *
 * </pre>
 */
public class L1_IntroductionTest extends AbstractSecurityTest {

    private ContentSession testSession;

    @Override
    public void before() throws Exception {
        super.before();
        testSession = createTestSession();

        Principal testPrincipal = getTestUser().getPrincipal();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil a = rootNode.addChild("a", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        a.setString("aProp", "aValue");

        NodeUtil b = a.addChild("b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        b.setString("bProp", "bValue");
        // sibling
        NodeUtil bb = a.addChild("bb", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        bb.setString("bbProp", "bbValue");

        NodeUtil c = b.addChild("c", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        c.setString("cProp", "cValue");

        setupPermission(root, "/a", testPrincipal, true, PrivilegeConstants.JCR_READ);
        setupPermission(root, "/a/b", testPrincipal, true, PrivilegeConstants.JCR_ADD_CHILD_NODES);
        setupPermission(root, "/a/bb", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);
        setupPermission(root, "/a/b/c", testPrincipal, true, PrivilegeConstants.REP_ADD_PROPERTIES);

        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (testSession != null) {
                testSession.close();
            }
            root.getTree("/a").remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    /**
     * Setup simple allow/deny permissions (without restrictions).
     *
     * @param root The editing root.
     * @param path The path of the access controlled tree.
     * @param principal The principal for which new ACE is being created.
     * @param isAllow {@code true} if privileges are granted; {@code false} otherwise.
     * @param privilegeNames The privilege names.
     * @throws Exception If an error occurs.
     */
    private void setupPermission(@Nonnull Root root,
                                 @Nullable String path,
                                 @Nonnull Principal principal,
                                 boolean isAllow,
                                 @Nonnull String... privilegeNames) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = checkNotNull(AccessControlUtils.getAccessControlList(acMgr, path));
        acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), isAllow);
        acMgr.setPolicy(path, acl);
        root.commit();
    }

    @Test
    public void testReadPermissionWalkThrough() {
        Root testRoot = testSession.getLatestRoot();

        // EXERCISE verify if these tree are accessible using Tree#exists()
        // Question: can you explain why using Tree.exists is sufficient and you don't necessarily need to perform the check on the PermissionProvider?
        Tree rootTree = testRoot.getTree("/");
        Tree bTree = testRoot.getTree("/a/b");
        Tree cTree = testRoot.getTree("/a/b/c");

        // EXERCISE verify if this is an accessible property? Q: how can you do this withouth testing the readability on the PermissionProvider?
        PropertyState bProp = bTree.getProperty("bProp");
        PropertyState bbProp = testRoot.getTree("/a/bb").getProperty("bbProp");
        PropertyState cProp = cTree.getProperty("cProp");
    }

    @Test
    public void testWritePermissionWalkThrough() throws CommitFailedException {
        Root testRoot = testSession.getLatestRoot();

        // EXERCISE walk through the test and fix it such that it passes.

        Tree bTree = testRoot.getTree("/a/b");

        // add a new child node at '/a/b' and persist the change to trigger the permission evaluation.
        // EXERCISE: does it work with the current permission setup? if not, why (+ add exception handling)?
        try {
            Tree child = bTree.addChild("childName");
            child.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            testRoot.commit();
        } finally {
            testRoot.refresh();
        }

        // now change the primary type of the 'bTree'
        // EXERCISE: does it work with the current permission setup? if not, why (+ add exception handling)?
        try {
            bTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED);
            testRoot.commit();
        } finally {
            testRoot.refresh();
        }

        Tree cTree = testRoot.getTree("/a/b/c");

        // now change the regula property 'cProp' of the 'cTree'
        // EXERCISE: does it work with the current permission setup? if not, why (+ add exception handling)?
        try {
            cTree.setProperty("cProp", "changedValue");
            testRoot.commit();
        } finally {
            testRoot.refresh();
        }


        // finally we try to add a new property to the 'cTree'
        // EXERCISE: does it work with the current permission setup? if not, why (+ add exception handling)?
        try {
            cTree.setProperty("anotherCProp", "val");
            testRoot.commit();
        } finally {
            root.refresh();
        }
    }
}