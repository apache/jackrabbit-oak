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
package org.apache.jackrabbit.oak.security.authorization;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AuthorizationContextTest extends AbstractSecurityTest {

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    private void createAcl(@Nullable String path, String... privilegeNames) throws RepositoryException {
        AccessControlManager acMgr = getAccessControlManager(root);

        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        assertNotNull(acl);

        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(privilegeNames));
        acMgr.setPolicy(path, acl);
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2740">OAK-2740</a>
     */
    @Test
    public void testItemDefinitionsDefinesContextRoot() throws Exception {
        List<String> paths = Lists.newArrayList(
                "/jcr:system/jcr:nodeTypes/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy",
                "/jcr:system/jcr:nodeTypes/rep:RepoAccessControllable/rep:namedChildNodeDefinitions/rep:repoPolicy");

        for (String defPath : paths) {
            Tree tree = root.getTree(defPath);
            assertFalse(AuthorizationContext.getInstance().definesContextRoot(tree));
        }
    }

    @Test
    public void testPolicyDefinesContextRoot() throws Exception {
        createAcl("/", PrivilegeConstants.JCR_READ);

        Tree aclTree = root.getTree("/").getChild(AccessControlConstants.REP_POLICY);
        assertTrue(aclTree.exists());
        assertTrue(AuthorizationContext.getInstance().definesContextRoot(aclTree));
    }

    @Test
    public void testRepoPolicyDefinesContextRoot() throws Exception {
        createAcl(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT);

        Tree aclTree = root.getTree("/").getChild(AccessControlConstants.REP_REPO_POLICY);
        assertTrue(aclTree.exists());
        assertTrue(AuthorizationContext.getInstance().definesContextRoot(aclTree));
    }

    @Test
    public void testAceDefinesContextRoot() throws Exception {
        createAcl("/", PrivilegeConstants.JCR_READ);

        Tree aclTree = root.getTree("/").getChild(AccessControlConstants.REP_POLICY);
        assertTrue(aclTree.exists());

        for (Tree child : aclTree.getChildren()) {
            assertFalse(AuthorizationContext.getInstance().definesContextRoot(child));
        }
    }

    @Test
    public void testLocation() throws Exception {
        createAcl("/", PrivilegeConstants.JCR_READ);

        Context ctx = AuthorizationContext.getInstance();

        String policyPath = "/rep:policy";
        assertTrue(ctx.definesLocation(TreeLocation.create(root, policyPath + "/allow")));
        assertTrue(ctx.definesLocation(TreeLocation.create(root, policyPath + "/allow/" + AccessControlConstants.REP_PRINCIPAL_NAME)));
        assertTrue(ctx.definesLocation(TreeLocation.create(root, policyPath + "/allow/" + AccessControlConstants.REP_PRIVILEGES)));

        List<String> existingRegular = ImmutableList.of(
                "/",
                "/jcr:system"
        );
        for (String path : existingRegular) {
            assertFalse(path, ctx.definesLocation(TreeLocation.create(root, path)));
            assertFalse(path, ctx.definesLocation(TreeLocation.create(root, PathUtils.concat(path, JcrConstants.JCR_PRIMARYTYPE))));
        }

        List<String> nonExistingItem = ImmutableList.of(
                '/' + AccessControlConstants.REP_REPO_POLICY,
                "/content/" + AccessControlConstants.REP_POLICY,
                "/content/" + AccessControlConstants.REP_PRIVILEGES,
                "/content/" + AccessControlConstants.REP_REPO_POLICY,
                "/jcr:system/" + AccessControlConstants.REP_POLICY,
                PermissionConstants.PERMISSIONS_STORE_PATH + "/nonexisting");
        for (String path : nonExistingItem) {
            assertTrue(path, ctx.definesLocation(TreeLocation.create(root, path)));
            assertTrue(path, ctx.definesLocation(TreeLocation.create(root, PathUtils.concat(path, AccessControlConstants.REP_PRIVILEGES))));
        }
    }

    @Test
    public void testGetType() throws Exception {
        TreeTypeProvider ttp = new TreeTypeProvider(AuthorizationContext.getInstance());
        for (TypeTest test : TypeTest.createTests(root)) {
            assertEquals(test.path, test.type, ttp.getType(root.getTree(test.path)));
        }
    }

    @Test
    public void testGetTypeWithParentType() throws Exception {
        TreeTypeProvider ttp = new TreeTypeProvider(AuthorizationContext.getInstance());
        for (TypeTest test : TypeTest.createTests(root)) {
            assertEquals(test.path, test.type, ttp.getType(root.getTree(test.path), test.parentType));
        }
    }

    @Test
    public void testGetTypeWithDefaultParentType() throws Exception {
        TreeTypeProvider ttp = new TreeTypeProvider(AuthorizationContext.getInstance());
        for (TypeTest test : TypeTest.createTests(root)) {
            TreeType typeIfParentDefault = ttp.getType(root.getTree(test.path), TreeType.DEFAULT);

            if (TreeType.DEFAULT == test.parentType) {
                assertEquals(test.path, test.type, typeIfParentDefault);
            } else {
                assertNotEquals(test.path, test.type, typeIfParentDefault);
            }
        }
    }


    private static final class TypeTest {

        private final String path;
        private final TreeType type;
        private final TreeType parentType;

        private TypeTest(@Nonnull String path, TreeType type) {
            this(path, type, TreeType.DEFAULT);
        }

        private TypeTest(@Nonnull String path, TreeType type, TreeType parentType) {
            this.path = path;
            this.type = type;
            this.parentType = parentType;
        }

        private static List<TypeTest> createTests(@Nonnull Root root) throws Exception {
            List<TypeTest> tests = new ArrayList();
            tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy", TreeType.DEFAULT));
            tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy/rep:Policy", TreeType.DEFAULT));
            tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:ACL/rep:residualChildNodeDefinitions/rep:ACE", TreeType.DEFAULT));
            tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:GrantACE/rep:namedChildNodeDefinitions/rep:restrictions", TreeType.DEFAULT));
            tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:RepoAccessControllable/rep:namedChildNodeDefinitions/rep:repoPolicy", TreeType.DEFAULT));
            tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:PermissionStore", TreeType.DEFAULT));


            tests.add(new TypeTest(PermissionConstants.PERMISSIONS_STORE_PATH, TreeType.INTERNAL));
            tests.add(new TypeTest(PermissionConstants.PERMISSIONS_STORE_PATH + "/a/b/child", TreeType.INTERNAL, TreeType.INTERNAL));

            NodeUtil testTree = new NodeUtil(root.getTree("/")).addChild("test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            for (String name : AccessControlConstants.POLICY_NODE_NAMES) {
                NodeUtil acl = testTree.addChild(name, AccessControlConstants.NT_REP_ACL);
                tests.add(new TypeTest(acl.getTree().getPath(), TreeType.ACCESS_CONTROL));

                NodeUtil ace = acl.addChild("ace", AccessControlConstants.NT_REP_DENY_ACE);
                tests.add(new TypeTest(ace.getTree().getPath(), TreeType.ACCESS_CONTROL, TreeType.ACCESS_CONTROL));

                NodeUtil ace2 = acl.addChild("ace2", AccessControlConstants.NT_REP_GRANT_ACE);
                tests.add(new TypeTest(ace2.getTree().getPath(), TreeType.ACCESS_CONTROL, TreeType.ACCESS_CONTROL));

                NodeUtil rest = ace2.addChild(AccessControlConstants.REP_RESTRICTIONS, AccessControlConstants.NT_REP_RESTRICTIONS);
                tests.add(new TypeTest(rest.getTree().getPath(), TreeType.ACCESS_CONTROL, TreeType.ACCESS_CONTROL));

                NodeUtil invalid = rest.addChild("invalid", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                tests.add(new TypeTest(invalid.getTree().getPath(), TreeType.ACCESS_CONTROL, TreeType.ACCESS_CONTROL));
            }
            return tests;
        }
    }
}