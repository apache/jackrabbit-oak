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

import java.util.List;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
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

}