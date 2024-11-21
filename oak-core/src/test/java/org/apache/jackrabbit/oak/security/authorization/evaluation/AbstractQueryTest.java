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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.query.Query;
import javax.jcr.security.AccessControlManager;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractQueryTest extends AbstractOakCoreTest {
    
    Tree node;
    Tree subnode;

    @Before
    public void before() throws Exception {
        super.before();

        createIndexDefinition();

        node = TreeUtil.addChild(root.getTree("/"), "node", JcrConstants.NT_UNSTRUCTURED);
        subnode = TreeUtil.addChild(node, "subnode", JcrConstants.NT_UNSTRUCTURED);
        root.commit();
    }

    void grantPropertyReadAccess(@NotNull String propertyName) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
        if (acl != null) {
            Map<String, Value[]> restrictions = Map.of(AccessControlConstants.REP_ITEM_NAMES, new Value[] {getValueFactory(root).createValue(propertyName, PropertyType.NAME)});
            acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.REP_READ_PROPERTIES), true, null, restrictions);
            acMgr.setPolicy(acl.getPath(), acl);
        }
    }

    void createIndexDefinition() throws RepositoryException {};
    abstract String getStatement();

    @Test
    public void testQueryWithEmptyGlobRestriction() throws Exception {
        // setup permissions for testuser using rep:glob restriction such that
        // - access to /node is granted
        // - access to /node/subnode is denied
        AccessControlManager acm = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, node.getPath());
        if (acl != null) {
            Map<String, Value> restrictions = Map.of(AccessControlConstants.REP_GLOB, getValueFactory(root).createValue(""));
            acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acm, PrivilegeConstants.JCR_ALL), true, restrictions);
            acm.setPolicy(acl.getPath(), acl);
            root.commit();
        }

        assertAccess(node.getPath(), subnode.getPath(), false);

        // test that query result corresponds to the direct access (node readable, subnode not readable)
        Result result = getTestRoot().getQueryEngine().executeQuery(getStatement(), Query.JCR_SQL2, Collections.emptyMap(), Collections.emptyMap());

        Iterable<String> expected = Set.of(node.getPath());
        assertTrue(Iterables.elementsEqual(expected, Iterables.transform(result.getRows(), ResultRow::getPath)));
    }

    @Test
    public void testQueryWithEmptyGlobRestrictionAndPropertyRead() throws Exception {
        // setup permissions for testuser using rep:glob restriction such that
        // - access to /node is granted
        // - access to /node/subnode is denied
        AccessControlManager acm = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, node.getPath());
        if (acl != null) {
            Map<String, Value> restrictions = Map.of(AccessControlConstants.REP_GLOB, getValueFactory(root).createValue(""));
            acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ALL), true, restrictions);

            restrictions = Map.of(AccessControlConstants.REP_GLOB, getValueFactory(root).createValue("/"+NodeTypeConstants.JCR_PRIMARYTYPE));
            acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.REP_READ_PROPERTIES), true, restrictions);

            acm.setPolicy(acl.getPath(), acl);
            root.commit();
        }

        assertAccess(node.getPath(), subnode.getPath(), true);

        // test that query result corresponds to the direct access (node readable, subnode not readable)
        Result result = getTestRoot().getQueryEngine().executeQuery(getStatement(), Query.JCR_SQL2, Collections.emptyMap(), Collections.emptyMap());

        Iterable<String> expected = Set.of(node.getPath());
        assertTrue(Iterables.elementsEqual(expected, Iterables.transform(result.getRows(), row -> row.getPath())));
    }

    @Test
    public void testQueryWithAllowNodeAndDenySubNode() throws Exception {
        // setup permissions for testuser using 2 aces such that
        // - access to /node is granted
        // - access to /node/subnode is denied
        setupPermission(node.getPath(), testPrincipal, true, PrivilegeConstants.JCR_ALL);
        setupPermission(subnode.getPath(), testPrincipal, false, PrivilegeConstants.JCR_ALL);

        assertAccess(node.getPath(), subnode.getPath(), true);

        // test that query result corresponds to the direct access (node readable, subnode not readable)
        Result result = getTestRoot().getQueryEngine().executeQuery(getStatement(), Query.JCR_SQL2, Collections.emptyMap(), Collections.emptyMap());

        Iterable<String> expected = Set.of(node.getPath());
        assertTrue(Iterables.elementsEqual(expected, Iterables.transform(result.getRows(), row -> row.getPath())));
    }

    private void assertAccess(@NotNull String nodePath, @NotNull String subnodePath, boolean canReadPrimaryType) throws Exception {
        // verify access control setup
        assertTrue(getTestRoot().getTree(nodePath).exists());
        assertFalse(getTestRoot().getTree(subnodePath).exists());

        // verify PermissionProvider.isGranted(String, String) as it is used inside
        // the query code base (FilterImpl.isAccessible)
        PermissionProvider pp = getConfig(AuthorizationConfiguration.class).getPermissionProvider(getTestRoot(), getTestSession().getWorkspaceName(), getTestSession().getAuthInfo().getPrincipals());
        assertTrue(pp.isGranted(nodePath, Session.ACTION_READ));

        assertEquals(canReadPrimaryType, pp.isGranted(nodePath + '/' + JcrConstants.JCR_PRIMARYTYPE, Session.ACTION_READ));
        assertEquals(canReadPrimaryType, pp.isGranted(nodePath + '/' + JcrConstants.JCR_PRIMARYTYPE, Permissions.getString(Permissions.READ_PROPERTY)));

        assertFalse(pp.isGranted(subnodePath, Session.ACTION_READ));
        assertFalse(pp.isGranted(subnodePath + '/' + JcrConstants.JCR_PRIMARYTYPE, Session.ACTION_READ));
        assertFalse(pp.isGranted(subnodePath + '/' + JcrConstants.JCR_PRIMARYTYPE, Permissions.getString(Permissions.READ_PROPERTY)));
    }
}