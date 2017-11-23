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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test read access to version related information both in the regular
 * content and in the version storage.
 */
public class VersionTest extends AbstractCugTest implements NodeTypeConstants, VersionConstants {

    private ContentSession testSession;
    private Root testRoot;
    private ReadOnlyVersionManager versionManager;

    private List<String> readAccess = new ArrayList<String>();
    private List<String> noReadAccess = new ArrayList<String>();

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        readAccess = ImmutableList.of(
                SUPPORTED_PATH,
                "/content/subtree",
                "/content/aa");

        noReadAccess = ImmutableList.of(
                UNSUPPORTED_PATH,  /* no access */
                "/content2",       /* granted by cug only */
                "/content/a",      /* granted by ace, denied by cug */
                "/content/aa/bb"   /* granted by ace, denied by cug */
        );

        for (String path : Iterables.concat(readAccess, noReadAccess)) {
            addVersionContent(path);
        }

        testSession = createTestSession();
        testRoot = testSession.getLatestRoot();

        versionManager = ReadOnlyVersionManager.getInstance(root, NamePathMapper.DEFAULT);
    }

    @Override
    public void after() throws Exception {
        try {
            if (testSession != null) {
                testSession.close();
            }
        } finally {
            super.after();
        }
    }

    private Tree addVersionContent(@Nonnull String path) throws Exception {
        Tree t = root.getTree(path);

        Tree typesRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        TreeUtil.addMixin(t, JcrConstants.MIX_VERSIONABLE, typesRoot, null);
        root.commit();

        // force the creation of a version with frozen node
        t.setProperty(JCR_ISCHECKEDOUT, false);
        root.commit();
        t.setProperty(JCR_ISCHECKEDOUT, true);
        root.commit();

        if (testRoot != null) {
            testRoot.refresh();
        }

        return t;
    }

    @Test
    public void testReadVersionContent() throws Exception {
        IdentifierManager idMgr = new IdentifierManager(testRoot);
        ReadOnlyVersionManager vMgr = ReadOnlyVersionManager.getInstance(testRoot, NamePathMapper.DEFAULT);

        for (String path : readAccess) {
            Tree t = testRoot.getTree(path);
            assertTrue(path, t.exists());

            PropertyState ps = t.getProperty(JCR_VERSIONHISTORY);
            assertNotNull(ps);

            String vhUUID = ps.getValue(Type.STRING);
            assertEquals(vhUUID, ps.getValue(Type.STRING));

            Tree versionHistory = vMgr.getVersionHistory(t);
            assertNotNull(versionHistory);
            assertTrue(versionHistory.exists());
            assertTrue(versionHistory.getChild(JCR_ROOTVERSION).exists());
            assertFalse(versionHistory.getParent().exists());

            Tree vhTree = testRoot.getTree(versionHistory.getPath());
            assertTrue(vhTree.exists());

            String vhPath = idMgr.resolveUUID(vhUUID);
            assertNotNull(vhPath);

            assertEquals(versionHistory.getPath(), vhPath);
            assertTrue(testRoot.getTree(vhPath).exists());

            assertTrue(testRoot.getTree(vhPath + '/' + JCR_ROOTVERSION).exists());
        }
    }

    @Test
    public void testReadVersionContentNoAccess() throws Exception {
        IdentifierManager idMgr = new IdentifierManager(testRoot);

        for (String path : noReadAccess) {
            String vhUUID = checkNotNull(TreeUtil.getString(root.getTree(path), JCR_VERSIONHISTORY));
            String vhPath = PathUtils.concat(VERSION_STORE_PATH, versionManager.getVersionHistoryPath(vhUUID));

            Tree vHistory = testRoot.getTree(vhPath);
            assertFalse(vHistory.exists());
            assertFalse(vHistory.getParent().exists());
            assertFalse(vHistory.getChild(JCR_ROOTVERSION).exists());
            assertFalse(testRoot.getTree(vhPath + '/' + JCR_ROOTVERSION).exists());

            String vh = idMgr.resolveUUID(vhUUID);
            assertNull(path, vh);
        }
    }

    @Test
    public void testReadVersionStorage() {
        assertFalse(testRoot.getTree(VersionConstants.VERSION_STORE_PATH).exists());
    }

    @Test
    public void testSupportedPermissions() throws Exception {
        Tree versionable = addVersionContent("/content/a/b/c");

        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2));

        Tree versionStorage = root.getTree(VersionConstants.VERSION_STORE_PATH);
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(versionStorage, null, Permissions.READ));
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(versionStorage.getParent(), null, Permissions.READ));

        // tree with cug (access is granted)
        Tree vh = versionManager.getVersionHistory(versionable);
        assertEquals(Permissions.READ, pp.supportedPermissions(vh, null, Permissions.READ));

        // tree with cug (but no access granted)
        vh = versionManager.getVersionHistory(root.getTree("/content2"));
        assertEquals(Permissions.READ, pp.supportedPermissions(vh, null, Permissions.READ));

        // tree without cug
        vh = versionManager.getVersionHistory(root.getTree(UNSUPPORTED_PATH));
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(vh, null, Permissions.READ));

        // tree without cug
        vh = versionManager.getVersionHistory(root.getTree(SUPPORTED_PATH));
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(vh, null, Permissions.READ));
    }

    @Test
    public void testVersionableRemoved() throws Exception {
        // cug at /content/a/b/c grants access
        Tree versionable = addVersionContent("/content/a/b/c");

        Tree vh = checkNotNull(versionManager.getVersionHistory(versionable));

        assertTrue(testRoot.getTree(vh.getPath()).exists());

        versionable.remove();
        root.commit();

        // the cug-permission provider still supports the path as there exists
        // a cug higher up in the hierarchy
        // -> the parent cug takes effect now
        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2));
        assertEquals(Permissions.READ, pp.supportedPermissions(vh, null, Permissions.READ));
        assertFalse(pp.isGranted(vh, null, Permissions.READ));

        // the vh associated with /content/a/b/c is no longer accessible
        testRoot.refresh();
        assertFalse(testRoot.getTree(vh.getPath()).exists());
    }

    @Test
    public void testVersionableRemoved2() throws Exception {
        // cug at /content/a/b/c denies access
        Tree versionable = root.getTree("/content/a");

        Tree vh = checkNotNull(versionManager.getVersionHistory(versionable));

        assertFalse(testRoot.getTree(vh.getPath()).exists());

        versionable.remove();
        root.commit();

        // removing this versionable node removes the CUG in this tree
        // -> the permission provider is no longer responsible
        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2));
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(vh, null, Permissions.READ));
        assertFalse(pp.isGranted(vh, null, Permissions.READ));

        // subsequently the deny of the former CUG is gone as well
        testRoot.refresh();
        assertTrue(testRoot.getTree(vh.getPath()).exists());
    }

    @Test
    public void testTreePermissionVersionable() throws Exception {
        Tree versionable = root.getTree("/content/a");
        Tree vh = checkNotNull(versionManager.getVersionHistory(versionable));

        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2), EveryonePrincipal.getInstance());

        Tree t = root.getTree("/");
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);

        String path = PathUtils.concat(vh.getPath(), "1.0", JCR_FROZENNODE, "b/c");
        for (String segm : PathUtils.elements(path)) {
            t = t.getChild(segm);
            tp = pp.getTreePermission(t, tp);

            if (JCR_SYSTEM.equals(segm) || ReadOnlyVersionManager.isVersionStoreTree(t)) {
                assertTrue(t.getPath(), tp instanceof EmptyCugTreePermission);
            } else {
                assertTrue(t.getPath(), tp instanceof CugTreePermission);
                assertEquals(t.getPath(), "c".equals(segm), tp.canRead());
            }
        }
    }

    @Test
    public void testTreePermissionVersionable2() throws Exception {
        Tree versionable = root.getTree("/content");
        Tree vh = checkNotNull(versionManager.getVersionHistory(versionable));

        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2));

        Tree t = root.getTree("/");
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);

        String path = PathUtils.concat(vh.getPath(), "1.0", JCR_FROZENNODE, "aa");
        for (String segm : PathUtils.elements(path)) {
            t = t.getChild(segm);
            tp = pp.getTreePermission(t, tp);

            if (JCR_SYSTEM.equals(segm) || ReadOnlyVersionManager.isVersionStoreTree(t)) {
                assertTrue(t.getPath(), tp instanceof EmptyCugTreePermission);
            } else {
                assertTrue(t.getPath(), tp instanceof CugTreePermission);
            }
        }
    }

    @Test
    public void testTreePermissionVersionableUnsupportedPath() throws Exception {
        Tree versionable = root.getTree(UNSUPPORTED_PATH);
        Tree vh = checkNotNull(versionManager.getVersionHistory(versionable));

        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2));

        Tree t = root.getTree("/");
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);

        for (String segm : PathUtils.elements(vh.getPath())) {
            t = t.getChild(segm);
            tp = pp.getTreePermission(t, tp);

            if (JCR_SYSTEM.equals(segm) || ReadOnlyVersionManager.isVersionStoreTree(t)) {
                assertTrue(t.getPath(), tp instanceof EmptyCugTreePermission);
            } else {
                assertSame(t.getPath(), TreePermission.NO_RECOURSE, tp);
            }
        }
    }

    @Test
    public void testTreePermissionAtVersionableAboveSupported() throws Exception {
        Tree vh = checkNotNull(versionManager.getVersionHistory(root.getTree(SUPPORTED_PATH)));

        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH + "/a"));
        TreePermission tp = getTreePermission(root, vh.getPath(), pp);
        assertTrue(tp instanceof EmptyCugTreePermission);
    }

    @Test
    public void testCugAtRoot() throws Exception {
        Tree versionable = root.getTree(UNSUPPORTED_PATH);
        String vhPath = checkNotNull(versionManager.getVersionHistory(versionable)).getPath();

        try {
            createCug(root, PathUtils.ROOT_PATH, EveryonePrincipal.NAME);
            root.commit();

            CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of("/"));

            Tree t = root.getTree("/");
            TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
            assertTrue(tp instanceof CugTreePermission);

            for (String segm : PathUtils.elements(vhPath)) {
                t = t.getChild(segm);
                tp = pp.getTreePermission(t, tp);
                assertTrue(tp instanceof CugTreePermission);
            }
        } finally {
            root.getTree("/").removeProperty(JCR_MIXINTYPES);
            Tree cug = root.getTree("/rep:cugPolicy");
            if (cug.exists()) {
                cug.remove();
            }
            root.commit();
        }
    }

    @Test
    public void testVersionableWithUnsupportedType() throws Exception {
        Tree versionable = root.getTree("/content");
        Tree vh = checkNotNull(versionManager.getVersionHistory(versionable));
        Tree frozen = vh.getChild("1.0").getChild(JCR_FROZENNODE).getChild("a").getChild("b").getChild("c");

        Tree invalidFrozen = frozen.addChild(REP_CUG_POLICY);
        invalidFrozen.setProperty(JCR_PRIMARYTYPE, NT_REP_CUG_POLICY);

        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2));
        TreePermission tp = getTreePermission(root, PathUtils.concat(vh.getPath(), "1.0", JCR_FROZENNODE, "a/b/c"), pp);

        TreePermission tpForUnsupportedType = pp.getTreePermission(invalidFrozen, TreeType.VERSION, tp);
        assertEquals(TreePermission.NO_RECOURSE, tpForUnsupportedType);
    }

    @Test
    public void testVersionableWithCugParent() throws Exception {
        addVersionContent("/content/aa/bb/cc");

        Tree cc = root.getTree("/content/aa/bb/cc");
        assertFalse(CugUtil.hasCug(cc));

        Tree vh = checkNotNull(versionManager.getVersionHistory(cc));
        Tree t = root.getTree("/");
        CugPermissionProvider pp = createCugPermissionProvider(
                ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2), getTestGroupPrincipal());

        TreePermission tp = getTreePermission(root, vh.getPath(), pp);

        assertTrue(tp instanceof CugTreePermission);
        assertTrue(((CugTreePermission) tp).isInCug());
        assertTrue(((CugTreePermission) tp).isAllow());
    }
}