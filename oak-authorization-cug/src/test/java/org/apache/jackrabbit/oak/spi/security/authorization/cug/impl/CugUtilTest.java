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

import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractTree;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CugUtilTest extends AbstractCugTest {

    @Override
    public void before() throws Exception {
        super.before();

        createCug(SUPPORTED_PATH, EveryonePrincipal.getInstance());
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Nonnull
    private static NodeState getNodeState(@Nonnull Tree tree) {
        return ((AbstractTree) tree).getNodeState();
    }

    @Test
    public void testHasCug() throws Exception {
        assertTrue(CugUtil.hasCug(root.getTree(SUPPORTED_PATH)));

        for (String path : new String[] {PathUtils.ROOT_PATH, INVALID_PATH, UNSUPPORTED_PATH, SUPPORTED_PATH + "/subtree", SUPPORTED_PATH2, SUPPORTED_PATH3}) {
            assertFalse(CugUtil.hasCug(root.getTree(path)));
        }

        new NodeUtil(root.getTree(SUPPORTED_PATH2)).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED).getTree();
        assertTrue(CugUtil.hasCug(root.getTree(SUPPORTED_PATH2)));
    }

    @Test
    public void testHasCugNodeState() throws Exception {
        assertTrue(CugUtil.hasCug(getNodeState(root.getTree(SUPPORTED_PATH))));

        assertFalse(CugUtil.hasCug((NodeState) null));

        for (String path : new String[] {PathUtils.ROOT_PATH, INVALID_PATH, UNSUPPORTED_PATH, SUPPORTED_PATH + "/subtree", SUPPORTED_PATH2, SUPPORTED_PATH3}) {
            assertFalse(CugUtil.hasCug(getNodeState(root.getTree(path))));
        }

        new NodeUtil(root.getTree(SUPPORTED_PATH2)).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertTrue(CugUtil.hasCug(getNodeState(root.getTree(SUPPORTED_PATH2))));
    }

    @Test
    public void testHasCugNodeBuilder() throws Exception {
        assertTrue(CugUtil.hasCug(getNodeState(root.getTree(SUPPORTED_PATH)).builder()));

        assertFalse(CugUtil.hasCug((NodeBuilder) null));
        for (String path : new String[] {PathUtils.ROOT_PATH, INVALID_PATH, UNSUPPORTED_PATH, SUPPORTED_PATH + "/subtree", SUPPORTED_PATH2, SUPPORTED_PATH3}) {
            assertFalse(CugUtil.hasCug(getNodeState(root.getTree(path)).builder()));
        }

        new NodeUtil(root.getTree(SUPPORTED_PATH2)).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertTrue(CugUtil.hasCug(getNodeState(root.getTree(SUPPORTED_PATH2)).builder()));
    }

    @Test
    public void testGetCug() throws Exception {
        assertNotNull(CugUtil.getCug(root.getTree(SUPPORTED_PATH)));

        for (String path : new String[] {PathUtils.ROOT_PATH, INVALID_PATH, UNSUPPORTED_PATH, SUPPORTED_PATH + "/subtree", SUPPORTED_PATH2, SUPPORTED_PATH3}) {
            assertNull(CugUtil.getCug(root.getTree(path)));
        }

        new NodeUtil(root.getTree(SUPPORTED_PATH2)).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertNull(CugUtil.getCug(root.getTree(SUPPORTED_PATH2)));
    }

    @Test
    public void testDefinesCug() throws Exception {
        assertFalse(CugUtil.definesCug(root.getTree(PathUtils.concat(INVALID_PATH, REP_CUG_POLICY))));
        assertTrue(CugUtil.definesCug(root.getTree(PathUtils.concat(SUPPORTED_PATH, REP_CUG_POLICY))));

        Tree invalid = new NodeUtil(root.getTree(SUPPORTED_PATH2)).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED).getTree();
        assertFalse(CugUtil.definesCug(invalid));
    }

    @Test
    public void testIsSupportedPath() {
        Set<String> configuredPaths = CUG_CONFIG.getConfigValue(PARAM_CUG_SUPPORTED_PATHS, ImmutableSet.<String>of());
        assertFalse(CugUtil.isSupportedPath(null, configuredPaths));
        assertFalse(CugUtil.isSupportedPath(UNSUPPORTED_PATH, configuredPaths));

        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH, configuredPaths));
        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH2, configuredPaths));
        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH + "/child", configuredPaths));
        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH2 + "/child", configuredPaths));
    }

    @Test
    public void testGetSupportedPathsDefaultMountInfoProvider() {
        Set<String> expected = CUG_CONFIG.getConfigValue(PARAM_CUG_SUPPORTED_PATHS, ImmutableSet.<String>of());
        assertEquals(expected, CugUtil.getSupportedPaths(CUG_CONFIG, Mounts.defaultMountInfoProvider()));
    }

    @Test
    public void testGetSupportedPathsWithDifferentMounts() {
        Set<String> expected = CUG_CONFIG.getConfigValue(PARAM_CUG_SUPPORTED_PATHS, ImmutableSet.<String>of());
        MountInfoProvider mip = Mounts.newBuilder().mount("private", "/libs", "/apps", "/nonCugPath").build();
        assertNotSame(expected, CugUtil.getSupportedPaths(CUG_CONFIG, mip));
        assertEquals(expected, CugUtil.getSupportedPaths(CUG_CONFIG, mip));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetSupportedPathsMountsAtSupportedPath() {
        MountInfoProvider mip = Mounts.newBuilder().mount("private", "/libs", SUPPORTED_PATH3).build();
        CugUtil.getSupportedPaths(CUG_CONFIG, mip);
    }


    @Test(expected = IllegalStateException.class)
    public void testGetSupportedPathsMountsBelowSupportedPath() {
        MountInfoProvider mip = Mounts.newBuilder().mount("private", "/libs", "/apps" ).build();
        CugUtil.getSupportedPaths(ConfigurationParameters.of(PARAM_CUG_SUPPORTED_PATHS, new String[] {"/"}), mip);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetSupportedPathsMountsBelowSupportedPath2() {
        MountInfoProvider mip = Mounts.newBuilder().mount("private", "/libs", SUPPORTED_PATH + "/any/path/below").build();
        CugUtil.getSupportedPaths(CUG_CONFIG, mip);
    }


    @Test(expected = IllegalStateException.class)
    public void testGetSupportedPathsMountsAboveSupportedPath() {
        MountInfoProvider mip = Mounts.newBuilder().mount("private", PathUtils.ROOT_PATH).build();
        CugUtil.getSupportedPaths(CUG_CONFIG, mip);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetSupportedPathsMountsAboveSupportedPath2() {
        MountInfoProvider mip = Mounts.newBuilder().mount("private", PathUtils.getAncestorPath(SUPPORTED_PATH3, 2)).build();
        CugUtil.getSupportedPaths(CUG_CONFIG, mip);
    }

    @Test
    public void testGetImportBehavior() {
        assertSame(ImportBehavior.ABORT, CugUtil.getImportBehavior(ConfigurationParameters.EMPTY));
    }
}