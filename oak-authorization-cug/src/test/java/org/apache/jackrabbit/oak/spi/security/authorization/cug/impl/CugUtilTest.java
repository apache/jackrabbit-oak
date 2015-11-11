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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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

    @Test
    public void testHasCug() throws Exception {
        assertFalse(CugUtil.hasCug(root.getTree("/")));
        assertFalse(CugUtil.hasCug(root.getTree(INVALID_PATH)));
        assertFalse(CugUtil.hasCug(root.getTree(UNSUPPORTED_PATH)));
        assertFalse(CugUtil.hasCug(root.getTree(SUPPORTED_PATH + "/subtree")));
        assertFalse(CugUtil.hasCug(root.getTree(SUPPORTED_PATH2)));

        assertTrue(CugUtil.hasCug(root.getTree(SUPPORTED_PATH)));

        new NodeUtil(root.getTree(SUPPORTED_PATH2)).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED).getTree();
        assertTrue(CugUtil.hasCug(root.getTree(SUPPORTED_PATH2)));
    }

    @Test
    public void testGetCug() throws Exception {
        assertNull(CugUtil.getCug(root.getTree("/")));
        assertNull(CugUtil.getCug(root.getTree(INVALID_PATH)));
        assertNull(CugUtil.getCug(root.getTree(UNSUPPORTED_PATH)));
        assertNull(CugUtil.getCug(root.getTree(SUPPORTED_PATH + "/subtree")));
        assertNull(CugUtil.getCug(root.getTree(SUPPORTED_PATH2)));

        assertNotNull(CugUtil.getCug(root.getTree(SUPPORTED_PATH)));

        Tree invalid = new NodeUtil(root.getTree(SUPPORTED_PATH2)).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED).getTree();
        assertNull(CugUtil.getCug(invalid));
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
        assertFalse(CugUtil.isSupportedPath(null, CUG_CONFIG));
        assertFalse(CugUtil.isSupportedPath(UNSUPPORTED_PATH, CUG_CONFIG));

        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH, CUG_CONFIG));
        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH2, CUG_CONFIG));
        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH + "/child", CUG_CONFIG));
        assertTrue(CugUtil.isSupportedPath(SUPPORTED_PATH2 + "/child", CUG_CONFIG));
    }

    @Test
    public void testGetImportBehavior() {
        assertSame(ImportBehavior.ABORT, CugUtil.getImportBehavior(ConfigurationParameters.EMPTY));
    }
}