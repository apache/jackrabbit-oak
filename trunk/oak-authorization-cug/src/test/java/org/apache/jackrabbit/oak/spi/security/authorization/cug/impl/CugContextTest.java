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

import java.util.List;
import javax.jcr.AccessDeniedException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CugContextTest extends AbstractCugTest implements NodeTypeConstants {

    private static String CUG_PATH = "/content/a/rep:cugPolicy";
    private static List<String> NO_CUG_PATH = ImmutableList.of(
            "/content",
            "/content/a",
            "/content/rep:policy",
            "/content/rep:cugPolicy",
            "/content/a/rep:cugPolicy/rep:principalNames",
            UNSUPPORTED_PATH + "/rep:cugPolicy"
    );

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        // add more child nodes
        NodeUtil n = new NodeUtil(root.getTree(SUPPORTED_PATH));
        n.addChild("a", NT_OAK_UNSTRUCTURED).addChild("b", NT_OAK_UNSTRUCTURED).addChild("c", NT_OAK_UNSTRUCTURED);
        n.addChild("aa", NT_OAK_UNSTRUCTURED).addChild("bb", NT_OAK_UNSTRUCTURED).addChild("cc", NT_OAK_UNSTRUCTURED);

        // create cugs
        createCug("/content/a", getTestUser().getPrincipal());

        // setup regular acl at /content
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/content");
        acl.addAccessControlEntry(getTestUser().getPrincipal(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy("/content", acl);

        root.commit();
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
    public void testDefinesContextRoot() {
        assertTrue(CugContext.INSTANCE.definesContextRoot(root.getTree(CUG_PATH)));

        for (String path : NO_CUG_PATH) {
            assertFalse(path, CugContext.INSTANCE.definesContextRoot(root.getTree(path)));
        }
    }

    @Test
    public void testDefinesTree() {
        assertTrue(CugContext.INSTANCE.definesTree(root.getTree(CUG_PATH)));

        for (String path : NO_CUG_PATH) {
            assertFalse(path, CugContext.INSTANCE.definesTree(root.getTree(path)));
        }
    }

    @Test
    public void testDefinesProperty() {
        Tree cugTree = root.getTree(CUG_PATH);
        PropertyState repPrincipalNames = cugTree.getProperty(CugConstants.REP_PRINCIPAL_NAMES);
        assertTrue(CugContext.INSTANCE.definesProperty(cugTree, repPrincipalNames));
        assertFalse(CugContext.INSTANCE.definesProperty(cugTree, cugTree.getProperty(JcrConstants.JCR_PRIMARYTYPE)));

        for (String path : NO_CUG_PATH) {
            assertFalse(path, CugContext.INSTANCE.definesProperty(root.getTree(path), repPrincipalNames));
        }
    }

    @Test
    public void testDefinesLocation() throws AccessDeniedException {
        assertTrue(CugContext.INSTANCE.definesLocation(TreeLocation.create(root, CUG_PATH)));
        assertTrue(CugContext.INSTANCE.definesLocation(TreeLocation.create(root, CUG_PATH + "/" + CugConstants.REP_PRINCIPAL_NAMES)));

        List<String> existingNoCug = ImmutableList.of(
                "/content",
                "/content/a",
                "/content/rep:policy"
        );
        for (String path : existingNoCug) {
            assertFalse(path, CugContext.INSTANCE.definesLocation(TreeLocation.create(root, path)));
            assertFalse(path, CugContext.INSTANCE.definesLocation(TreeLocation.create(root, path + "/" + CugConstants.REP_PRINCIPAL_NAMES)));
        }

        List<String> nonExistingCug = ImmutableList.of(
                "/content/rep:cugPolicy",
                UNSUPPORTED_PATH + "/rep:cugPolicy");
        for (String path : nonExistingCug) {
            assertTrue(path, CugContext.INSTANCE.definesLocation(TreeLocation.create(root, path)));
            assertTrue(path, CugContext.INSTANCE.definesLocation(TreeLocation.create(root, path + "/" + CugConstants.REP_PRINCIPAL_NAMES)));
            assertFalse(path, CugContext.INSTANCE.definesLocation(TreeLocation.create(root, path + "/" + JcrConstants.JCR_PRIMARYTYPE)));
        }
    }

    @Test
    public void testInvalidCug() throws Exception {
        PropertyState ps = PropertyStates.createProperty(CugConstants.REP_PRINCIPAL_NAMES, ImmutableSet.of(getTestUser().getPrincipal().getName()), Type.STRINGS);

        // cug at unsupported path -> context doesn't take supported paths into account.
        Tree invalidCug = new NodeUtil(root.getTree(UNSUPPORTED_PATH)).addChild(CugConstants.REP_CUG_POLICY, CugConstants.NT_REP_CUG_POLICY).getTree();
        invalidCug.setProperty(ps);

        assertTrue(CugContext.INSTANCE.definesContextRoot(invalidCug));
        assertTrue(CugContext.INSTANCE.definesTree(invalidCug));
        assertTrue(CugContext.INSTANCE.definesProperty(invalidCug, invalidCug.getProperty(CugConstants.REP_PRINCIPAL_NAMES)));

        // 'cug' with wrong node type -> detected as no-cug by context
        invalidCug = new NodeUtil(root.getTree(UNSUPPORTED_PATH)).addChild(CugConstants.REP_CUG_POLICY, NT_OAK_UNSTRUCTURED).getTree();
        invalidCug.setProperty(ps);

        assertFalse(CugContext.INSTANCE.definesContextRoot(invalidCug));
        assertFalse(CugContext.INSTANCE.definesTree(invalidCug));
        assertFalse(CugContext.INSTANCE.definesProperty(invalidCug, invalidCug.getProperty(CugConstants.REP_PRINCIPAL_NAMES)));
    }
}