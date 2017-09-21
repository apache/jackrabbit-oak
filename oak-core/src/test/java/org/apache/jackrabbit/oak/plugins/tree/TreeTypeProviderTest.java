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
package org.apache.jackrabbit.oak.plugins.tree;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TreeTypeProviderTest extends AbstractSecurityTest {

    private TreeTypeProvider typeProvider;

    private List<TypeTest> tests;

    @Override
    public void before() throws Exception {
        super.before();

        typeProvider = new TreeTypeProvider(getConfig(AuthorizationConfiguration.class).getContext());

        tests = new ArrayList<TypeTest>();
        tests.add(new TypeTest("/", TreeType.DEFAULT));
        tests.add(new TypeTest("/content", TreeType.DEFAULT));
        tests.add(new TypeTest('/' + JcrConstants.JCR_SYSTEM, TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH, TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:versionStorage", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:activities", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:configurations", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy/rep:Policy", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:ACL/rep:residualChildNodeDefinitions/rep:ACE", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:GrantACE/rep:namedChildNodeDefinitions/rep:restrictions", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:RepoAccessControllable/rep:namedChildNodeDefinitions/rep:repoPolicy", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:PermissionStore", TreeType.DEFAULT));

        tests.add(new TypeTest("/:hidden", TreeType.HIDDEN));
        tests.add(new TypeTest("/:hidden/child", TreeType.HIDDEN, TreeType.HIDDEN));

        tests.add(new TypeTest("/oak:index/nodetype/:index", TreeType.HIDDEN));
        tests.add(new TypeTest("/oak:index/nodetype/:index/child", TreeType.HIDDEN, TreeType.HIDDEN));

        for (String versionPath : VersionConstants.SYSTEM_PATHS) {
            tests.add(new TypeTest(versionPath, TreeType.VERSION));
            tests.add(new TypeTest(versionPath + "/a/b/child", TreeType.VERSION, TreeType.VERSION));
        }

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
    public void testGetType() {
        for (TypeTest test : tests) {
            assertEquals(test.path, test.type, typeProvider.getType(root.getTree(test.path)));
        }
    }

    @Test
    public void testGetTypeWithParentType() {
        for (TypeTest test : tests) {
            assertEquals(test.path, test.type, typeProvider.getType(root.getTree(test.path), test.parentType));
        }
    }

    @Test
    public void testGetTypeWithDefaultParentType() {
        for (TypeTest test : tests) {
            TreeType typeIfParentDefault = typeProvider.getType(root.getTree(test.path), TreeType.DEFAULT);

            if (TreeType.DEFAULT == test.parentType) {
                assertEquals(test.path, test.type, typeIfParentDefault);
            } else {
                assertNotEquals(test.path, test.type, typeIfParentDefault);
            }
        }
    }

    @Test
    public void testGetTypeForRootTree() {
        Tree t = root.getTree("/");
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t));

        // the type of the root tree is always 'DEFAULT' irrespective of the passed parent type.
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.DEFAULT));
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.HIDDEN));
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.VERSION));
    }

    @Test
    public void testGetTypeForImmutableTree() {
        for (String path : new String[] {"/", "/testPath"}) {
            Tree t = RootFactory.createReadOnlyRoot(root).getTree(path);
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t));
            // also for repeated calls
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t));

            // the type of an immutable tree is set after the first call irrespective of the passed parent type.
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.DEFAULT));
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.HIDDEN));
        }
    }

    @Test
    public void testGetTypeForImmutableTreeWithParent() {
        Tree t = RootFactory.createReadOnlyRoot(root).getTree("/:hidden/testPath");
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.HIDDEN));

        // the type of an immutable tree is set after the first call irrespective of the passed parent type.
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t));
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.DEFAULT));
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.ACCESS_CONTROL));
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.VERSION));
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
    }
}