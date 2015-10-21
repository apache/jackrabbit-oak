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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.apache.jackrabbit.oak.security.authorization.permission.TreeTypeProvider.TYPE_AC;
import static org.apache.jackrabbit.oak.security.authorization.permission.TreeTypeProvider.TYPE_DEFAULT;
import static org.apache.jackrabbit.oak.security.authorization.permission.TreeTypeProvider.TYPE_HIDDEN;
import static org.apache.jackrabbit.oak.security.authorization.permission.TreeTypeProvider.TYPE_INTERNAL;
import static org.apache.jackrabbit.oak.security.authorization.permission.TreeTypeProvider.TYPE_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TreeTypeProviderTest extends AbstractSecurityTest {

    private TreeTypeProvider typeProvider;
    private List<TreeType> treeTypes;

    @Override
    public void before() throws Exception {
        super.before();

        typeProvider = new TreeTypeProvider(getConfig(AuthorizationConfiguration.class).getContext());

        treeTypes = new ArrayList<TreeType>();
        treeTypes.add(new TreeType("/", TYPE_DEFAULT));
        treeTypes.add(new TreeType("/content", TYPE_DEFAULT));
        treeTypes.add(new TreeType('/' + JcrConstants.JCR_SYSTEM, TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH, TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:versionStorage", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:activities", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:configurations", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:AccessControllable/rep:namedChildNodeDefinitions/rep:policy/rep:Policy", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:ACL/rep:residualChildNodeDefinitions/rep:ACE", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:GrantACE/rep:namedChildNodeDefinitions/rep:restrictions", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:RepoAccessControllable/rep:namedChildNodeDefinitions/rep:repoPolicy", TYPE_DEFAULT));
        treeTypes.add(new TreeType(NodeTypeConstants.NODE_TYPES_PATH + "/rep:PermissionStore", TYPE_DEFAULT));

        treeTypes.add(new TreeType("/:hidden", TYPE_HIDDEN));
        treeTypes.add(new TreeType("/:hidden/child", TYPE_HIDDEN, TYPE_HIDDEN));

        treeTypes.add(new TreeType("/oak:index/nodetype/:index", TYPE_HIDDEN));
        treeTypes.add(new TreeType("/oak:index/nodetype/:index/child", TYPE_HIDDEN, TYPE_HIDDEN));

        for (String versionPath : VersionConstants.SYSTEM_PATHS) {
            treeTypes.add(new TreeType(versionPath, TYPE_VERSION));
            treeTypes.add(new TreeType(versionPath + "/a/b/child", TYPE_VERSION, TYPE_VERSION));
        }

        treeTypes.add(new TreeType(PermissionConstants.PERMISSIONS_STORE_PATH, TYPE_INTERNAL));
        treeTypes.add(new TreeType(PermissionConstants.PERMISSIONS_STORE_PATH + "/a/b/child", TYPE_INTERNAL, TYPE_INTERNAL));

        NodeUtil testTree = new NodeUtil(root.getTree("/")).addChild("test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        for (String name : AccessControlConstants.POLICY_NODE_NAMES) {
            NodeUtil acl = testTree.addChild(name, AccessControlConstants.NT_REP_ACL);
            treeTypes.add(new TreeType(acl.getTree().getPath(), TYPE_AC));

            NodeUtil ace = acl.addChild("ace", AccessControlConstants.NT_REP_DENY_ACE);
            treeTypes.add(new TreeType(ace.getTree().getPath(), TYPE_AC, TYPE_AC));

            NodeUtil ace2 = acl.addChild("ace2", AccessControlConstants.NT_REP_GRANT_ACE);
            treeTypes.add(new TreeType(ace2.getTree().getPath(), TYPE_AC, TYPE_AC));

            NodeUtil rest = ace2.addChild(AccessControlConstants.REP_RESTRICTIONS, AccessControlConstants.NT_REP_RESTRICTIONS);
            treeTypes.add(new TreeType(rest.getTree().getPath(), TYPE_AC, TYPE_AC));

            NodeUtil invalid = rest.addChild("invalid", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            treeTypes.add(new TreeType(invalid.getTree().getPath(), TYPE_AC, TYPE_AC));
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
        for (TreeType treeType : treeTypes) {
            assertEquals(treeType.path, treeType.type, typeProvider.getType(root.getTree(treeType.path)));
        }
    }

    @Test
    public void testGetTypeWithParentType() {
        for (TreeType treeType : treeTypes) {
            assertEquals(treeType.path, treeType.type, typeProvider.getType(root.getTree(treeType.path), treeType.parentType));
        }
    }

    @Test
    public void testGetTypeWithDefaultParentType() {
        for (TreeType treeType : treeTypes) {
            int typeIfParentDefault = typeProvider.getType(root.getTree(treeType.path), TYPE_DEFAULT);

            if (TYPE_DEFAULT == treeType.parentType) {
                assertEquals(treeType.path, treeType.type, typeIfParentDefault);
            } else {
                assertNotEquals(treeType.path, treeType.type, typeIfParentDefault);
            }
        }
    }
    
    private static final class TreeType {

        private final String path;
        private final int type;
        private final int parentType;

        private TreeType(@Nonnull String path, int type) {
            this(path, type, TreeTypeProvider.TYPE_DEFAULT);
        }
        private TreeType(@Nonnull String path, int type, int parentType) {
            this.path = path;
            this.type = type;
            this.parentType = parentType;
        }
    }
}