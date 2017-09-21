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
package org.apache.jackrabbit.oak.jcr.version;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;

import java.util.List;

/**
 * Test OPV IGNORE
 */
public class OpvIgnoreTest extends AbstractJCRTest {

    private VersionManager versionManager;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // add child nodes that have OPV COPY and thus end up in the frozen node
        testRootNode.addNode(nodeName1, NodeTypeConstants.NT_OAK_UNSTRUCTURED).addNode(nodeName2, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        superuser.save();

        versionManager = superuser.getWorkspace().getVersionManager();
    }

    private void addIgnoredChild(@Nonnull Node node) throws Exception {
        AccessControlManager acMgr = superuser.getAccessControlManager();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, node.getPath());
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ));
        acMgr.setPolicy(acl.getPath(), acl);
        superuser.save();

        Node c = node.getNode(AccessControlConstants.REP_POLICY);
        assertEquals(OnParentVersionAction.IGNORE, c.getDefinition().getOnParentVersion());
    }

    public void testDirectChild() throws Exception {
        addIgnoredChild(testRootNode);

        testRootNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        // enforce the creation of the version (and the frozen node)
        Version version = versionManager.checkpoint(testRoot);
        Node frozen = version.getFrozenNode();

        assertTrue(frozen.hasNode(nodeName1));
        assertTrue(frozen.getNode(nodeName1).hasNode(nodeName2));
        assertFalse(frozen.hasNode(AccessControlConstants.REP_POLICY));
    }

    public void testChildInSubTree() throws Exception {
        addIgnoredChild(testRootNode.getNode(nodeName1));

        testRootNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        // enforce the creation of the version (and the frozen node)
        Version version = versionManager.checkpoint(testRoot);
        Node frozen = version.getFrozenNode();

        assertTrue(frozen.hasNode(nodeName1));
        Node frozenChild = frozen.getNode(nodeName1);
        assertTrue(frozenChild.hasNode(nodeName2));
        assertFalse(frozenChild.hasNode(AccessControlConstants.REP_POLICY));
    }

    //OAK-3328
    public void testWritePropertyWithIgnoreOPVAfterCheckIn() throws RepositoryException {
        Node ignoreTestNode = testRootNode.addNode("ignoreTestNode", JcrConstants.NT_UNSTRUCTURED);
        String ignoredPropertyName = "ignoredProperty";
        NodeTypeTemplate mixinWithIgnoreProperty = createNodeTypeWithIgnoreOPVProperty(ignoredPropertyName);

        Node node = ignoreTestNode.addNode("testNode", testNodeType);
        node.addMixin(mixinWithIgnoreProperty.getName());
        node.setProperty(ignoredPropertyName, "initial value");
        node.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        if (!node.isCheckedOut()) {
            vMgr.checkout(node.getPath());
        }
        vMgr.checkin(node.getPath());
        node.setProperty(ignoredPropertyName, "next value");
        superuser.save();
        Property ignoreProperty = node.getProperty(ignoredPropertyName);
        assertEquals("next value", ignoreProperty.getString());
    }

    //OAK-3328
    public void testRemovePropertyWithIgnoreOPVAfterCheckIn() throws RepositoryException {
        Node ignoreTestNode = testRootNode.addNode("ignoreTestNode", JcrConstants.NT_UNSTRUCTURED);
        String ignoredPropertyName = "test:ignoredProperty";
        NodeTypeTemplate mixinWithIgnoreProperty = createNodeTypeWithIgnoreOPVProperty(ignoredPropertyName);

        Node node = ignoreTestNode.addNode("testNode", testNodeType);
        node.addMixin(mixinWithIgnoreProperty.getName());
        node.setProperty(ignoredPropertyName, "initial value");
        node.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        if (!node.isCheckedOut()) {
            vMgr.checkout(node.getPath());
        }
        vMgr.checkin(node.getPath());
        node.getProperty(ignoredPropertyName).remove();
        superuser.save();
        assertFalse(node.hasProperty(ignoredPropertyName));
    }

    //OAK-3328
    public void testAddChildNodeWithIgnoreOPVAfterCheckIn() throws RepositoryException {
        Node ignoreTestNode = testRootNode.addNode("ignoreTestNode", JcrConstants.NT_UNSTRUCTURED);
        String nodeTypeName = "testOpvIgnore";
        NodeDefinitionTemplate nodeDefinition = createNodeDefinitionWithIgnoreOPVNode(nodeTypeName);

        ignoreTestNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        ignoreTestNode.addMixin(nodeTypeName);
        superuser.save();

        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        if (!ignoreTestNode.isCheckedOut()) {
            vMgr.checkout(ignoreTestNode.getPath());
        }
        vMgr.checkin(ignoreTestNode.getPath());

        Node expected = ignoreTestNode.addNode(nodeDefinition.getName(), NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        superuser.save();
        Node childNode = ignoreTestNode.getNode(nodeDefinition.getName());
        assertTrue(expected.isSame(childNode));
    }

    //OAK-3328
    public void testRemoveChildNodeWithIgnoreOPVAfterCheckIn() throws RepositoryException {
        Node ignoreTestNode = testRootNode.addNode("ignoreTestNode", JcrConstants.NT_UNSTRUCTURED);
        String nodeTypeName = "testOpvIgnore";
        NodeDefinitionTemplate nodeDefinition = createNodeDefinitionWithIgnoreOPVNode(nodeTypeName);

        ignoreTestNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        ignoreTestNode.addMixin(nodeTypeName);
        Node expected = ignoreTestNode.addNode(nodeDefinition.getName(), NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        superuser.save();

        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        if (!ignoreTestNode.isCheckedOut()) {
            vMgr.checkout(ignoreTestNode.getPath());
        }
        vMgr.checkin(ignoreTestNode.getPath());

        Node child = ignoreTestNode.getNode(nodeDefinition.getName());
        child.remove();

        superuser.save();
        assertFalse(ignoreTestNode.hasNode(nodeDefinition.getName()));
    }

    //OAK-3328
    public void testIsCheckedOutOPVIgnore() throws RepositoryException {
        Node test = testRootNode.addNode("test", JcrConstants.NT_UNSTRUCTURED);
        String nodeTypeName = "testOpvIgnore";
        NodeDefinitionTemplate nodeDefinition = createNodeDefinitionWithIgnoreOPVNode(nodeTypeName);

        test.addMixin(JcrConstants.MIX_VERSIONABLE);
        test.addMixin(nodeTypeName);
        Node ignored = test.addNode(nodeDefinition.getName(), NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        superuser.save();

        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkin(test.getPath());

        assertTrue(ignored.isCheckedOut());
        assertTrue(vMgr.isCheckedOut(ignored.getPath()));
    }

    private NodeTypeTemplate createNodeTypeWithIgnoreOPVProperty(String propertyName) throws RepositoryException {
        NodeTypeManager manager = superuser.getWorkspace().getNodeTypeManager();

        NodeTypeTemplate nt = manager.createNodeTypeTemplate();
        nt.setName("testType");
        nt.setMixin(true);
        PropertyDefinitionTemplate opt = manager.createPropertyDefinitionTemplate();
        opt.setMandatory(false);
        opt.setName(propertyName);
        opt.setRequiredType(PropertyType.STRING);
        opt.setOnParentVersion(OnParentVersionAction.IGNORE);
        List pdt = nt.getPropertyDefinitionTemplates();
        pdt.add(opt);
        manager.registerNodeType(nt, true);

        return nt;
    }

    private NodeDefinitionTemplate createNodeDefinitionWithIgnoreOPVNode(String nodeTypeName) throws RepositoryException {
        NodeTypeManager manager = superuser.getWorkspace().getNodeTypeManager();

        NodeDefinitionTemplate def = manager.createNodeDefinitionTemplate();
        def.setOnParentVersion(OnParentVersionAction.IGNORE);
        def.setName("child");
        def.setRequiredPrimaryTypeNames(new String[]{JcrConstants.NT_BASE});

        NodeTypeTemplate tmpl = manager.createNodeTypeTemplate();
        tmpl.setName(nodeTypeName);
        tmpl.setMixin(true);
        tmpl.getNodeDefinitionTemplates().add(def);
        manager.registerNodeType(tmpl, true);

        return def;
    }
}