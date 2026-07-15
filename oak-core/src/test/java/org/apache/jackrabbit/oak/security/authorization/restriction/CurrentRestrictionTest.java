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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import java.util.Collections;
import java.util.Map;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ADD_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CurrentRestrictionTest extends AbstractRestrictionTest {

    @Override
    boolean addEntry(@NotNull JackrabbitAccessControlList acl) {
        return false;
    }

    @Test
    public void testDefinedProperties() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a/d/b/e/c");
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ), true,
                Collections.emptyMap(),
                Map.of(AccessControlConstants.REP_CURRENT, new Value[] {
                        vf.createValue(JCR_PRIMARYTYPE),
                        vf.createValue(JCR_MIXINTYPES)}));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        Root testRoot = testSession.getLatestRoot();
        assertFalse(testRoot.getTree("/a").exists());
        Tree t = testRoot.getTree("/a/d/b/e/c");
        assertTrue(t.exists());
        assertTrue(t.hasProperty(JCR_PRIMARYTYPE));
        assertTrue(t.hasProperty(JCR_MIXINTYPES));
        assertFalse(t.hasProperty("prop"));
        assertFalse(t.hasProperty("a"));
        assertEquals(2, t.getPropertyCount());
        assertEquals(0, t.getChildrenCount(1));
    }

    @Test
    public void testNoProperties() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a/d/b/e/c");
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ), true,
                Collections.emptyMap(),
                Collections.singletonMap(AccessControlConstants.REP_CURRENT, new Value[0]));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        Root testRoot = testSession.getLatestRoot();
        assertFalse(testRoot.getTree("/a").exists());
        Tree t = testRoot.getTree("/a/d/b/e/c");
        assertTrue(t.exists());
        assertEquals(0, t.getPropertyCount());
        assertEquals(0, t.getChildrenCount(1));
    }
    
    @Test
    public void testAllProperties() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a/d/b/e/c");
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ), true, 
                Collections.emptyMap(),
                Collections.singletonMap(AccessControlConstants.REP_CURRENT, new Value[] {vf.createValue(NodeTypeConstants.RESIDUAL_NAME)
                }));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();
        
        Root testRoot = testSession.getLatestRoot();
        assertFalse(testRoot.getTree("/a").exists());
        Tree t = testRoot.getTree("/a/d/b/e/c");
        assertTrue(t.exists());
        assertTrue(t.hasProperty(JCR_PRIMARYTYPE));
        assertTrue(t.hasProperty(JCR_MIXINTYPES));
        assertTrue(t.hasProperty("prop"));
        assertTrue(t.hasProperty("a"));
        assertFalse(t.hasChild("f"));
        assertEquals(4, t.getPropertyCount());
        assertEquals(0, t.getChildrenCount(1));
    }
    
    @Test
    public void testSetProperties() throws Exception {
        String propertyName = "prop";
        PropertyState prop = PropertyStates.createProperty(propertyName, "value");
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a");
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ), true);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_MODIFY_PROPERTIES), true,
                Collections.emptyMap(),
                Collections.singletonMap(AccessControlConstants.REP_CURRENT, new Value[] {vf.createValue(propertyName)}));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        Root testRoot = testSession.getLatestRoot();
        
        // on /a added a property 'prop' must be allowed
        Tree a = testRoot.getTree("/a");
        assertTrue(a.exists());
        a.setProperty(prop);
        testRoot.commit();

        // for any other property name jcr:modifyProperties is not granted
        try {
            a.setProperty(PropertyStates.createProperty("another", "value"));
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
        
        // nor is it jcr:modifyProperties granted on another node
        Tree c = testRoot.getTree("/a/d/b/e/c");
        assertTrue(c.exists());
        try {
            c.setProperty(prop);
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
    }

    @Test
    public void testAddChildNodes() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a");
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ), true);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_ADD_CHILD_NODES), true,
                Collections.emptyMap(),
                // NOTE: specifying property names doesn't make sense for jcr:addChildNode privilege
                Collections.singletonMap(AccessControlConstants.REP_CURRENT, new Value[0]));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        Root testRoot = testSession.getLatestRoot();

        // on /a adding a child node is allowed
        Tree a = testRoot.getTree("/a");
        assertTrue(a.exists());
        TreeUtil.addChild(a, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        testRoot.commit();

        // however, jcr:addChildNodes is not granted in the subtree
        Tree c = testRoot.getTree("/a/d/b/e/c");
        assertTrue(c.exists());
        try {
            TreeUtil.addChild(c, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
    }
}
