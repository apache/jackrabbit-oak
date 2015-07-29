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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


public class PermissionsTest extends AbstractSecurityTest {

    @Test
    public void testGetPermissionsFromActions() {
        TreeLocation tl = TreeLocation.create(root.getTree("/"));
        Map<String, Long> map = ImmutableMap.of(
                Session.ACTION_READ, Permissions.READ_NODE,
                Session.ACTION_READ + "," + Session.ACTION_REMOVE, Permissions.READ_NODE|Permissions.REMOVE_NODE
        );

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            assertEquals(entry.getValue().longValue(), Permissions.getPermissions(entry.getKey(), tl, false));
        }
    }

    @Test
    public void testGetPermissionsFromPermissionNameActions() {
        TreeLocation tl = TreeLocation.create(root.getTree("/"));
        long permissions = Permissions.NODE_TYPE_MANAGEMENT|Permissions.LOCK_MANAGEMENT|Permissions.VERSION_MANAGEMENT;
        Set<String> names = Permissions.getNames(permissions);
        String jcrActions = Text.implode(names.toArray(new String[names.size()]), ",");
        assertEquals(permissions, Permissions.getPermissions(jcrActions, tl, false));
    }

    @Test
    public void testGetPermissionsFromInvalidActions() {
        TreeLocation tl = TreeLocation.create(root.getTree("/"));
        List<String> l = ImmutableList.of(
                Session.ACTION_READ + ",invalid", "invalid", "invalid," + Session.ACTION_REMOVE
        );

        for (String invalid : l) {
            try {
                Permissions.getPermissions(invalid, tl, false);
                fail();
            } catch (IllegalArgumentException e) {
                // success
            }
        }
    }

    @Test
    public void testGetPermissionsFromJackrabbitActions() {
        TreeLocation tl = TreeLocation.create(root.getTree("/"));
        Map<String, Long> map = new HashMap<String, Long>();
        map.put(Session.ACTION_ADD_NODE, Permissions.ADD_NODE);
        map.put(JackrabbitSession.ACTION_ADD_PROPERTY, Permissions.ADD_PROPERTY);
        map.put(JackrabbitSession.ACTION_MODIFY_PROPERTY, Permissions.MODIFY_PROPERTY);
        map.put(JackrabbitSession.ACTION_REMOVE_PROPERTY, Permissions.REMOVE_PROPERTY);
        map.put(JackrabbitSession.ACTION_REMOVE_NODE, Permissions.REMOVE_NODE);
        map.put(JackrabbitSession.ACTION_NODE_TYPE_MANAGEMENT, Permissions.NODE_TYPE_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_LOCKING, Permissions.LOCK_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_VERSIONING, Permissions.VERSION_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_READ_ACCESS_CONTROL, Permissions.READ_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_USER_MANAGEMENT, Permissions.USER_MANAGEMENT);

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            assertEquals(entry.getValue().longValue(), Permissions.getPermissions(entry.getKey(), tl, false));
        }
    }

    @Test
    public void testGetPermissionsOnAccessControlledNode() {
        TreeLocation tl = TreeLocation.create(root.getTree("/rep:policy"));
        Map<String, Long> map = new HashMap<String, Long>();

        // read -> mapped to read-access-control
        map.put(Session.ACTION_READ, Permissions.READ_ACCESS_CONTROL);

        // all regular write -> mapped to modify-access-control (compatible and in
        // accordance to the previous behavior, where specifying an explicit
        // modify_access_control action was not possible.
        map.put(Session.ACTION_ADD_NODE, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(Session.ACTION_REMOVE, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(Session.ACTION_SET_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_ADD_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_MODIFY_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_REMOVE_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_REMOVE_NODE, Permissions.MODIFY_ACCESS_CONTROL);

        // all other actions are mapped to the corresponding permission without
        // testing for item being ac-content
        map.put(JackrabbitSession.ACTION_READ_ACCESS_CONTROL, Permissions.READ_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_LOCKING, Permissions.LOCK_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_VERSIONING, Permissions.VERSION_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_USER_MANAGEMENT, Permissions.USER_MANAGEMENT);

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            assertEquals(entry.getKey(), entry.getValue().longValue(), Permissions.getPermissions(entry.getKey(), tl, true));
        }
    }

    @Test
    public void testActionSetProperty() {
        TreeLocation treeLocation = TreeLocation.create(root.getTree("/"));
        assertNull(treeLocation.getProperty());
        assertEquals(Permissions.ADD_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, treeLocation, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, treeLocation, true));

        TreeLocation nonExistingTree = TreeLocation.create(root.getTree("/nonExisting"));
        assertNull(nonExistingTree.getProperty());
        assertEquals(Permissions.ADD_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingTree, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingTree, true));

        TreeLocation nonExistingProp = TreeLocation.create(root, "/nonExisting");
        assertNull(nonExistingProp.getProperty());
        assertEquals(Permissions.ADD_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingProp, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingProp, true));

        TreeLocation existingProp = TreeLocation.create(root, "/jcr:primaryType");
        assertNotNull(existingProp.getProperty());
        assertEquals(Permissions.MODIFY_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, existingProp, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, existingProp, true));
    }

    @Test
    public void testActionRemove() {
        // TODO
    }

    @Test
    public void testAggregates() {
        // TODO
    }

}