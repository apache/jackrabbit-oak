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

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
    public void testAggregates() {
        // TODO
    }

}