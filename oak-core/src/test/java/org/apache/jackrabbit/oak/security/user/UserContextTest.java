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
package org.apache.jackrabbit.oak.security.user;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class UserContextTest implements UserConstants {

    private final Context ctx = UserContext.getInstance();

    private static Tree mockTree(@Nonnull String name, @Nonnull String ntName) {
        Tree t = Mockito.mock(Tree.class);
        when(t.getName()).thenReturn(name);
        when(t.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, ntName, Type.NAME));
        return t;
    }


    @Test
    public void testDefinesUserProperty() {
        Set<String> propNames = Sets.newHashSet(USER_PROPERTY_NAMES);
        propNames.removeAll(GROUP_PROPERTY_NAMES);

        for (String propName : propNames) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_USER), property));
            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_SYSTEM_USER), property));
            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_GROUP), property));
        }
    }

    @Test
    public void testDefinesGroupProperty() {
        Set<String> propNames = Sets.newHashSet(GROUP_PROPERTY_NAMES);
        propNames.removeAll(USER_PROPERTY_NAMES);

        for (String propName : propNames) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_USER), property));
            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_SYSTEM_USER), property));
            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_GROUP), property));
        }
    }

    @Test
    public void testDefinesAuthorizableProperty() {
        for (String propName : new String[] {REP_AUTHORIZABLE_ID, REP_PRINCIPAL_NAME}) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_USER), property));
            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_SYSTEM_USER), property));
            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_GROUP), property));
        }
    }

    @Test
    public void testDefinesPasswordNodeProperty() {
        for (String propName : PWD_PROPERTY_NAMES) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_PASSWORD), property));
            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_USER), property));
            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_SYSTEM_USER), property));
            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_GROUP), property));
        }
    }

    @Test
    public void testDefinesMemberRefProperty() {
        PropertyState property = PropertyStates.createProperty(REP_MEMBERS, "value");

        assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_MEMBER_REFERENCES), property));
        assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_GROUP), property));

        assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_USER), property));
        assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_SYSTEM_USER), property));
    }

    @Test
    public void testDefinesMemberProperty() {
        for (String propName : new String[] {"any", "prop"}) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            assertTrue(ctx.definesProperty(mockTree("nodeName", NT_REP_MEMBERS), property));

            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_GROUP), property));
            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_USER), property));
            assertFalse(ctx.definesProperty(mockTree("nodeName", NT_REP_SYSTEM_USER), property));
        }
    }

    @Test
    public void testNameNotDefinesProperty() {
        for (String propName : new String[] {"anyName", JcrConstants.JCR_PRIMARYTYPE}) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            for (String ntName : NT_NAMES) {
                boolean defines = NT_REP_MEMBERS.equals(ntName);
                assertEquals(defines, ctx.definesProperty(mockTree("anyName", ntName), property));
            }
        }
    }

    @Test
    public void testParentNotDefinesProperty() {
        for (String propName : Iterables.concat(USER_PROPERTY_NAMES, GROUP_PROPERTY_NAMES)) {
            PropertyState property = PropertyStates.createProperty(propName, "value");

            for (String ntName : new String[] {NodeTypeConstants.NT_OAK_UNSTRUCTURED, NT_REP_AUTHORIZABLE_FOLDER}) {
                assertFalse(ctx.definesProperty(mockTree("anyName", ntName), property));
            }
        }
    }

    @Test
    public void testDefinesContextRoot() {
        for (String ntName : NT_NAMES) {
            assertTrue(ctx.definesContextRoot(mockTree("anyName", ntName)));
        }
    }

    @Test
    public void testNotDefinesContextRoot() {
        for (String ntName : new String[] {
                JcrConstants.NT_UNSTRUCTURED,
                JcrConstants.NT_BASE,
                NodeTypeConstants.NT_REP_SYSTEM,
                NodeTypeConstants.NT_REP_ROOT,
                NT_REP_AUTHORIZABLE_FOLDER}) {
            assertFalse(ctx.definesContextRoot(mockTree("anyName", ntName)));
        }
    }

    @Test
    public void testDefinesTree() {
        for (String ntName : NT_NAMES) {
            assertTrue(ctx.definesTree(mockTree("anyName", ntName)));
        }
    }

    @Test
    public void testEmptyNotDefinesTree() {
        TreeProvider treeProvider = new TreeProviderService();
        assertFalse(ctx.definesTree(treeProvider.createReadOnlyTree(EmptyNodeState.EMPTY_NODE)));
    }

    @Test
    public void testNotDefinesTree() {
        for (String ntName : new String[] {
                JcrConstants.NT_UNSTRUCTURED,
                JcrConstants.NT_BASE,
                NodeTypeConstants.NT_REP_SYSTEM,
                NodeTypeConstants.NT_REP_ROOT,
                NT_REP_AUTHORIZABLE_FOLDER}) {
            assertFalse(ctx.definesTree(mockTree("anyName", ntName)));
        }
    }

    @Test
    public void testDefinesLocation() {
        for (String ntName : NT_NAMES) {
            Tree t = mockTree("anyName", ntName);

            TreeLocation location = Mockito.mock(TreeLocation.class);
            when(location.getTree()).thenReturn(t);
            when(location.exists()).thenReturn(true);

            assertTrue(ctx.definesLocation(location));
        }
    }

    @Test
    public void testPropertyDefinesLocation() {
        Map<String, Collection<String>> m = ImmutableMap.of(
                NT_REP_GROUP, GROUP_PROPERTY_NAMES,
                NT_REP_USER, USER_PROPERTY_NAMES,
                NT_REP_PASSWORD, PWD_PROPERTY_NAMES,
                NT_REP_MEMBER_REFERENCES, ImmutableList.of(REP_MEMBERS)
        );

        for (String ntName : m.keySet()) {
            Tree t = mockTree("anyName", ntName);

            TreeLocation location = Mockito.mock(TreeLocation.class);
            when(location.getTree()).thenReturn(t);
            when(location.exists()).thenReturn(true);

            for (String propName : m.get(ntName)) {
                PropertyState property = PropertyStates.createProperty(propName, "value");
                when(location.getProperty()).thenReturn(property);

                assertTrue(ctx.definesLocation(location));
            }

            PropertyState property = PropertyStates.createProperty("anyName", "value");
            when(location.getProperty()).thenReturn(property);
            assertFalse(ctx.definesLocation(location));
        }
    }

    @Test
    public void testNonExistingTreeDefinesLocation() {
        for (String ntName : NT_NAMES) {
            Tree t = mockTree("anyName", ntName);
            TreeLocation location = Mockito.mock(TreeLocation.class);
            when(location.getTree()).thenReturn(t);
            when(location.exists()).thenReturn(false);
            when(location.getPath()).thenReturn("/somePath");

            assertFalse(ctx.definesLocation(location));
        }
    }

    @Test
    public void testNonExistingTreeDefinesLocation2() {
        for (String name : Iterables.concat(USER_PROPERTY_NAMES, GROUP_PROPERTY_NAMES)) {
            String path = "/some/path/endingWith/reservedName/" + name;

            for (String ntName : NT_NAMES) {
                Tree t = mockTree("anyName", ntName);
                TreeLocation location = Mockito.mock(TreeLocation.class);
                when(location.getTree()).thenReturn(t);
                when(location.exists()).thenReturn(false);
                when(location.getPath()).thenReturn(path);

                assertTrue(ctx.definesLocation(location));
            }
        }
    }

    @Test
    public void testNoTreeDefinesLocation() {
        for (String name : Iterables.concat(USER_PROPERTY_NAMES, GROUP_PROPERTY_NAMES)) {
            String path = "/some/path/endingWith/reservedName/" + name;
            TreeLocation location = Mockito.mock(TreeLocation.class);
            when(location.getPath()).thenReturn(path);

            assertTrue(path, ctx.definesLocation(location));
        }
    }

    @Test
    public void testNotDefinesLocation() {
        List<String> paths = ImmutableList.of(
                PathUtils.ROOT_PATH,
                NodeTypeConstants.NODE_TYPES_PATH,
                "/content",
                ""
        );

        for (String path : paths) {
            TreeLocation location = Mockito.mock(TreeLocation.class);
            when(location.getPath()).thenReturn(path);
            assertFalse(path, ctx.definesLocation(location));
        }
    }

    @Test
    public void testDefinesInternal() {
        for (String ntName : NT_NAMES) {
            assertFalse(ctx.definesInternal(mockTree("anyName", ntName)));
        }
    }
}