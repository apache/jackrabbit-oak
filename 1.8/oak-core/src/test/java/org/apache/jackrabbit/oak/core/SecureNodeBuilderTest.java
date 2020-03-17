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
package org.apache.jackrabbit.oak.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.core.TestPermissionProvider.NAME_ACCESSIBLE;
import static org.apache.jackrabbit.oak.core.TestPermissionProvider.NAME_NON_ACCESSIBLE;
import static org.apache.jackrabbit.oak.core.TestPermissionProvider.NAME_NON_EXISTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SecureNodeBuilderTest {

    private final NodeStore store = new MemoryNodeStore();
    private final TestPermissionProvider permissionProvider = new TestPermissionProvider();

    private SecureNodeBuilder secureNodeBuilder;

    @Before
    public void before() throws Exception {
        NodeBuilder rootBuilder = store.getRoot().builder();
        rootBuilder.setProperty("prop", "value").setProperty(NAME_NON_ACCESSIBLE, "value");
        rootBuilder.child(NAME_ACCESSIBLE).setProperty("prop", "value").setProperty(NAME_NON_ACCESSIBLE, "value");
        rootBuilder.child(NAME_NON_ACCESSIBLE).setProperty("prop", "value").setProperty(NAME_NON_ACCESSIBLE, "value");
        store.merge(rootBuilder, new EmptyHook(), new CommitInfo("id", null));

        rootBuilder = store.getRoot().builder();
        secureNodeBuilder = new SecureNodeBuilder(rootBuilder, new LazyValue<PermissionProvider>() {
            @Override
            protected PermissionProvider createValue() {
                return permissionProvider;
            }
        });
    }

    @Test
    public void testGetChildNodeNonExisting() {
        NodeBuilder child = secureNodeBuilder.getChildNode(NAME_NON_EXISTING);
        assertTrue(child instanceof SecureNodeBuilder);
    }

    @Test
    public void testGetChildNode() {
        NodeBuilder child = secureNodeBuilder.getChildNode(NAME_ACCESSIBLE);
        assertTrue(child instanceof SecureNodeBuilder);
        assertTrue(child.exists());
    }

    @Test
    public void testGetChildNodeNonAccessible() {
        NodeBuilder child = secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE);
        assertTrue(child instanceof SecureNodeBuilder);
    }

    @Test
    public void testExists() {
        assertTrue(secureNodeBuilder.exists());
    }

    @Test
    public void testExistsNonExisting() {
        assertFalse(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).exists());
    }

    @Test
    public void testExistsAccessible() {
        assertTrue(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).exists());
    }

    @Test
    public void testExistsNonAccessible() {
        assertFalse(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).exists());
    }

    @Test
    public void testGetBaseState() {
        NodeState ns = secureNodeBuilder.getBaseState();

        assertTrue(ns instanceof SecureNodeState);
    }

    @Test
    public void testGetBaseStateNonExisting() {
        NodeState ns = secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getBaseState();

        assertTrue(ns instanceof SecureNodeState);
    }

    @Test
    public void testGetBaseStateNonAccessible() {
        NodeState ns = secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getBaseState();

        assertTrue(ns instanceof SecureNodeState);
    }

    @Test
    public void testGetNodeState() {
        NodeState ns = secureNodeBuilder.getNodeState();

        assertTrue(ns instanceof SecureNodeState);
    }

    @Test
    public void testGetNodeStateNonExisting() {
        NodeState ns = secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getNodeState();

        assertTrue(ns instanceof SecureNodeState);
    }

    @Test
    public void testGetNodeStateNonAccessible() {
        NodeState ns = secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getNodeState();

        assertTrue(ns instanceof SecureNodeState);
    }

    /**
     * Illustrating usage of {@link SecureNodeBuilder#baseChanged()} as it is
     * currently present with {@link MutableRoot#commit()}, {@link MutableRoot#rebase()}
     * and {@link MutableRoot#refresh()}: baseChanged is call <strong>before</strong>
     * the permission provider is refreshed, which depending on the implementation
     * may lead to a stale {@code TreePermission} being obtained with the non-lazy
     * refresh as described in OAK-5355.
     *
     * @see <a href"https://issues.apache.org/jira/browse/OAK-5355">OAK-5355</a>
     */
    @Test
    public void testBaseChanged() {
        secureNodeBuilder.baseChanged();
        // change the behavior of the permission provider to assure that subsequent
        // calls are always reading the most up to date information
        try {
            permissionProvider.denyAll = true;
            assertFalse(secureNodeBuilder.exists());
            assertFalse(secureNodeBuilder.hasChildNode(NAME_ACCESSIBLE));
            assertFalse(secureNodeBuilder.hasProperty("prop"));
            assertFalse(secureNodeBuilder.isNew());
            assertFalse(secureNodeBuilder.getBaseState().exists());
            assertFalse(secureNodeBuilder.getNodeState().exists());
        } finally {
            permissionProvider.denyAll = false;
        }
    }

    @Test
    public void testHasProperty() {
        assertTrue(secureNodeBuilder.hasProperty("prop"));
        assertTrue(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).hasProperty("prop"));
        assertTrue(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).hasProperty("prop"));
        assertFalse(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).hasProperty("prop"));
    }

    @Test
    public void testHasPropertyNonAccessible() {
        assertFalse(secureNodeBuilder.hasProperty(NAME_NON_ACCESSIBLE));
        assertFalse(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).hasProperty(NAME_NON_ACCESSIBLE));
        assertFalse(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).hasProperty(NAME_NON_ACCESSIBLE));
        assertFalse(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).hasProperty(NAME_NON_ACCESSIBLE));
    }

    @Test
    public void testHasPropertyNonExisting() {
        assertFalse(secureNodeBuilder.hasProperty(NAME_NON_EXISTING));
        assertFalse(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).hasProperty(NAME_NON_EXISTING));
        assertFalse(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).hasProperty(NAME_NON_EXISTING));
        assertFalse(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).hasProperty(NAME_NON_EXISTING));
    }

    @Test
    public void testGetProperty() {
        assertNotNull(secureNodeBuilder.getProperty("prop"));
        assertNotNull(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).getProperty("prop"));
        assertNotNull(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getProperty("prop"));
        assertNull(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getProperty("prop"));
    }

    @Test
    public void testGetPropertyNonAccessible() {
        assertNull(secureNodeBuilder.getProperty(NAME_NON_ACCESSIBLE));
        assertNull(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).getProperty(NAME_NON_ACCESSIBLE));
        assertNull(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getProperty(NAME_NON_ACCESSIBLE));
        assertNull(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getProperty(NAME_NON_ACCESSIBLE));
    }

    @Test
    public void testGetPropertyNonExisting() {
        assertNull(secureNodeBuilder.getProperty(NAME_NON_EXISTING));
        assertNull(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).getProperty(NAME_NON_EXISTING));
        assertNull(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getProperty(NAME_NON_EXISTING));
        assertNull(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getProperty(NAME_NON_EXISTING));
    }

    @Test
    public void testGetPropertyCount() {
        assertEquals(1, secureNodeBuilder.getPropertyCount());
        assertEquals(1, secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).getPropertyCount());
        assertEquals(1, secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getPropertyCount());
        assertEquals(0, secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getPropertyCount());
    }

    @Test
    public void testGetPropertyCountCanReadProperties() {
        try {
            permissionProvider.canReadProperties = true;
            assertEquals(2, secureNodeBuilder.getPropertyCount());
            assertEquals(2, secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).getPropertyCount());
            assertEquals(2, secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getPropertyCount());
            assertEquals(0, secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getPropertyCount());
        } finally {
            permissionProvider.canReadProperties = false;
        }
    }

    @Test
    public void testGetProperties() {
        assertEquals(1, Iterables.size(secureNodeBuilder.getProperties()));
        assertEquals(1, Iterables.size(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).getProperties()));
        assertEquals(1, Iterables.size(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getProperties()));
        assertEquals(0, Iterables.size(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getProperties()));
    }

    @Test
    public void testGetPropertiesCanReadProperties() {
        try {
            permissionProvider.canReadProperties = true;
            assertEquals(2, Iterables.size(secureNodeBuilder.getProperties()));
            assertEquals(2, Iterables.size(secureNodeBuilder.getChildNode(NAME_ACCESSIBLE).getProperties()));
            assertEquals(2, Iterables.size(secureNodeBuilder.getChildNode(NAME_NON_ACCESSIBLE).getProperties()));
            assertEquals(0, Iterables.size(secureNodeBuilder.getChildNode(NAME_NON_EXISTING).getProperties()));
        } finally {
            permissionProvider.canReadProperties = false;
        }
    }

    @Test
    public void testGetPropertiesAfterSet() {
        secureNodeBuilder.setProperty(PropertyStates.createProperty("another", ImmutableList.of("v", "v2"), Type.STRINGS));
        assertEquals(2, Iterables.size(secureNodeBuilder.getProperties()));
    }

    @Test
    public void testGetPropertiesAfterRemoval() {
        secureNodeBuilder.removeProperty("prop");
        assertEquals(0, Iterables.size(secureNodeBuilder.getProperties()));
    }

    @Test
    public void testGetBoolean() {
        assertFalse(secureNodeBuilder.getBoolean("prop"));
        assertFalse(secureNodeBuilder.getBoolean(NAME_NON_EXISTING));
        assertFalse(secureNodeBuilder.getBoolean(NAME_NON_ACCESSIBLE));
    }

    @Test
    public void testGetBooleanTypeBoolean() {
        secureNodeBuilder.setProperty("boolean", true, Type.BOOLEAN);
        assertTrue(secureNodeBuilder.getBoolean("boolean"));
    }

    @Test
    public void testGetBooleanTypeBooleans() {
        secureNodeBuilder.setProperty("booleans", ImmutableList.of(true, false), Type.BOOLEANS);
        assertFalse(secureNodeBuilder.getBoolean("booleans"));
    }

    @Test
    public void testGetString() {
        assertEquals("value", secureNodeBuilder.getString("prop"));
        assertNull(secureNodeBuilder.getString(NAME_NON_EXISTING));
        assertNull(secureNodeBuilder.getString(NAME_NON_ACCESSIBLE));
    }

    @Test
    public void testGetStringTypeStrings() {
        secureNodeBuilder.setProperty("strings", ImmutableList.of("a", "b"), Type.STRINGS);
        assertNull(secureNodeBuilder.getString("strings"));
    }

    @Test
    public void testGetStringTypeLong() {
        secureNodeBuilder.setProperty("long", Long.MAX_VALUE, Type.LONG);
        assertNull(secureNodeBuilder.getString("long"));
    }

    @Test
    public void testGetName() {
        assertNull(secureNodeBuilder.getName("prop"));
        assertNull(secureNodeBuilder.getName(NAME_NON_EXISTING));
        assertNull(secureNodeBuilder.getName(NAME_NON_ACCESSIBLE));
    }

    @Test
    public void testGetNameTypeNames() {
        secureNodeBuilder.setProperty("names", ImmutableList.of("a", "b"), Type.NAMES);
        assertNull(secureNodeBuilder.getName("names"));
    }

    @Test
    public void testGetNameTypeName() {
        secureNodeBuilder.setProperty("name", "value", Type.NAME);
        assertEquals("value", secureNodeBuilder.getName("name"));
    }

    @Test
    public void testGetNames() {
        assertEquals(0, Iterables.size(secureNodeBuilder.getNames("prop")));
        assertEquals(0, Iterables.size(secureNodeBuilder.getNames(NAME_NON_EXISTING)));
        assertEquals(0, Iterables.size(secureNodeBuilder.getNames(NAME_NON_ACCESSIBLE)));
    }

    @Test
    public void testGetNamesTypeNames() {
        Iterable<String> names = ImmutableList.of("a", "b");
        secureNodeBuilder.setProperty("names", names, Type.NAMES);
        assertTrue(Iterables.elementsEqual(names, secureNodeBuilder.getNames("names")));
    }

    @Test
    public void testGetNamesTypeName() {
        secureNodeBuilder.setProperty("name", "value", Type.NAME);
        assertEquals(0, Iterables.size(secureNodeBuilder.getNames("name")));
    }

    @Test
    public void testSetProperty() {
        secureNodeBuilder.setProperty("another", "value");
        assertTrue(secureNodeBuilder.hasProperty("another"));
    }

    @Test
    public void testSetPropertyReturnsSame() {
        assertSame(secureNodeBuilder, secureNodeBuilder.setProperty("another", "value"));
    }

    @Test
    public void testSetPropertyModifiedNonAccessible() {
        secureNodeBuilder.setProperty(NAME_NON_ACCESSIBLE, "anothervalue");
        // modified property must exist irrespective of permission eval
        assertTrue(secureNodeBuilder.hasProperty(NAME_NON_ACCESSIBLE));
    }

    @Test
    public void testSetPropertyNewNonAccessible() {
        String name = NAME_NON_ACCESSIBLE+"-new";
        secureNodeBuilder.setProperty(name, "value");
        // new property must exist irrespective of permission eval
        assertTrue(secureNodeBuilder.hasProperty(name));
    }

    @Test
    public void testRemoveProperty() {
        secureNodeBuilder.removeProperty("prop");
        assertFalse(secureNodeBuilder.hasProperty("prop"));
    }

    @Test
    public void testRemovePropertyNonAccessible() {
        secureNodeBuilder.removeProperty(NAME_NON_ACCESSIBLE);

        // verify that property has not been removed
        try {
            permissionProvider.canReadProperties = true;
            assertTrue(secureNodeBuilder.hasProperty(NAME_NON_ACCESSIBLE));
        } finally {
            permissionProvider.canReadProperties = false;
        }
    }

    @Test
    public void testRemovePropertyReturnsSame() {
        assertSame(secureNodeBuilder, secureNodeBuilder.removeProperty("prop"));
    }

    @Test
    public void testHasChildNode() {
        assertTrue(secureNodeBuilder.hasChildNode(NAME_ACCESSIBLE));
    }

    @Test
    public void testHasChildNodeNonAccessible() {
        assertFalse(secureNodeBuilder.hasChildNode(NAME_NON_ACCESSIBLE));
    }

    @Test
    public void testHasChildNodeNonExisting() {
        assertFalse(secureNodeBuilder.hasChildNode(NAME_NON_EXISTING));
    }

    @Test
    public void testGetChildNodeCount() {
        assertEquals(1, secureNodeBuilder.getChildNodeCount(Long.MAX_VALUE));
    }

    @Test
    public void testGetChildNodeCountCanReadAll() {
        try {
            permissionProvider.canReadAll = true;
            assertEquals(2, secureNodeBuilder.getChildNodeCount(Long.MAX_VALUE));
        } finally {
            permissionProvider.canReadAll = false;
        }
    }

}