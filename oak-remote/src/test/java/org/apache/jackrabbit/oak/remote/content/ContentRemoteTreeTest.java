/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.content;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.remote.RemoteTree;
import org.apache.jackrabbit.oak.remote.RemoteTreeFilters;
import org.apache.jackrabbit.oak.remote.RemoteValue;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ContentRemoteTreeTest {

    private ContentRemoteTree createTree(Tree tree) {
        return new ContentRemoteTree(tree, 0, new RemoteTreeFilters(), mock(ContentRemoteBinaries.class));
    }

    private ContentRemoteTree createTree(Tree tree, ContentRemoteBinaries binaries) {
        return new ContentRemoteTree(tree, 0, new RemoteTreeFilters(), binaries);
    }

    private ContentRemoteTree createTree(Tree tree, RemoteTreeFilters filters) {
        return new ContentRemoteTree(tree, 0, filters, mock(ContentRemoteBinaries.class));
    }

    @Test
    public void testGetBinaryProperty() {
        InputStream stream = mock(InputStream.class);

        Blob blob = mock(Blob.class);
        doReturn(stream).when(blob).getNewStream();

        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.BINARY).when(property).getType();
        doReturn(blob).when(property).getValue(Type.BINARY);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public long getBinaryThreshold() {
                return Long.MAX_VALUE;
            }

        });

        Map<String, RemoteValue> properties = remoteTree.getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isBinary());
        assertEquals(stream, properties.get("name").asBinary().get());
    }

    @Test
    public void testGetMultiBinaryProperty() {
        InputStream stream = mock(InputStream.class);

        Blob blob = mock(Blob.class);
        doReturn(stream).when(blob).getNewStream();

        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.BINARIES).when(property).getType();
        doReturn(singletonList(blob)).when(property).getValue(Type.BINARIES);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public long getBinaryThreshold() {
                return Long.MAX_VALUE;
            }

        });

        Map<String, RemoteValue> properties = remoteTree.getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiBinary());
        assertEquals(stream, getOnlyElement(properties.get("name").asMultiBinary()).get());
    }

    @Test
    public void testGetBinaryIdProperty() {
        Blob blob = mock(Blob.class);

        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.BINARY).when(property).getType();
        doReturn(blob).when(property).getValue(Type.BINARY);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn("id").when(binaries).put(blob);

        Map<String, RemoteValue> properties = createTree(tree, binaries).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isBinaryId());
        assertEquals("id", properties.get("name").asBinaryId());
    }

    @Test
    public void testGetMultiBinaryIdProperty() {
        Blob blob = mock(Blob.class);

        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.BINARIES).when(property).getType();
        doReturn(singletonList(blob)).when(property).getValue(Type.BINARIES);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn("id").when(binaries).put(blob);

        Map<String, RemoteValue> properties = createTree(tree, binaries).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiBinaryId());
        assertEquals("id", getOnlyElement(properties.get("name").asMultiBinaryId()));
    }

    @Test
    public void testGetBooleanProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.BOOLEAN).when(property).getType();
        doReturn(true).when(property).getValue(Type.BOOLEAN);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isBoolean());
        assertEquals(true, properties.get("name").asBoolean());
    }

    @Test
    public void testGetMultiBooleanProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.BOOLEANS).when(property).getType();
        doReturn(singletonList(true)).when(property).getValue(Type.BOOLEANS);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiBoolean());
        assertEquals(true, getOnlyElement(properties.get("name").asMultiBoolean()));
    }

    @Test
    public void testGetDateProperty() {
        Calendar calendar = Calendar.getInstance();

        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.DATE).when(property).getType();
        doReturn(ISO8601.format(calendar)).when(property).getValue(Type.DATE);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isDate());
        assertEquals(calendar.getTimeInMillis(), properties.get("name").asDate().longValue());
    }

    @Test
    public void testGetMultiDateProperty() {
        Calendar calendar = Calendar.getInstance();

        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.DATES).when(property).getType();
        doReturn(singletonList(ISO8601.format(calendar))).when(property).getValue(Type.DATES);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiDate());
        assertEquals(calendar.getTimeInMillis(), getOnlyElement(properties.get("name").asMultiDate()).longValue());
    }

    @Test
    public void testDecimalProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.DECIMAL).when(property).getType();
        doReturn(BigDecimal.ONE).when(property).getValue(Type.DECIMAL);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isDecimal());
        assertEquals(BigDecimal.ONE, properties.get("name").asDecimal());
    }

    @Test
    public void testGetMultiDecimalProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.DECIMALS).when(property).getType();
        doReturn(singletonList(BigDecimal.ONE)).when(property).getValue(Type.DECIMALS);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiDecimal());
        assertEquals(BigDecimal.ONE, getOnlyElement(properties.get("name").asMultiDecimal()));
    }

    @Test
    public void testDoubleProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.DOUBLE).when(property).getType();
        doReturn(4.2).when(property).getValue(Type.DOUBLE);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isDouble());
        assertEquals(4.2, properties.get("name").asDouble(), 1e-9);
    }

    @Test
    public void testGetMultiDoubleProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.DOUBLES).when(property).getType();
        doReturn(singletonList(4.2)).when(property).getValue(Type.DOUBLES);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiDouble());
        assertEquals(4.2, getOnlyElement(properties.get("name").asMultiDouble()), 1e-9);
    }

    @Test
    public void testLongProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.LONG).when(property).getType();
        doReturn(42L).when(property).getValue(Type.LONG);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isLong());
        assertEquals(42L, properties.get("name").asLong().longValue());
    }

    @Test
    public void testGetMultiLongProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.LONGS).when(property).getType();
        doReturn(singletonList(42L)).when(property).getValue(Type.LONGS);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiLong());
        assertEquals(42L, getOnlyElement(properties.get("name").asMultiLong()).longValue());
    }

    @Test
    public void testNameProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.NAME).when(property).getType();
        doReturn("value").when(property).getValue(Type.NAME);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isName());
        assertEquals("value", properties.get("name").asName());
    }

    @Test
    public void testGetMultiNameProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.NAMES).when(property).getType();
        doReturn(singletonList("value")).when(property).getValue(Type.NAMES);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiName());
        assertEquals("value", getOnlyElement(properties.get("name").asMultiName()));
    }

    @Test
    public void testPathProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.PATH).when(property).getType();
        doReturn("value").when(property).getValue(Type.PATH);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isPath());
        assertEquals("value", properties.get("name").asPath());
    }

    @Test
    public void testGetMultiPathProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.PATHS).when(property).getType();
        doReturn(singletonList("value")).when(property).getValue(Type.PATHS);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiPath());
        assertEquals("value", getOnlyElement(properties.get("name").asMultiPath()));
    }

    @Test
    public void testReferenceProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.REFERENCE).when(property).getType();
        doReturn("value").when(property).getValue(Type.REFERENCE);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isReference());
        assertEquals("value", properties.get("name").asReference());
    }

    @Test
    public void testGetMultiReferenceProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.REFERENCES).when(property).getType();
        doReturn(singletonList("value")).when(property).getValue(Type.REFERENCES);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiReference());
        assertEquals("value", getOnlyElement(properties.get("name").asMultiReference()));
    }

    @Test
    public void testTextProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.STRING).when(property).getType();
        doReturn("value").when(property).getValue(Type.STRING);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isText());
        assertEquals("value", properties.get("name").asText());
    }

    @Test
    public void testGetMultiTextProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.STRINGS).when(property).getType();
        doReturn(singletonList("value")).when(property).getValue(Type.STRINGS);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiText());
        assertEquals("value", getOnlyElement(properties.get("name").asMultiText()));
    }

    @Test
    public void testUriProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.URI).when(property).getType();
        doReturn("value").when(property).getValue(Type.URI);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isUri());
        assertEquals("value", properties.get("name").asUri());
    }

    @Test
    public void testGetMultiUriProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.URIS).when(property).getType();
        doReturn(singletonList("value")).when(property).getValue(Type.URIS);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiUri());
        assertEquals("value", getOnlyElement(properties.get("name").asMultiUri()));
    }

    @Test
    public void testWeakReferenceProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.WEAKREFERENCE).when(property).getType();
        doReturn("value").when(property).getValue(Type.WEAKREFERENCE);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isWeakReference());
        assertEquals("value", properties.get("name").asWeakReference());
    }

    @Test
    public void testGetMultiWeakReferenceProperty() {
        PropertyState property = mock(PropertyState.class);
        doReturn("name").when(property).getName();
        doReturn(Type.WEAKREFERENCES).when(property).getType();
        doReturn(singletonList("value")).when(property).getValue(Type.WEAKREFERENCES);

        Tree tree = mock(Tree.class);
        doReturn(singletonList(property)).when(tree).getProperties();

        Map<String, RemoteValue> properties = createTree(tree).getProperties();

        assertTrue(properties.containsKey("name"));
        assertTrue(properties.get("name").isMultiWeakReference());
        assertEquals("value", getOnlyElement(properties.get("name").asMultiWeakReference()));
    }

    @Test
    public void testFilterPropertyIn() {
        PropertyState fooProperty = mock(PropertyState.class);
        doReturn("foo").when(fooProperty).getName();
        doReturn(Type.BOOLEAN).when(fooProperty).getType();
        doReturn(true).when(fooProperty).getValue(Type.BOOLEAN);

        PropertyState barProperty = mock(PropertyState.class);
        doReturn("bar").when(barProperty).getName();
        doReturn(Type.BOOLEAN).when(barProperty).getType();
        doReturn(true).when(barProperty).getValue(Type.BOOLEAN);

        Tree tree = mock(Tree.class);
        doReturn(asList(fooProperty, barProperty)).when(tree).getProperties();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public Set<String> getPropertyFilters() {
                return newHashSet("foo");
            }

        });

        Map<String, RemoteValue> properties = remoteTree.getProperties();

        assertTrue(properties.containsKey("foo"));
        assertFalse(properties.containsKey("bar"));
    }

    @Test
    public void testFilterPropertyOut() {
        PropertyState fooProperty = mock(PropertyState.class);
        doReturn("foo").when(fooProperty).getName();
        doReturn(Type.BOOLEAN).when(fooProperty).getType();
        doReturn(true).when(fooProperty).getValue(Type.BOOLEAN);

        PropertyState barProperty = mock(PropertyState.class);
        doReturn("bar").when(barProperty).getName();
        doReturn(Type.BOOLEAN).when(barProperty).getType();
        doReturn(true).when(barProperty).getValue(Type.BOOLEAN);

        Tree tree = mock(Tree.class);
        doReturn(asList(fooProperty, barProperty)).when(tree).getProperties();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public Set<String> getPropertyFilters() {
                return newHashSet("-bar");
            }

        });

        Map<String, RemoteValue> properties = remoteTree.getProperties();

        assertTrue(properties.containsKey("foo"));
        assertFalse(properties.containsKey("bar"));
    }

    @Test
    public void testGetChildrenMaxDepth() {
        Tree child = mock(Tree.class);
        doReturn("child").when(child).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(child)).when(tree).getChildren();

        Map<String, RemoteTree> children = createTree(tree).getChildren();

        assertTrue(children.containsKey("child"));
        assertNull(children.get("child"));
    }

    @Test
    public void testGetChildren() {
        Tree child = mock(Tree.class);
        doReturn("child").when(child).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(child)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getDepth() {
                return 1;
            }

        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertTrue(children.containsKey("child"));
        assertNotNull(children.get("child"));
    }

    @Test
    public void testGetChildrenWithStart() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenStart() {
                return 1;
            }

        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertFalse(children.containsKey("foo"));
        assertTrue(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithNegativeStart() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenStart() {
                return -1;
            }

        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertTrue(children.containsKey("foo"));
        assertTrue(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithStartTooBig() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenStart() {
                return 2;
            }

        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertFalse(children.containsKey("foo"));
        assertFalse(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithCount() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenCount() {
                return 1;
            }
        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertTrue(children.containsKey("foo"));
        assertFalse(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithNegativeCount() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenCount() {
                return -1;
            }
        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertTrue(children.containsKey("foo"));
        assertTrue(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithZeroCount() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenCount() {
                return 0;
            }
        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertFalse(children.containsKey("foo"));
        assertFalse(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithCountTooBig() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenCount() {
                return 3;
            }
        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertTrue(children.containsKey("foo"));
        assertTrue(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithSlicing() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree baz = mock(Tree.class);
        doReturn("baz").when(baz).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar, baz)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public int getChildrenStart() {
                return 1;
            }

            @Override
            public int getChildrenCount() {
                return 1;
            }
        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertFalse(children.containsKey("foo"));
        assertTrue(children.containsKey("bar"));
        assertFalse(children.containsKey("baz"));
    }

    @Test
    public void testGetChildrenWithIncludeFilters() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public Set<String> getNodeFilters() {
                return newHashSet("foo");
            }

        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertTrue(children.containsKey("foo"));
        assertFalse(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithExcludeFilters() {
        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(foo, bar)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public Set<String> getNodeFilters() {
                return newHashSet("-bar");
            }

        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertTrue(children.containsKey("foo"));
        assertFalse(children.containsKey("bar"));
    }

    @Test
    public void testGetChildrenWithSlicingAndFiltering() {
        Tree bar = mock(Tree.class);
        doReturn("bar").when(bar).getName();

        Tree foo = mock(Tree.class);
        doReturn("foo").when(foo).getName();

        Tree baz = mock(Tree.class);
        doReturn("baz").when(baz).getName();

        Tree tree = mock(Tree.class);
        doReturn(asList(bar, foo, baz)).when(tree).getChildren();

        ContentRemoteTree remoteTree = createTree(tree, new RemoteTreeFilters() {

            @Override
            public Set<String> getNodeFilters() {
                return Sets.newHashSet("ba*");
            }

            @Override
            public int getChildrenStart() {
                return 1;
            }

            @Override
            public int getChildrenCount() {
                return 1;
            }
        });

        Map<String, RemoteTree> children = remoteTree.getChildren();

        assertFalse(children.containsKey("bar"));
        assertFalse(children.containsKey("foo"));
        assertFalse(children.containsKey("baz"));
    }

}
