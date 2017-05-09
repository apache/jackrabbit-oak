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

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.remote.RemoteCommitException;
import org.apache.jackrabbit.oak.remote.RemoteValue;
import org.apache.jackrabbit.oak.remote.RemoteValue.Supplier;
import org.apache.jackrabbit.util.ISO8601;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

import static com.google.common.collect.Iterables.any;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toBinary;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toBinaryId;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toBoolean;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toDate;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toDecimal;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toDouble;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toLong;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiBinary;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiBinaryId;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiBoolean;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiDate;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiDecimal;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiDouble;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiLong;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiName;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiPath;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiReference;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiText;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiUri;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toMultiWeakReference;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toName;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toPath;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toReference;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toText;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toUri;
import static org.apache.jackrabbit.oak.remote.RemoteValue.toWeakReference;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SetContentRemoteOperationTest {

    private SetContentRemoteOperation createOperation(String path, String name, RemoteValue value) {
        return new SetContentRemoteOperation(mock(ContentRemoteBinaries.class), path, name, value);
    }

    private SetContentRemoteOperation createOperation(ContentRemoteBinaries binaries, String path, String name, RemoteValue value) {
        return new SetContentRemoteOperation(binaries, path, name, value);
    }

    private <T> Matcher<Iterable<T>> isIterableReferencing(final T value) {
        return new BaseMatcher<Iterable<T>>() {

            @Override
            public boolean matches(Object item) {
                Iterable iterable = null;

                if (item instanceof Iterable) {
                    iterable = (Iterable) item;
                }

                if (iterable == null) {
                    return false;
                }

                return any(iterable, new Predicate() {

                    @Override
                    public boolean apply(Object element) {
                        return element == value;
                    }

                });
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("an iterable referencing ").appendValue(value);
            }

        };
    }

    private <T> Matcher<Iterable<T>> isIterableContaining(final T value) {
        return new BaseMatcher<Iterable<T>>() {

            @Override
            public boolean matches(Object item) {
                Iterable iterable = null;

                if (item instanceof Iterable) {
                    iterable = (Iterable) item;
                }

                if (iterable == null) {
                    return false;
                }

                return any(iterable, new Predicate() {

                    @Override
                    public boolean apply(Object element) {
                        if (element == null && value == null) {
                            return true;
                        }

                        if (element != null && value != null) {
                            return element.equals(value);
                        }

                        return false;
                    }

                });
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("an iterable containing ").appendValue(value);
            }

        };
    }

    @Test(expected = RemoteCommitException.class)
    public void testSetWithNonExistingTree() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(false).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", mock(RemoteValue.class)).apply(root);
    }

    @Test
    public void testSetBinaryProperty() throws Exception {
        Blob blob = mock(Blob.class);

        InputStream stream = mock(InputStream.class);

        Supplier<InputStream> supplier = mock(Supplier.class);
        doReturn(stream).when(supplier).get();

        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");
        doReturn(blob).when(root).createBlob(stream);

        createOperation("/test", "name", toBinary(supplier)).apply(root);

        verify(tree).setProperty("name", blob, Type.BINARY);
    }

    @Test
    public void testSetMultiBinaryProperty() throws Exception {
        Blob blob = mock(Blob.class);

        InputStream stream = mock(InputStream.class);

        Supplier<InputStream> supplier = mock(Supplier.class);
        doReturn(stream).when(supplier).get();

        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");
        doReturn(blob).when(root).createBlob(stream);

        createOperation("/test", "name", toMultiBinary(singletonList(supplier))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableReferencing(blob)), eq(Type.BINARIES));
    }

    @Test
    public void testSetBinaryIdProperty() throws Exception {
        Blob blob = mock(Blob.class);

        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn(blob).when(binaries).get("id");

        createOperation(binaries, "/test", "name", toBinaryId("id")).apply(root);

        verify(tree).setProperty("name", blob, Type.BINARY);
    }

    @Test
    public void setMultiBinaryIdProperty() throws Exception {
        Blob blob = mock(Blob.class);

        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        ContentRemoteBinaries binaries = mock(ContentRemoteBinaries.class);
        doReturn(blob).when(binaries).get("id");

        createOperation(binaries, "/test", "name", toMultiBinaryId(singletonList("id"))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableReferencing(blob)), eq(Type.BINARIES));
    }

    @Test
    public void testSetBooleanProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toBoolean(true)).apply(root);

        verify(tree).setProperty("name", true, Type.BOOLEAN);
    }

    @Test
    public void testSetMultiBooleanProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiBoolean(singletonList(true))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining(true)), eq(Type.BOOLEANS));
    }

    @Test
    public void testSetDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toDate(calendar.getTimeInMillis())).apply(root);

        verify(tree).setProperty("name", ISO8601.format(calendar), Type.DATE);
    }

    @Test
    public void testSetMultiDateProperty() throws Exception {
        Calendar calendar = Calendar.getInstance();

        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiDate(singletonList(calendar.getTimeInMillis()))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining(ISO8601.format(calendar))), eq(Type.DATES));
    }

    @Test
    public void testSetDecimalProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toDecimal(BigDecimal.ONE)).apply(root);

        verify(tree).setProperty("name", BigDecimal.ONE, Type.DECIMAL);
    }

    @Test
    public void testSetMultiDecimalProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiDecimal(singletonList(BigDecimal.ONE))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining(BigDecimal.ONE)), eq(Type.DECIMALS));
    }

    @Test
    public void testSetDoubleProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toDouble(4.2)).apply(root);

        verify(tree).setProperty("name", 4.2, Type.DOUBLE);
    }

    @Test
    public void testSetMultiDoubleProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiDouble(singletonList(4.2))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining(4.2)), eq(Type.DOUBLES));
    }

    @Test
    public void testSetLongProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toLong(42L)).apply(root);

        verify(tree).setProperty("name", 42L, Type.LONG);
    }

    @Test
    public void testSetMultiLongProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiLong(singletonList(42L))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining(42L)), eq(Type.LONGS));
    }

    @Test
    public void testSetNameProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toName("value")).apply(root);

        verify(tree).setProperty("name", "value", Type.NAME);
    }

    @Test
    public void testSetMultiNameProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiName(singletonList("value"))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining("value")), eq(Type.NAMES));
    }

    @Test
    public void testSetPathProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toPath("value")).apply(root);

        verify(tree).setProperty("name", "value", Type.PATH);
    }

    @Test
    public void testSetMultiPathProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiPath(singletonList("value"))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining("value")), eq(Type.PATHS));
    }

    @Test
    public void testSetReferenceProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toReference("value")).apply(root);

        verify(tree).setProperty("name", "value", Type.REFERENCE);
    }

    @Test
    public void testSetMultiReferenceProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiReference(singletonList("value"))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining("value")), eq(Type.REFERENCES));
    }

    @Test
    public void testSetStringProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toText("value")).apply(root);

        verify(tree).setProperty("name", "value", Type.STRING);
    }

    @Test
    public void testSetMultiStringProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiText(singletonList("value"))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining("value")), eq(Type.STRINGS));
    }

    @Test
    public void testSetUriProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toUri("value")).apply(root);

        verify(tree).setProperty("name", "value", Type.URI);
    }

    @Test
    public void testSetMultiUriProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiUri(singletonList("value"))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining("value")), eq(Type.URIS));
    }

    @Test
    public void testSetWeakReferenceProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toWeakReference("value")).apply(root);

        verify(tree).setProperty("name", "value", Type.WEAKREFERENCE);
    }

    @Test
    public void testSetMultiWeakReferenceProperty() throws Exception {
        Tree tree = mock(Tree.class);
        doReturn(true).when(tree).exists();

        Root root = mock(Root.class);
        doReturn(tree).when(root).getTree("/test");

        createOperation("/test", "name", toMultiWeakReference(singletonList("value"))).apply(root);

        verify(tree).setProperty(eq("name"), argThat(isIterableContaining("value")), eq(Type.WEAKREFERENCES));
    }

}
