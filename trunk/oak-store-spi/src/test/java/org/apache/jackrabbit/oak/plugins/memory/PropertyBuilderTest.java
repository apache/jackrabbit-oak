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
package org.apache.jackrabbit.oak.plugins.memory;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PropertyBuilderTest {

    @Test
    public void testStringProperty() {
        PropertyBuilder builder = PropertyBuilder.scalar(Type.STRING);
        builder.setName("foo").setValue("bar");
        Assert.assertEquals(StringPropertyState.stringProperty("foo", "bar"),
                builder.getPropertyState());

        builder.setArray();
        Assert.assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("bar")),
                builder.getPropertyState());
    }

    @Test
    public void testLongProperty() {
        PropertyBuilder builder = PropertyBuilder.scalar(Type.LONG);
        builder.setName("foo").setValue(42L);
        Assert.assertEquals(LongPropertyState.createLongProperty("foo", 42L),
                builder.getPropertyState());

        builder.setArray();
        Assert.assertEquals(MultiLongPropertyState.createLongProperty("foo", Arrays.asList(42L)),
                builder.getPropertyState());
    }

    @Test
    public void testStringsProperty() {
        PropertyBuilder builder = PropertyBuilder.array(Type.STRING);
        builder.setName("foo")
                .addValue("one")
                .addValue("two");
        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("one", "two")),
                builder.getPropertyState());

        builder.setScalar();
        try {
            builder.getPropertyState();
        } catch (IllegalStateException expected) {
            // success
        }

        builder.removeValue("one");
        assertEquals(StringPropertyState.stringProperty("foo", "two"),
                builder.getPropertyState());
    }

    @Test
    public void testDateProperty() {
        PropertyBuilder builder = PropertyBuilder.array(Type.DATE);
        String date1 = "1970-01-01T00:00:00.000Z";
        String date2 = "1971-01-01T00:00:00.000Z";
        builder.setName("foo")
                .addValue(date1)
                .addValue(date2);
        Assert.assertEquals(MultiGenericPropertyState.dateProperty("foo", Arrays.asList(date1, date2)),
                builder.getPropertyState());

        builder.setScalar();
        try {
            builder.getPropertyState();
        } catch (IllegalStateException expected) {
        }

        builder.removeValue(date1);
        Assert.assertEquals(GenericPropertyState.dateProperty("foo", date2),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromLong() {
        PropertyState source = LongPropertyState.createLongProperty("foo", 42L);
        PropertyBuilder builder = PropertyBuilder.scalar(Type.STRING);
        builder.assignFrom(source);
        assertEquals(StringPropertyState.stringProperty("foo", "42"),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromString() {
        PropertyState source = StringPropertyState.stringProperty("foo", "42");
        PropertyBuilder builder = PropertyBuilder.scalar(Type.LONG);
        builder.assignFrom(source);
        assertEquals(LongPropertyState.createLongProperty("foo", 42L),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromDate() {
        String date = "1970-01-01T00:00:00.000Z";
        PropertyState source = GenericPropertyState.dateProperty("foo", date);
        PropertyBuilder builder = PropertyBuilder.scalar(Type.DATE);
        builder.assignFrom(source);
        assertEquals(source, builder.getPropertyState());
    }

    @Test(expected = NumberFormatException.class)
    public void testAssignFromStringNumberFormatException() {
        PropertyState source = StringPropertyState.stringProperty("foo", "bar");
        PropertyBuilder builder = PropertyBuilder.scalar(Type.LONG);
        builder.assignFrom(source);
    }

    @Test
    public void testAssignFromLongs() {
        PropertyState source = MultiLongPropertyState.createLongProperty("foo", Arrays.asList(1L, 2L, 3L));
        PropertyBuilder builder = PropertyBuilder.scalar(Type.STRING);
        builder.assignFrom(source);
        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("1", "2", "3")),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromStrings() {
        PropertyState source = MultiStringPropertyState.stringProperty("foo", Arrays.asList("1", "2", "3"));
        PropertyBuilder builder = PropertyBuilder.scalar(Type.LONG);
        builder.assignFrom(source);
        assertEquals(MultiLongPropertyState.createLongProperty("foo", Arrays.asList(1L, 2L, 3L)),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromDates() {
        String date1 = "1970-01-01T00:00:00.000Z";
        String date2 = "1971-01-01T00:00:00.000Z";
        PropertyState source = MultiGenericPropertyState.dateProperty("foo", Arrays.asList(date1, date2));
        PropertyBuilder builder = PropertyBuilder.scalar(Type.DATE);
        builder.assignFrom(source);
        assertEquals(source, builder.getPropertyState());
    }

    @Test
    public void testAssignInvariant() {
        PropertyState source = MultiStringPropertyState.stringProperty("source", Arrays.asList("1", "2", "3"));
        PropertyBuilder builder = PropertyBuilder.scalar(Type.STRING);
        builder.assignFrom(source);
        assertEquals(source, builder.getPropertyState());
    }

}
