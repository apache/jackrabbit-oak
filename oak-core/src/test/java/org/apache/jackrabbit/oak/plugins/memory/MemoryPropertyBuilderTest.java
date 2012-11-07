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
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemoryPropertyBuilderTest {

    @Test
    public void testStringProperty() {
        PropertyBuilder<String> builder = MemoryPropertyBuilder.scalar(Type.STRING);
        builder.setName("foo").setValue("bar");
        assertEquals(StringPropertyState.stringProperty("foo", "bar"),
                builder.getPropertyState());

        builder.setArray();
        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("bar")),
                builder.getPropertyState());
    }

    @Test
    public void testLongProperty() {
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.scalar(Type.LONG);
        builder.setName("foo").setValue(42L);
        assertEquals(LongPropertyState.createLongProperty("foo", 42L),
                builder.getPropertyState());

        builder.setArray();
        assertEquals(MultiLongPropertyState.createLongProperty("foo", Arrays.asList(42L)),
                builder.getPropertyState());
    }

    @Test
    public void testStringsProperty() {
        PropertyBuilder<String> builder = MemoryPropertyBuilder.array(Type.STRING);
        builder.setName("foo")
                .addValue("one")
                .addValue("two");
        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("one", "two")),
                builder.getPropertyState());

        builder.setScalar();
        try {
            builder.getPropertyState();
        }
        catch (IllegalStateException expected) {
        }

        builder.removeValue("one");
        assertEquals(StringPropertyState.stringProperty("foo", "two"),
                builder.getPropertyState());
    }

    @Test
    public void testDateProperty() {
        PropertyBuilder<String> builder = MemoryPropertyBuilder.array(Type.DATE);
        String date1 = "1970-01-01T00:00:00.000Z";
        String date2 = "1971-01-01T00:00:00.000Z";
        builder.setName("foo")
                .addValue(date1)
                .addValue(date2);
        assertEquals(MultiLongPropertyState.createDateProperty("foo", Arrays.asList(date1, date2)),
                builder.getPropertyState());

        builder.setScalar();
        try {
            builder.getPropertyState();
        }
        catch (IllegalStateException expected) {
        }

        builder.removeValue(date1);
        assertEquals(LongPropertyState.createDateProperty("foo", date2),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromLong() {
        PropertyState source = LongPropertyState.createLongProperty("foo", 42L);
        PropertyBuilder<String> builder = MemoryPropertyBuilder.scalar(Type.STRING);
        builder.assignFrom(source);
        assertEquals(StringPropertyState.stringProperty("foo", "42"),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromString() {
        PropertyState source = StringPropertyState.stringProperty("foo", "42");
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.scalar(Type.LONG);
        builder.assignFrom(source);
        assertEquals(LongPropertyState.createLongProperty("foo", 42L),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromDate() {
        String date = "1970-01-01T00:00:00.000Z";
        PropertyState source = LongPropertyState.createDateProperty("foo", date);
        PropertyBuilder<String> builder = MemoryPropertyBuilder.scalar(Type.DATE);
        builder.assignFrom(source);
        assertEquals(source, builder.getPropertyState());
    }

    @Test(expected = NumberFormatException.class)
    public void testAssignFromStringNumberFormatException() {
        PropertyState source = StringPropertyState.stringProperty("foo", "bar");
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.scalar(Type.LONG);
        builder.assignFrom(source);
    }

    @Test
    public void testAssignFromLongs() {
        PropertyState source = MultiLongPropertyState.createLongProperty("foo", Arrays.asList(1L, 2L, 3L));
        PropertyBuilder<String> builder = MemoryPropertyBuilder.scalar(Type.STRING);
        builder.assignFrom(source);
        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("1", "2", "3")),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromStrings() {
        PropertyState source = MultiStringPropertyState.stringProperty("foo", Arrays.asList("1", "2", "3"));
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.scalar(Type.LONG);
        builder.assignFrom(source);
        assertEquals(MultiLongPropertyState.createLongProperty("foo", Arrays.asList(1L, 2L, 3L)),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromDates() {
        String date1 = "1970-01-01T00:00:00.000Z";
        String date2 = "1971-01-01T00:00:00.000Z";
        PropertyState source = MultiLongPropertyState.createDateProperty("foo", Arrays.asList(date1, date2));
        PropertyBuilder<String> builder = MemoryPropertyBuilder.scalar(Type.DATE);
        builder.assignFrom(source);
        assertEquals(source, builder.getPropertyState());
    }

    @Test
    public void testAssignInvariant() {
        PropertyState source = MultiStringPropertyState.stringProperty("source", Arrays.asList("1", "2", "3"));
        PropertyBuilder<String> builder = MemoryPropertyBuilder.scalar(Type.STRING);
        builder.assignFrom(source);
        assertEquals(source, builder.getPropertyState());
    }

}
