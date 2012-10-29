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
        PropertyBuilder<String> builder = MemoryPropertyBuilder.create(Type.STRING);
        builder.setName("foo").setValue("bar");
        assertEquals(StringPropertyState.stringProperty("foo", "bar"),
                builder.getPropertyState());

        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("bar")),
                builder.getPropertyState(true));
    }

    @Test
    public void testLongProperty() {
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.create(Type.LONG);
        builder.setName("foo").setValue(42L);
        assertEquals(LongPropertyState.createLongProperty("foo", 42L),
                builder.getPropertyState());

        assertEquals(MultiLongPropertyState.createLongProperty("foo", Arrays.asList(42L)),
                builder.getPropertyState(true));
    }

    @Test
    public void testStringsProperty() {
        PropertyBuilder<String> builder = MemoryPropertyBuilder.create(Type.STRING);
        builder.setName("foo")
                .addValue("one")
                .addValue("two");
        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("one", "two")),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromLong() {
        PropertyState source = LongPropertyState.createLongProperty("foo", 42L);
        PropertyBuilder<String> builder = MemoryPropertyBuilder.create(Type.STRING);
        builder.assignFrom(source);
        assertEquals(StringPropertyState.stringProperty("foo", "42"),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromString() {
        PropertyState source = StringPropertyState.stringProperty("foo", "42");
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.create(Type.LONG);
        builder.assignFrom(source);
        assertEquals(LongPropertyState.createLongProperty("foo", 42L),
                builder.getPropertyState());
    }

    @Test(expected = NumberFormatException.class)
    public void testAssignFromStringNumberFormatException() {
        PropertyState source = StringPropertyState.stringProperty("foo", "bar");
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.create(Type.LONG);
        builder.assignFrom(source);
    }

    @Test
    public void testAssignFromLongs() {
        PropertyState source = MultiLongPropertyState.createLongProperty("foo", Arrays.asList(1L, 2L, 3L));
        PropertyBuilder<String> builder = MemoryPropertyBuilder.create(Type.STRING);
        builder.assignFrom(source);
        assertEquals(MultiStringPropertyState.stringProperty("foo", Arrays.asList("1", "2", "3")),
                builder.getPropertyState());
    }

    @Test
    public void testAssignFromStrings() {
        PropertyState source = MultiStringPropertyState.stringProperty("foo", Arrays.asList("1", "2", "3"));
        PropertyBuilder<Long> builder = MemoryPropertyBuilder.create(Type.LONG);
        builder.assignFrom(source);
        assertEquals(MultiLongPropertyState.createLongProperty("foo", Arrays.asList(1L, 2L, 3L)),
                builder.getPropertyState());
    }

    @Test
    public void testAssignInvariant() {
        PropertyState source = MultiStringPropertyState.stringProperty("source", Arrays.asList("1", "2", "3"));
        PropertyBuilder<String> builder = MemoryPropertyBuilder.create(Type.STRING);
        builder.assignFrom(source);
        assertEquals(source, builder.getPropertyState(true));
    }

}
