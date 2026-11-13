/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Before;
import org.junit.Test;

public class TemplateTest {
    private MemoryStore store;

    @Before
    public void setup() throws IOException {
        store = new MemoryStore();
    }

    @Test
    public void testHashCode() throws IOException {
        // child node null vs ""
        PropertyState primary = createProperty("primary", "primary");
        PropertyState mixin = createProperty("mixin", "mixin");
        PropertyTemplate[] properties = new PropertyTemplate[0];

        Template t0 = new Template(store.getReader(), primary, mixin, properties, "");
        Template t1 = new Template(store.getReader(), primary, mixin, properties, null);

        assertNotEquals(t0.hashCode(), t1.hashCode());
    }

    @Test
    public void testHashCode2() throws IOException {
        // mixins null vs []
        PropertyState primary = createProperty("primary", "primary");
        PropertyState mixin = createProperty("mixin", new ArrayList<String>(),
                Type.STRINGS);
        PropertyTemplate[] properties = new PropertyTemplate[0];
        String childNode = "c";

        Template t0 = new Template(store.getReader(), primary, null, properties, childNode);
        Template t1 = new Template(store.getReader(), primary, mixin, properties, childNode);

        assertNotEquals(t0.hashCode(), t1.hashCode());
    }

    @Test
    public void testEquals() throws IOException {
        // same properties, different order
        PropertyState primary = createProperty("primary", "primary");
        PropertyState mixin = createProperty("mixin", "mixin");

        PropertyTemplate p0 = new PropertyTemplate(createProperty("p0", "v0"));
        PropertyTemplate p1 = new PropertyTemplate(createProperty("p1", "v1"));
        PropertyTemplate[] pt0 = new PropertyTemplate[] { p0, p1 };
        PropertyTemplate[] pt1 = new PropertyTemplate[] { p1, p0 };

        String childNode = "c";

        Template t0 = new Template(store.getReader(), primary, mixin, pt0, childNode);
        Template t1 = new Template(store.getReader(), primary, mixin, pt1, childNode);

        assertEquals(t0, t1);
    }

    @Test
    public void testGetPropertyTemplateNoProperties() throws IOException {
        PropertyTemplate[] properties = new PropertyTemplate[0];
        Template t = new Template(store.getReader(), null, null, properties, null);

        assertNull(t.getPropertyTemplate("missing"));
    }

    @Test
    public void testGetPropertyTemplateSingleProperty() throws IOException {
        PropertyTemplate p0 = new PropertyTemplate(createProperty("p0", "v0"));
        PropertyTemplate[] properties = new PropertyTemplate[] { p0 };
        Template t = new Template(store.getReader(), null, null, properties, null);

        assertEquals(p0, t.getPropertyTemplate("p0"));
        assertNull(t.getPropertyTemplate("missing"));
    }

    @Test
    public void testGetPropertyTemplateMultipleProperties() throws IOException {
        // odd number of properties so the binary search midpoint lands both on
        // an even and an odd boundary across the lookups below
        PropertyTemplate[] properties = new PropertyTemplate[] {
                new PropertyTemplate(createProperty("a", "1")),
                new PropertyTemplate(createProperty("b", "2")),
                new PropertyTemplate(createProperty("c", "3")),
                new PropertyTemplate(createProperty("d", "4")),
                new PropertyTemplate(createProperty("e", "5"))
        };
        Template t = new Template(store.getReader(), null, null, properties, null);

        for (PropertyTemplate property : properties) {
            assertEquals(property, t.getPropertyTemplate(property.getName()));
        }
        assertNull(t.getPropertyTemplate("missing"));
    }

    @Test
    public void testGetPropertyTemplateHashCollision() throws IOException {
        // "Aa" and "BB" are distinct names that share the same String.hashCode()
        // (2112), exercising the hash-collision run walk after the binary
        // search locates the shared hash.
        assertEquals("Aa".hashCode(), "BB".hashCode());

        PropertyTemplate collision1 = new PropertyTemplate(createProperty("Aa", "v1"));
        PropertyTemplate collision2 = new PropertyTemplate(createProperty("BB", "v2"));
        PropertyTemplate other = new PropertyTemplate(createProperty("other", "v3"));
        PropertyTemplate[] properties = new PropertyTemplate[] { collision1, collision2, other };

        Template t = new Template(store.getReader(), null, null, properties, null);

        assertEquals(collision1, t.getPropertyTemplate("Aa"));
        assertEquals(collision2, t.getPropertyTemplate("BB"));
        assertEquals(other, t.getPropertyTemplate("other"));
    }
}
