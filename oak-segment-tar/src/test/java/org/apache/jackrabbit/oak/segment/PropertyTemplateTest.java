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

import org.apache.jackrabbit.oak.api.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PropertyTemplateTest {

    @Test
    public void testCompareToWithDifferentNames() {
        PropertyTemplate p1 = new PropertyTemplate(0, "aName", Type.STRING);
        PropertyTemplate p2 = new PropertyTemplate(1, "bName", Type.STRING);

        // Since hashCode is used for primary comparison and hashCode uses only name,
        // templates with different names should be ordered by name's hashCode
        Assert.assertTrue(p1.hashCode() < p2.hashCode());
        Assert.assertTrue(p1.compareTo(p2) < 0);
        Assert.assertTrue(p2.compareTo(p1) > 0);
    }

    @Test
    public void testCompareToWithSameNamesDifferentTypes() {
        PropertyTemplate p1 = new PropertyTemplate(0, "sameName", Type.STRING);
        PropertyTemplate p2 = new PropertyTemplate(0, "sameName", Type.BOOLEAN);

        // Same name (so same hashCode), different types
        Assert.assertEquals(p1.hashCode(), p2.hashCode());
        // Type comparison is determined by the Type enum's natural order
        Assert.assertNotEquals(0, p1.compareTo(p2));
    }

    @Test
    public void testCompareToWithSameNamesSameTypesDifferentIndices() {
        PropertyTemplate p1 = new PropertyTemplate(0, "sameName", Type.STRING);
        PropertyTemplate p2 = new PropertyTemplate(1, "sameName", Type.STRING);

        // Same name and type, different indices
        Assert.assertEquals(p1.hashCode(), p2.hashCode());
        Assert.assertEquals(0, p1.compareTo(p2)); // Index is not used in comparison
        Assert.assertEquals(p1, p2); // Index is not used in equals either
    }

    @Test
    public void testConsistencyBetweenEqualsAndCompareTo() {
        PropertyTemplate p1 = new PropertyTemplate(0, "name", Type.STRING);
        PropertyTemplate p2 = new PropertyTemplate(0, "name", Type.STRING);
        PropertyTemplate p3 = new PropertyTemplate(0, "name", Type.BOOLEAN);

        // Basic equality check
        Assert.assertEquals(p1, p1); // Reflexivity
        Assert.assertEquals(p1, p2); // Symmetry
        Assert.assertEquals(0, p1.compareTo(p2));

        // Different type
        Assert.assertNotEquals(p1, p3);
        Assert.assertNotEquals(0, p1.compareTo(p3));

        // Not a PropertyTemplate
        Assert.assertNotEquals("not a template", p1);
    }

    @Test
    public void testSortingBehavior() {
        List<PropertyTemplate> templates = new ArrayList<>();

        // Create templates with varying names and types
        templates.add(new PropertyTemplate(0, "c", Type.STRING));
        templates.add(new PropertyTemplate(1, "a", Type.STRING));
        templates.add(new PropertyTemplate(2, "b", Type.STRING));
        templates.add(new PropertyTemplate(3, "a", Type.BOOLEAN));

        Collections.sort(templates);

        // Check the expected sort order based on hashCode, then name, then type
        Assert.assertEquals("a", templates.get(0).getName()); // 'a' has lowest hashCode
        // BOOLEAN comes after STRING for same name 'a'
        // since they are compared by Type enum tag value (javax.jcr.PropertyType) which is higher for BOOLEAN
        Assert.assertEquals(Type.STRING, templates.get(0).getType());
        Assert.assertEquals("a", templates.get(1).getName());
        Assert.assertEquals(Type.BOOLEAN, templates.get(1).getType());
        Assert.assertEquals("b", templates.get(2).getName());
        Assert.assertEquals("c", templates.get(3).getName());
    }

}