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
package org.apache.jackrabbit.mongomk.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.oak.commons.PathUtils;

public class NodeAssert {

    public static void assertDeepEquals(Node expected, Node actual) {
        assertEquals(expected, actual);

        int expectedCount = expected.getChildNodeCount();
        int actualCount = actual.getChildNodeCount();
        Assert.assertEquals(expectedCount, actualCount);

        for (Iterator<Node> it = expected.getChildNodeEntries(0, -1); it.hasNext(); ) {
            Node expectedChild = it.next();
            String expectedChildName = PathUtils.getName(expectedChild.getPath());
            boolean valid = false;
            for (Iterator<Node> it2 = actual.getChildNodeEntries(0, -1); it2.hasNext(); ) {
                Node actualChild = it2.next();
                String actualChildName = PathUtils.getName(actualChild.getPath());
                if (expectedChildName.equals(actualChildName)) {
                    assertDeepEquals(expectedChild, actualChild);
                    valid = true;
                    break;
                }
            }

            Assert.assertTrue(valid);
        }
    }

    public static void assertEquals(Iterator<Node> expecteds, Collection<Node> actuals) {
        for (Iterator<Node> iter1 = expecteds; iter1.hasNext();) {
            Node expected = iter1.next();
            boolean valid = false;
            for (Iterator<Node> iter2 = actuals.iterator(); iter2.hasNext();) {
                Node actual = iter2.next();
                if (expected.getPath().equals(actual.getPath())) {
                    assertEquals(expected, actual);
                    valid = true;
                    break;
                }
            }
            Assert.assertTrue(valid);
        }
    }

    public static void assertEquals(Collection<Node> expecteds, Collection<Node> actuals) {
        Assert.assertEquals(expecteds.size(), actuals.size());

        for (Node expected : expecteds) {
            boolean valid = false;
            for (Node actual : actuals) {
                if (expected.getPath().equals(actual.getPath())) {
                    assertEquals(expected, actual);
                    valid = true;

                    break;
                }
            }

            Assert.assertTrue(valid);
        }
    }

    public static void assertEquals(Node expected, Node actual) {
        Assert.assertEquals(expected.getPath(), actual.getPath());

        Long expectedRevisionId = expected.getRevisionId();
        Long actualRevisionId = actual.getRevisionId();

        if (expectedRevisionId == null) {
            Assert.assertNull(actualRevisionId);
        }
        if (actualRevisionId == null) {
            Assert.assertNull(expectedRevisionId);
        }

        if ((actualRevisionId != null) && (expectedRevisionId != null)) {
            Assert.assertEquals(expectedRevisionId, actualRevisionId);
        }

        Map<String, String> expectedProperties = expected.getProperties();
        Map<String, String> actualProperties = actual.getProperties();

        if (expectedProperties == null) {
            Assert.assertNull(actualProperties);
        }

        if (actualProperties == null) {
            Assert.assertNull(expectedProperties);
        }

        if ((actualProperties != null) && (expectedProperties != null)) {
            Assert.assertEquals(expectedProperties, actualProperties);
        }
    }
}
