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
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mongomk.api.model.Node;

import junit.framework.Assert;


/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class NodeAssert {

    public static void assertDeepEquals(Node expected, Node actual) {
        assertEquals(expected, actual);

        Set<Node> expectedChildren = expected.getChildren();
        Set<Node> actualChildren = actual.getChildren();

        if (expectedChildren == null) {
            Assert.assertNull(actualChildren);
        } else {
            Assert.assertNotNull(actualChildren);
            Assert.assertEquals(expectedChildren.size(), actualChildren.size());

            for (Node expectedChild : expectedChildren) {
                boolean valid = false;
                for (Node actualChild : actualChildren) {
                    if (expectedChild.getName().equals(actualChild.getName())) {
                        assertDeepEquals(expectedChild, actualChild);
                        valid = true;

                        break;
                    }
                }

                Assert.assertTrue(valid);
            }
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
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.getPath(), actual.getPath());

        String expectedRevisionId = expected.getRevisionId();
        String actualRevisionId = actual.getRevisionId();

        if (expectedRevisionId == null) {
            Assert.assertNull(actualRevisionId);
        }
        if (actualRevisionId == null) {
            Assert.assertNull(expectedRevisionId);
        }

        if ((actualRevisionId != null) && (expectedRevisionId != null)) {
            Assert.assertEquals(expectedRevisionId, actualRevisionId);
        }

        Map<String, Object> expectedProperties = expected.getProperties();
        Map<String, Object> actualProperties = actual.getProperties();

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

    private NodeAssert() {
        // no instantiation
    }
}
