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
package org.apache.jackrabbit.oak.jcr;

import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;

public class OrderableNodesTest extends AbstractRepositoryTest {

    @Test
    public void testSimpleOrdering() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode().addNode("test");

        root.addNode("a");
        root.addNode("b");
        root.addNode("c");

        NodeIterator iterator;

        root.orderBefore("a", "b");
        root.orderBefore("c", null);
        iterator = root.getNodes();
        assertEquals("a", iterator.nextNode().getName());
        assertEquals("b", iterator.nextNode().getName());
        assertEquals("c", iterator.nextNode().getName());
        assertFalse(iterator.hasNext());

        root.orderBefore("c", "a");
        iterator = root.getNodes();
        assertEquals("c", iterator.nextNode().getName());
        assertEquals("a", iterator.nextNode().getName());
        assertEquals("b", iterator.nextNode().getName());
        assertFalse(iterator.hasNext());

        root.orderBefore("b", "c");
        iterator = root.getNodes();
        assertEquals("b", iterator.nextNode().getName());
        assertEquals("c", iterator.nextNode().getName());
        assertEquals("a", iterator.nextNode().getName());
        assertFalse(iterator.hasNext());
    }

}
