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
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class CRUDTest extends AbstractRepositoryTest {

    @Test
    public void testCRUD() throws RepositoryException {
        Session session = createAnonymousSession();
        try {
            // Create
            Node hello = session.getRootNode().addNode("hello");
            hello.setProperty("world",  "hello world");
            session.save();

            // Read
            assertEquals(
                    "hello world",
                    session.getProperty("/hello/world").getString());

            // Update
            session.getNode("/hello").setProperty("world", "Hello, World!");
            session.save();
            assertEquals(
                    "Hello, World!",
                    session.getProperty("/hello/world").getString());

            // Delete
            session.getNode("/hello").remove();
            session.save();
            assertTrue(!session.propertyExists("/hello/world"));
        } finally {
            session.logout();
        }
    }

}
