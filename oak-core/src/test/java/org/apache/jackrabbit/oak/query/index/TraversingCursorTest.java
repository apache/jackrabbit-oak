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
package org.apache.jackrabbit.oak.query.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.apache.jackrabbit.mk.MicroKernelFactory;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the TraversingCursor.
 */
public class TraversingCursorTest {

    MicroKernel mk;
    String head;

    @Before
    public void setUp() {
        mk = MicroKernelFactory.getInstance("simple:/target/temp;clear");
        head = mk.getHeadRevision();
    }

    @After
    public void tearDown() {
        mk.dispose();
    }

    @Test
    public void traverse() throws Exception {
        TraversingIndex t = new TraversingIndex(mk);
        traverse(t);
    }

    @Test
    public void traverseBlockwise() throws Exception {
        TraversingIndex t = new TraversingIndex(mk);
        t.setChildBlockSize(2);
        traverse(t);
    }

    private void traverse(TraversingIndex t) {
        head = mk.commit("/", "+ \"parents\": { \"p0\": {\"id\": \"0\"}, \"p1\": {\"id\": \"1\"}, \"p2\": {\"id\": \"2\"}}", head, "");
        head = mk.commit("/", "+ \"children\": { \"c1\": {\"p\": \"1\"}, \"c2\": {\"p\": \"1\"}, \"c3\": {\"p\": \"2\"}, \"c4\": {\"p\": \"3\"}}", head, "");
        Filter f = new Filter(null);
        Cursor c;
        f.setPath("/");
        c = t.query(f, head);
        String[] list = {"/", "/parents", "/parents/p0", "/parents/p1",  "/parents/p2",
                "/children", "/children/c1", "/children/c2", "/children/c3", "/children/c4"};
        for (String s : list) {
            assertTrue(c.next());
            assertEquals(s, c.currentPath());
        }
        assertFalse(c.next());
        assertFalse(c.next());
        f.setPath("/nowhere");
        c = t.query(f, head);
        assertFalse(c.next());
        assertFalse(c.next());
    }

}
