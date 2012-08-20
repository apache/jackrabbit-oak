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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeState;
import org.apache.jackrabbit.oak.spi.Cursor;
import org.junit.Test;

/**
 * Tests the TraversingCursor.
 */
public class TraversingCursorTest {

    @Test
    public void traverse() throws Exception {
        TraversingIndex t = new TraversingIndex();

        MicroKernel mk = new MicroKernelImpl();
        String head = mk.getHeadRevision();
        head = mk.commit("/", "+ \"parents\": { \"p0\": {\"id\": \"0\"}, \"p1\": {\"id\": \"1\"}, \"p2\": {\"id\": \"2\"}}", head, "");
        head = mk.commit("/", "+ \"children\": { \"c1\": {\"p\": \"1\"}, \"c2\": {\"p\": \"1\"}, \"c3\": {\"p\": \"2\"}, \"c4\": {\"p\": \"3\"}}", head, "");
        FilterImpl f = new FilterImpl(null);

        f.setPath("/");
        List<String> paths = new ArrayList<String>();
        Cursor c = t.query(f, head, new KernelNodeState(mk, "/", head));
        while (c.next()) {
            paths.add(c.currentRow().getPath());
        }
        Collections.sort(paths);
        assertEquals(Arrays.asList(
                "/", "/children", "/children/c1", "/children/c2",
                "/children/c3", "/children/c4", "/parents",
                "/parents/p0", "/parents/p1",  "/parents/p2"),
                paths);
        assertFalse(c.next());
        // endure it stays false
        assertFalse(c.next());

        f.setPath("/nowhere");
        c = t.query(f, head, new KernelNodeState(mk, "/", head));
        assertFalse(c.next());
        // endure it stays false
        assertFalse(c.next());
    }

}
