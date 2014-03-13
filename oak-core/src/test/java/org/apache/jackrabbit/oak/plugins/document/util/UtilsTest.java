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
package org.apache.jackrabbit.oak.plugins.document.util;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Utils}.
 */
public class UtilsTest {

    @Test
    public void getPreviousIdFor() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        String p = Utils.getIdFromPath("/");
        assertEquals("1:p/" + r.toString(), Utils.getPreviousIdFor(p, r));
        p = Utils.getIdFromPath("/test");
        assertEquals("2:p/test/" + r.toString(), Utils.getPreviousIdFor(p, r));
        p = Utils.getIdFromPath("/a/b/c/d/e/f/g/h/i/j/k/l/m");
        assertEquals("14:p/a/b/c/d/e/f/g/h/i/j/k/l/m/" + r.toString(), Utils.getPreviousIdFor(p, r));
    }

    @Ignore("Performance test")
    @Test
    public void performance_getPreviousIdFor() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        String id = Utils.getIdFromPath("/some/test/path/foo");
        // warm up
        for (int i = 0; i < 1 * 1000 * 1000; i++) {
            Utils.getPreviousIdFor(id, r);
        }
        long time = System.currentTimeMillis();
        for (int i = 0; i < 10 * 1000 * 1000; i++) {
            Utils.getPreviousIdFor(id, r);
        }
        time = System.currentTimeMillis() - time;
        System.out.println(time);
    }

    @Ignore("Performance test")
    @Test
    public void performance_revisionToString() {
        Revision r = new Revision(System.currentTimeMillis(), 0, 0);
        // warm up
        for (int i = 0; i < 1 * 1000 * 1000; i++) {
            r.toString();
        }
        long time = System.currentTimeMillis();
        for (int i = 0; i < 30 * 1000 * 1000; i++) {
            r.toString();
        }
        time = System.currentTimeMillis() - time;
        System.out.println(time);
    }
}
