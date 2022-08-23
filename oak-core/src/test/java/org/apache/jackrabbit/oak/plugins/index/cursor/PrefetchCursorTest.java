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
package org.apache.jackrabbit.oak.plugins.index.cursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class PrefetchCursorTest {
    
    @Test
    public void findMatch() {
        // find the file name, but only if it's a jpg
        assertEquals("hello", PrefetchCursor.findMatch(
                "/content/images/hello.jpg", 
                ".+/(.*)\\.jpg"));
        // it will find "null" it it's not a jpg
        assertNull(PrefetchCursor.findMatch(
                "/content/images/hello.tiff", 
                ".+/(.*)\\.jpg"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void findMatchException() {
        PrefetchCursor.findMatch(
                "/content/images/hello.tiff", 
                "***");
    }
    
    @Test
    public void applyAndResolvePatterns() {
        assertEquals("jcr:content/metadata", 
                PrefetchCursor.applyAndResolvePatterns(
                "jcr:content/metadata", 
                "/content/images/abc"));
        assertEquals("jcr:content/renditions/abc", 
                PrefetchCursor.applyAndResolvePatterns(
                "jcr:content/renditions/${regex(\".+/(.*)\\.jpg\")}", 
                "/content/images/abc.jpg"));
        assertNull(
                PrefetchCursor.applyAndResolvePatterns(
                "jcr:content/renditions/${regex(\".+/(.*)\\.jpg\")}", 
                "/content/images/abc.tiff"));
        assertEquals("jcr:content/renditions/abc", 
                PrefetchCursor.applyAndResolvePatterns(
                "jcr:content/renditions/${regex(\"\\{(.*)\\\\}\")}", 
                "/content/images/{abc}/def"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void applyAndResolvePatternsException() {
        PrefetchCursor.applyAndResolvePatterns(
                "jcr:content/${regex(\"***\")}",
                "/content/images/abc");
    }

    @Test
    public void resolve() {
        assertEquals("/content/images/abc/jcr:content/metadata", 
                PrefetchCursor.resolve(
                "jcr:content/metadata", 
                "/content/images/abc"));
        assertEquals("/content/images/abc.jpg/jcr:content/renditions/abc.xml", 
                PrefetchCursor.resolve(
                "jcr:content/renditions/${regex(\".+/(.*)\\.jpg\")}.xml", 
                "/content/images/abc.jpg"));
        assertNull(
                PrefetchCursor.resolve(
                "jcr:content/renditions/${regex(\".+/(.*)\\.jpg\")}.xml", 
                "/content/images/abc.tiff"));
        assertEquals("/fileTypes/jpg", 
                PrefetchCursor.resolve(
                "/fileTypes/${regex(\".+/.*\\.(.*)\")}", 
                "/content/images/abc.jpg"));
    }
    
    @Test
    public void cursor() {
        List<String> paths = Arrays.asList(
                "/test/a.jpg", 
                "/test/b.tiff", 
                "/test/c.pdf");
        TestCursor cursor = new TestCursor(paths.iterator());
        TestPrefetchNodeStore ns = new TestPrefetchNodeStore();
        NodeState rootState = null;
        List<String> prefetchRelative = Arrays.asList(
                "jcr:content/metadata",
                "jcr:content/renditions/${regex(\".+/(.*)\\.jpg\")}.xml",
                "/fileTypes/${regex(\".+/.*\\.(.*)\")}");
        PrefetchCursor pc = new PrefetchCursor(cursor, ns, 10, rootState, prefetchRelative);
        assertEquals("/test/a.jpg, /test/b.tiff, /test/c.pdf", 
                CursorUtils.toString(pc));
        assertEquals("[/fileTypes/jpg, /fileTypes/pdf, /fileTypes/tiff, " + 
                "/test, /test/a.jpg, /test/a.jpg/jcr:content/metadata, " + 
                "/test/a.jpg/jcr:content/renditions/a.xml, " + 
                "/test/b.tiff, /test/b.tiff/jcr:content/metadata, " + 
                "/test/c.pdf, /test/c.pdf/jcr:content/metadata]", ns.toString());
    }
    
    @Test
    public void cursorWithUnsupportedFeature() {
        List<String> paths = Arrays.asList(
                "/test/a.jpg", "abc");
        TestCursor cursor = new TestCursor(paths.iterator());
        TestPrefetchNodeStore ns = new TestPrefetchNodeStore();
        NodeState rootState = null;
        List<String> prefetchRelative = Arrays.asList(
                "jcr:content/${regex(\"jpg\")}",
                "jcr:content/${test(\\\"***\\\")}");
        PrefetchCursor pc = new PrefetchCursor(cursor, ns, 10, rootState, prefetchRelative);
        assertEquals("/test/a.jpg, abc",
                CursorUtils.toString(pc));
        assertEquals("[/test, /test/a.jpg]", ns.toString());
    }

    @Test
    public void cursorWithVirtualRow() {
        List<String> paths = Arrays.asList(
                "/test/a.jpg");
        TestCursorVirtual cursor = new TestCursorVirtual(paths.iterator());
        TestPrefetchNodeStore ns = new TestPrefetchNodeStore();
        NodeState rootState = null;
        List<String> prefetchRelative = Arrays.asList(
                "jcr:content");
        PrefetchCursor pc = new PrefetchCursor(cursor, ns, 10, rootState, prefetchRelative);
        assertEquals("/test/a.jpg",
                CursorUtils.toString(pc));
        assertEquals("[]", ns.toString());
    }

    @Test
    public void cursorWithManyResults() {
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            paths.add("/test/n" + i);
        }
        TestCursor cursor = new TestCursor(paths.iterator());
        TestPrefetchNodeStore ns = new TestPrefetchNodeStore();
        NodeState rootState = null;
        List<String> prefetchRelative = Arrays.asList("jcr:content/metadata");
        PrefetchCursor pc = new PrefetchCursor(cursor, ns, 10, rootState, prefetchRelative);
        for (int i = 0; i < 1000; i++) {
            assertTrue(pc.hasNext());
            assertEquals("/test/n" + i, pc.next().getPath().toString());
        }
        assertFalse(pc.hasNext());
    }

}
