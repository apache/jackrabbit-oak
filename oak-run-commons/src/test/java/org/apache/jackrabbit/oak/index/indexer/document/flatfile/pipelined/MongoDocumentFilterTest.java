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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoDocumentFilterTest {

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    @Test
    public void filterInDirectory() {
        var filter = new MongoDocumentFilter("/foo/bar", List.of("/skip/me", "/dont/include/me", "/not_this_one"));

        // nodes are in the include paths for filtering
        assertTrue(filter.shouldSkip(NodeDocument.ID, "5:/foo/bar/a/b/not_this_one"));
        assertTrue(filter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/not_this_one"));
        assertTrue(filter.shouldSkip(NodeDocument.ID, "6:/foo/bar/a/b/skip/me"));
        assertTrue(filter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/skip/me"));
        assertTrue(filter.shouldSkip(NodeDocument.ID, "7:/foo/bar/a/b/dont/include/me"));
        assertTrue(filter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/dont/include/me"));

        assertFalse(filter.shouldSkip(NodeDocument.ID, "4:/foo/bar/a/b"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b"));
        assertFalse(filter.shouldSkip(NodeDocument.ID, "6:/foo/bar/a/b/not_this_one/child"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/not_this_one/child"));


        // nodes are not in the include paths for filtering
        assertFalse(filter.shouldSkip(NodeDocument.ID, "4:/foo/a/b/not_this_one"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/foo/a/b/not_this_one"));
        assertFalse(filter.shouldSkip(NodeDocument.ID, "5:/foo/a/b/skip/me"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/foo/a/b/skip/me"));
        assertFalse(filter.shouldSkip(NodeDocument.ID, "6:/foo/a/b/dont/include/me"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/foo/a/b/dont/include/me"));
    }


    @Test
    public void filterIncludePathRoot() {
        var filter = new MongoDocumentFilter("/", List.of("/skip/me"));

        assertFalse(filter.shouldSkip(NodeDocument.ID, "1:/skip"));
        assertFalse(filter.shouldSkip(NodeDocument.ID, "1:/me"));
        assertTrue(filter.shouldSkip(NodeDocument.ID, "2:/skip/me"));
        assertTrue(filter.shouldSkip(NodeDocument.ID, "3:/foo/skip/me"));
    }

    @Test
    public void filterDisabled() {
        var filter = new MongoDocumentFilter("", List.of("/not_this_one"));
        assertFalse(filter.shouldSkip(NodeDocument.ID, "3:/a/b/not_this_one"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/a/b/not_this_one"));
        assertFalse(filter.shouldSkip(NodeDocument.ID, "1:/a"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/a"));
    }

    @Test
    public void emptySuffixList() {
        var filter = new MongoDocumentFilter("/", List.of());
        assertFalse(filter.shouldSkip(NodeDocument.ID, "3:/a/b/not_this_one"));
        assertFalse(filter.shouldSkip(NodeDocument.PATH, "/a/b/not_this_one"));
        assertFalse(filter.shouldSkip(NodeDocument.ID, "/a"));
    }
}