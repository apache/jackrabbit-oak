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

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.NodeDocumentFieldFilter.NODE_DOCUMENT_FILTER_INCLUDE_PATH;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.NodeDocumentFieldFilter.NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeDocumentFieldFilterTest {

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    @Test
    public void filterInDirectory() {
        System.setProperty(NODE_DOCUMENT_FILTER_INCLUDE_PATH, "/foo/bar");
        System.setProperty(NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP, "/skip/me;/dont/include/me;/not_this_one");
        NodeDocumentFieldFilter nodeDocumentFieldFilter = new NodeDocumentFieldFilter();

        // nodes are in the include paths for filtering
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "5:/foo/bar/a/b/not_this_one"));
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/not_this_one"));
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "6:/foo/bar/a/b/skip/me"));
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/skip/me"));
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "7:/foo/bar/a/b/dont/include/me"));
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/dont/include/me"));

        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "4:/foo/bar/a/b"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "6:/foo/bar/a/b/not_this_one/child"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/bar/a/b/not_this_one/child"));


        // nodes are not in the include paths for filtering
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "4:/foo/a/b/not_this_one"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/a/b/not_this_one"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "5:/foo/a/b/skip/me"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/a/b/skip/me"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "6:/foo/a/b/dont/include/me"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/foo/a/b/dont/include/me"));
    }


    @Test
    public void filterIncludePathRoot() {
        System.setProperty(NODE_DOCUMENT_FILTER_INCLUDE_PATH, "/");
        System.setProperty(NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP, "/skip/me");
        NodeDocumentFieldFilter nodeDocumentFieldFilter = new NodeDocumentFieldFilter();

        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "1:/skip"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "1:/me"));
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "2:/skip/me"));
        assertTrue(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "3:/foo/skip/me"));
    }

    @Test
    public void filterDisabled() {
        System.setProperty(NODE_DOCUMENT_FILTER_INCLUDE_PATH, "");
        System.setProperty(NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP, "/not_this_one");
        NodeDocumentFieldFilter nodeDocumentFieldFilter = new NodeDocumentFieldFilter();
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "3:/a/b/not_this_one"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/a/b/not_this_one"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "1:/a"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/a"));
    }

    @Test
    public void emptySuffixList() {
        System.setProperty(NODE_DOCUMENT_FILTER_INCLUDE_PATH, "/");
        System.setProperty(NODE_DOCUMENT_FILTER_SUFFIXES_TO_SKIP, "");
        NodeDocumentFieldFilter nodeDocumentFieldFilter = new NodeDocumentFieldFilter();
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "3:/a/b/not_this_one"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.PATH, "/a/b/not_this_one"));
        assertFalse(nodeDocumentFieldFilter.shouldSkip(NodeDocument.ID, "/a"));
    }
}