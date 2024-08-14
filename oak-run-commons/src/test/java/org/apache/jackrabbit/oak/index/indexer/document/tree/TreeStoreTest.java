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
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedTreeStoreTask;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TreeStoreTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void convertPathTest() {
        assertEquals("\t", TreeStore.toChildNodeEntry("/"));
        assertEquals("/\tabc", TreeStore.toChildNodeEntry("/abc"));
        assertEquals("/hello\tworld", TreeStore.toChildNodeEntry("/hello/world"));

        assertEquals("/\tabc", TreeStore.toChildNodeEntry("/", "abc"));
        assertEquals("/hello\tworld", TreeStore.toChildNodeEntry("/hello", "world"));
    }

    @Test
    public void buildAndIterateTest() throws IOException {
        File testFolder = temporaryFolder.newFolder();
        TreeStore store = new TreeStore("test", testFolder, null, 1);
        try {
            store.getSession().init();
            PipelinedTreeStoreTask.addEntry("/", "{}", store.getSession());
            PipelinedTreeStoreTask.addEntry("/content", "{}", store.getSession());
            Iterator<String> it = store.pathIterator();
            assertEquals("/", it.next());
            assertEquals("/content", it.next());
        } finally {
            store.close();
        }
    }


}
