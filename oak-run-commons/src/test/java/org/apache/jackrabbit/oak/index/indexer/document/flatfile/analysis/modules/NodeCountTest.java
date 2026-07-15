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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.junit.Test;

public class NodeCountTest {

    @Test
    public void nodeCount() {
        NodeCount nc = new NodeCount(1024, 42);

        for (int i = 0; i < 10_000_000; i++) {
            NodeData n = new NodeData(Arrays.asList("content", "dam", "folder" + (i % 10), "n" + i), Collections.emptyList());
            nc.add(n);
        }
        assertEquals(
                "NodeCount in million\n"
                + "/: 10\n"
                + "/content: 10\n"
                + "/content/dam: 10\n"
                + "/content/dam/folder1: 1\n"
                + "/content/dam/folder2: 1\n"
                + "/content/dam/folder3: 1\n"
                + "/content/dam/folder4: 1\n"
                + "/content/dam/folder6: 1\n"
                + "/content/dam/folder7: 1\n"
                + "/content/dam/folder8: 1\n"
                + "storage size: 0 MB; 4900 entries\n"
                + "", nc.toString());
    }

}
