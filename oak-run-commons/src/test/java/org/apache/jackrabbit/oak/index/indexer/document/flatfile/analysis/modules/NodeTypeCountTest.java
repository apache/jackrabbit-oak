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

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.junit.Test;

public class NodeTypeCountTest {

    @Test
    public void nodeCount() {
        NodeTypeCount nc = new NodeTypeCount();
        for (int i = 0; i < 10_000; i++) {
            NodeProperty p1, p2;
            if (i % 10 == 0) {
                p1 = new NodeProperty("jcr:primaryType", ValueType.NAME, "nt:unstructured");
                p2 = new NodeProperty("jcr:mixinTypes", ValueType.NAME, new String[] {"mix:a", "mix:b"}, true);
            } else {
                p1 = new NodeProperty("jcr:primaryType", ValueType.NAME, "oak:unstructured");
                p2 = new NodeProperty("jcr:mixinTypes", ValueType.NAME, new String[] {"mix:a", "mix:c"}, true);
            }
            NodeData n = new NodeData(Arrays.asList("content", "dam", "folder" + (i % 10), "n" + i), Arrays.asList(p1, p2));
            nc.add(n);
        }
        assertEquals(
                "NodeTypeCount\n"
                + "mixin/mix:a: 10000\n"
                + "mixin/mix:b: 1000\n"
                + "mixin/mix:c: 9000\n"
                + "primaryType/nt:unstructured: 1000\n"
                + "primaryType/oak:unstructured: 9000\n"
                + "storage size: 0 MB; 5 entries\n"
                + "", nc.toString());
    }

}
