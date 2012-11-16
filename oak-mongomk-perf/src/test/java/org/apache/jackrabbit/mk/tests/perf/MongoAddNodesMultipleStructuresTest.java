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
package org.apache.jackrabbit.mk.tests.perf;

import org.apache.jackrabbit.mk.scenarios.MicrokernelDifferentStructures;
import org.apache.jackrabbit.mk.testing.MicroKernelMongoTestBase;

import org.junit.Test;

public class MongoAddNodesMultipleStructuresTest extends
        MicroKernelMongoTestBase {

    static long nodesNumber = 100;
    static String nodeNamePrefix = "N";

    /**
     * Tree structure:
     * <p>
     * rootNode (/)
     * <p>
     * N0 N1... Nn-1 Nn
     */
    @Test
    public void testWriteNodesSameLevel() {
        MicrokernelDifferentStructures.writeNodesSameLevel(mk, chronometer,
                nodesNumber, nodeNamePrefix);
    }

    /**
     * Tree structure:
     * <p>
     * rootNode (/)
     * <p>
     * N0
     * <p>
     * N1
     * <p>
     * N2
     * <p>
     * N3
     */
    @Test
    public void testWriteNodes1Child() {
        MicrokernelDifferentStructures.writeNodes1Child(mk, chronometer,
                nodesNumber, nodeNamePrefix);
    }

    /**
     * Tree structure:
     * <p>
     * Number of nodes per <b>level</b> =10^(<b>level</b>).
     * <p>
     * Each node has 10 children.
     */
    @Test
    public void testWriteNodes10Children() {
        MicrokernelDifferentStructures.writeNodes10Children(mk, chronometer,
                nodesNumber, nodeNamePrefix);
    }

    /**
     * Tree structure:
     * <p>
     * Number of nodes per <b>level</b> =100^(<b>level</b>).
     * <p>
     * Each node has 100 children.
     */
    @Test
    public void testWriteNodes100Children() {
        MicrokernelDifferentStructures.writeNodes100Children(mk, chronometer,
                nodesNumber, nodeNamePrefix);
    }

    /**
     * Tree structure:
     * <p>
     * Number of nodes per <b>level</b> =1000^(<b>level</b>).
     * <p>
     * Each node has 1000 children.
     */
    @Test
    public void testWriteNodes1000Children() {
        MicrokernelDifferentStructures.writeNodes1000Children(mk, chronometer,
                nodesNumber, nodeNamePrefix);
    }
}
