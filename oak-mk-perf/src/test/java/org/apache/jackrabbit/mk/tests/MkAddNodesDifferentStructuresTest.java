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
package org.apache.jackrabbit.mk.tests;

import org.apache.jackrabbit.mk.util.MicroKernelOperation;
import org.apache.jackrabbit.mk.testing.MicroKernelTestBase;
import org.apache.jackrabbit.mk.util.Committer;
import org.junit.Test;

/**
 * Measure the time needed for writing nodes in different tree structures.All
 * the nodes are added in a single commit.
 */
public class MkAddNodesDifferentStructuresTest extends MicroKernelTestBase {

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

        String diff = MicroKernelOperation.buildPyramidDiff("/", 0, 0,
                nodesNumber, nodeNamePrefix, new StringBuilder()).toString();
        Committer committer = new Committer();
        chronometer.start();
        committer.addNodes(mk, diff, 0);
        chronometer.stop();
        System.out.println("Total time for testWriteNodesSameLevel is "
                + chronometer.getSeconds());
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
        int nodesNumber = 100;

        String diff = MicroKernelOperation.buildPyramidDiff("/", 0, 1,
                nodesNumber, nodeNamePrefix, new StringBuilder()).toString();
        Committer committer = new Committer();
        chronometer.start();
        committer.addNodes(mk, diff, 0);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes1Child is "
                + chronometer.getSeconds());
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

        String diff = MicroKernelOperation.buildPyramidDiff("/", 0, 10,
                nodesNumber, nodeNamePrefix, new StringBuilder()).toString();
        Committer committer = new Committer();
        chronometer.start();
        committer.addNodes(mk, diff, 0);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes10Children is "
                + chronometer.getSeconds());
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

        String diff = MicroKernelOperation.buildPyramidDiff("/", 0, 100,
                nodesNumber, nodeNamePrefix, new StringBuilder()).toString();
        Committer committer = new Committer();
        chronometer.start();
        committer.addNodes(mk, diff, 0);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes100Children is "
                + chronometer.getSeconds());
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
        String diff = MicroKernelOperation.buildPyramidDiff("/", 0, 1000,
                nodesNumber, nodeNamePrefix, new StringBuilder()).toString();
        Committer committer = new Committer();
        chronometer.start();
        committer.addNodes(mk, diff, 0);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes1000Children is "
                + chronometer.getSeconds());
    }
}
