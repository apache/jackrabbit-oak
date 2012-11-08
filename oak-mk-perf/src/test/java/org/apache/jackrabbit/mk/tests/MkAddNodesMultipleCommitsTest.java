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
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Measure the time needed for writing the same node structure in one or
 * multiple commit steps.
 * <p>
 * Tree structure:
 * <p>
 * Number of nodes per <b>level</b> =100^(<b>level</b>).
 * <p>
 * Each node has 100 children.
 */
public class MkAddNodesMultipleCommitsTest extends MicroKernelTestBase {

    static String diff;
    static int nodesNumber = 1000;
    static String nodeNamePrefix = "N";

    @BeforeClass
    public static void prepareDiff() {
        diff = MicroKernelOperation.buildPyramidDiff("/", 0, 10, nodesNumber,
                nodeNamePrefix, new StringBuilder()).toString();
    }

    @Test
    public void testWriteNodesAllNodes1Commit() {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 0);
        chronometer.stop();
        System.out.println("Total time for testWriteNodesAllNodes1Commit is "
                + chronometer.getSeconds());
    }

    @Test
    public void testWriteNodes1NodePerCommit() {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 1);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes1NodePerCommit is "
                + chronometer.getSeconds());
    }

    @Test
    public void testWriteNodes50NodesPerCommit() {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 50);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes50NodesPerCommit is "
                + chronometer.getSeconds());
    }

    @Test
    public void testWriteNodes1000NodesPerCommit() {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 10);
        chronometer.stop();
        System.out
                .println("Total time for testWriteNodes1000NodesPerCommit is "
                        + chronometer.getSeconds());
    }
}