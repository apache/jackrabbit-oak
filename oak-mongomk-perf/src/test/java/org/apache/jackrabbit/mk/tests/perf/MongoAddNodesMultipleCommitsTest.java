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

import org.apache.jackrabbit.mk.scenarios.MicroKernelMultipleCommits;
import org.apache.jackrabbit.mk.testing.MicroKernelMongoTestBase;
import org.apache.jackrabbit.mk.util.MicroKernelOperation;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class MongoAddNodesMultipleCommitsTest extends MicroKernelMongoTestBase {
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
        MicroKernelMultipleCommits.writeNodesAllNodes1Commit(mk, diff,
                chronometer);
    }

    @Ignore
    @Test
    public void testWriteNodes1NodePerCommit() {
        MicroKernelMultipleCommits.writeNodes1NodePerCommit(mk, diff,
                chronometer);
    }

    @Test
    public void testWriteNodes50NodesPerCommit() {
        MicroKernelMultipleCommits.writeNodes50NodesPerCommit(mk, diff,
                chronometer);
    }

    @Test
    public void testWriteNodes1000NodesPerCommit() {
        MicroKernelMultipleCommits.writeNodes1000NodesPerCommit(mk, diff,
                chronometer);
    }

}
