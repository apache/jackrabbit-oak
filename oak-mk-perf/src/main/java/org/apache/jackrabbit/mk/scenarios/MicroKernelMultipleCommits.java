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
package org.apache.jackrabbit.mk.scenarios;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.util.Chronometer;
import org.apache.jackrabbit.mk.util.Committer;

public class MicroKernelMultipleCommits {

    public static void writeNodesAllNodes1Commit(MicroKernel mk, String diff,
            Chronometer chronometer) {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 0);
        chronometer.stop();
        System.out.println("Total time for testWriteNodesAllNodes1Commit is "
                + chronometer.getSeconds());
    }

    public static void writeNodes1NodePerCommit(MicroKernel mk, String diff,
            Chronometer chronometer) {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 1);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes1NodePerCommit is "
                + chronometer.getSeconds());
    }

    public static void writeNodes50NodesPerCommit(MicroKernel mk, String diff,
            Chronometer chronometer) {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 50);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes50NodesPerCommit is "
                + chronometer.getSeconds());
    }

    public static void writeNodes1000NodesPerCommit(MicroKernel mk,
            String diff, Chronometer chronometer) {

        Committer commiter = new Committer();
        chronometer.start();
        commiter.addNodes(mk, diff, 10);
        chronometer.stop();
        System.out
                .println("Total time for testWriteNodes1000NodesPerCommit is "
                        + chronometer.getSeconds());
    }

}
