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

public class MicroKernelRelativePath {

    public static void writeNodesSameLevel(MicroKernel mk,
            Chronometer chronometer, long nodesNumber, String nodeNamePrefix)
            throws Exception {
        Committer commiter = new Committer();
        chronometer.start();
        commiter.addPyramidStructure(mk, "/", 0, 0, nodesNumber, nodeNamePrefix);
        chronometer.stop();
        System.out.println("Total time for testWriteNodesSameLevel is "
                + chronometer.getSeconds());
    }

    public static void writeNodes10Children(MicroKernel mk,
            Chronometer chronometer, long nodesNumber, String nodeNamePrefix) {
        Committer commiter = new Committer();
        chronometer.start();

        commiter.addPyramidStructure(mk, "/", 0, 10, nodesNumber,
                nodeNamePrefix);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes10Children is "
                + chronometer.getSeconds());
    }

    public static void writeNodes100Children(MicroKernel mk,
            Chronometer chronometer, long nodesNumber, String nodeNamePrefix) {
        Committer commiter = new Committer();
        chronometer.start();
        commiter.addPyramidStructure(mk, "/", 0, 100, nodesNumber,
                nodeNamePrefix);
        chronometer.stop();
        System.out.println("Total time for testWriteNodes100Children is "
                + chronometer.getSeconds());
    }
}