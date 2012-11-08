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

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.tasks.GenericWriteTask;
import org.apache.jackrabbit.mk.testing.ConcurrentMicroKernelTestBase;
import org.apache.jackrabbit.mk.util.MicroKernelOperation;
import org.junit.Test;

/**
 * Test class for microkernel concurrent writing.The microkernel is adding 1000
 * nodes per commit.
 */
public class MkConcurrentAddNodesMultipleCommitTest extends
        ConcurrentMicroKernelTestBase {

    // nodes for each worker
    int nodesNumber = 100;
    int numberOfNodesPerCommit = 10;

    /**
     * @Rule public CatchAllExceptionsRule catchAllExceptionsRule = new
     *       CatchAllExceptionsRule();
     */
    @Test
    public void testConcurentWritingFlatStructure() throws InterruptedException {

        int children = 0;
        ArrayList<GenericWriteTask> tasks = new ArrayList<GenericWriteTask>();
        String diff;
        for (int i = 0; i < mkNumber; i++) {
            diff = MicroKernelOperation.buildPyramidDiff("/", 0, children,
                    nodesNumber, "N" + i + "N", new StringBuilder()).toString();
            tasks.add(new GenericWriteTask(mks.get(i), diff,
                    numberOfNodesPerCommit));
            System.out.println("The diff size is " + diff.getBytes().length);
        }

        ExecutorService threadExecutor = Executors.newFixedThreadPool(mkNumber);
        chronometer.start();
        for (GenericWriteTask genericWriteTask : tasks) {
            threadExecutor.execute(genericWriteTask);
        }
        threadExecutor.shutdown();
        threadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        chronometer.stop();
        System.out.println("Total time is " + chronometer.getSeconds());
    }

    @Test
    public void testConcurentWritingPyramid1() throws InterruptedException {
        int children = 15;
        ArrayList<GenericWriteTask> tasks = new ArrayList<GenericWriteTask>();
        String diff;
        for (int i = 0; i < mkNumber; i++) {
            diff = MicroKernelOperation.buildPyramidDiff("/", 0, children,
                    nodesNumber, "N" + i + "N", new StringBuilder()).toString();
            tasks.add(new GenericWriteTask(mks.get(i), diff,
                    numberOfNodesPerCommit));
            System.out.println("The diff size is " + diff.getBytes().length);
        }

        ExecutorService threadExecutor = Executors.newFixedThreadPool(mkNumber);
        chronometer.start();
        for (GenericWriteTask genericWriteTask : tasks) {
            threadExecutor.execute(genericWriteTask);
        }
        threadExecutor.shutdown();
        threadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        chronometer.stop();
        System.out.println("Total time is " + chronometer.getSeconds());
    }

    @Test
    public void testConcurentWritingPyramid2() throws InterruptedException {
        int children = 2;
        ArrayList<GenericWriteTask> tasks = new ArrayList<GenericWriteTask>();
        String diff;
        for (int i = 0; i < mkNumber; i++) {

            diff = MicroKernelOperation.buildPyramidDiff("/", 0, children,
                    nodesNumber, "N" + i + "N", new StringBuilder()).toString();

            tasks.add(new GenericWriteTask(mks.get(i), diff,
                    numberOfNodesPerCommit));

        }

        ExecutorService threadExecutor = Executors.newFixedThreadPool(mkNumber);
        chronometer.start();
        for (GenericWriteTask genericWriteTask : tasks) {
            threadExecutor.execute(genericWriteTask);
        }
        threadExecutor.shutdown();
        threadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        chronometer.stop();
        System.out.println("Total time for is " + chronometer.getSeconds());
    }
}
