/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.impl.command;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for multiple MongoMKs writing against the same DB in separate trees.
 */
public class ConcurrentWriteMultipleMkMongoTest extends
        AbstractMongoConnectionTest {

    @Test
    public void testSmall() throws Exception {
        doTest(1000);
    }

    @Test
    // Ignored only because it takes a while to complete.
    @Ignore
    public void testLarge() throws Exception {
        doTest(10000);
    }

    private void doTest(int numberOfNodes) throws Exception {

        int numberOfChildren = 10;
        int numberOfMks = 3;
        String[] prefixes = new String[]{"a", "b", "c", "d", "e", "f"};

        ExecutorService executor = Executors.newFixedThreadPool(numberOfMks);
        List<DocumentMK> mks = new ArrayList<DocumentMK>();
        for (int i = 0; i < numberOfMks; i++) {
            String diff = buildPyramidDiff("/", 0, numberOfChildren,
                    numberOfNodes, prefixes[i], new StringBuilder()).toString();
            DocumentMK mk = new DocumentMK.Builder().open();
            mks.add(mk);
            GenericWriteTask task = new GenericWriteTask("mk-" + i, mk, diff, 10);
            executor.execute(task);
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
    }

    private StringBuilder buildPyramidDiff(String startingPoint,
            int index, int numberOfChildren, long nodesNumber,
            String nodePrefixName, StringBuilder diff) {
        if (numberOfChildren == 0) {
            for (long i = 0; i < nodesNumber; i++) {
                diff.append(addNodeToDiff(startingPoint, nodePrefixName + i));
            }
            return diff;
        }

        if (index >= nodesNumber) {
            return diff;
        }

        diff.append(addNodeToDiff(startingPoint, nodePrefixName + index));
        for (int i = 1; i <= numberOfChildren; i++) {
            if (!startingPoint.endsWith("/")) {
                startingPoint = startingPoint + "/";
            }
            buildPyramidDiff(startingPoint + nodePrefixName + index, index
                    * numberOfChildren + i, numberOfChildren, nodesNumber,
                    nodePrefixName, diff);
        }
        return diff;
    }

    private static String addNodeToDiff(String startingPoint, String nodeName) {
        if (!startingPoint.endsWith("/")) {
            startingPoint = startingPoint + "/";
        }
        return "+\"" + startingPoint + nodeName + "\" : {} \n";
    }

    /**
     * A simple write task.
     */
    private static class GenericWriteTask implements Runnable {

        private String id;
        private DocumentMK mk;
        private String diff;
        private int nodesPerCommit;

        public GenericWriteTask(String id, DocumentMK mk, String diff,
                int nodesPerCommit) {
            this.id = id;
            this.mk = mk;
            this.diff = diff;
            this.nodesPerCommit = nodesPerCommit;
        }

        @Override
        public String toString() {
            return id;
        }

        @Override
        public void run() {
            if (nodesPerCommit == 0) {
                mk.commit("", diff.toString(), null, "");
                return;
            }

            int i = 0;
            StringBuilder currentCommit = new StringBuilder();
            String[] diffs = diff.split(System.getProperty("line.separator"));
            for (String diff : diffs) {
                currentCommit.append(diff);
                i++;
                if (i == nodesPerCommit) {
                    //System.out.println("[" + id + "] Committing: " + currentCommit.toString());
                    mk.commit("", currentCommit.toString(), null, null);
                    //System.out.println("[" + id + "] Committed-" + rev + ":" + currentCommit.toString());
                    currentCommit.setLength(0);
                    i = 0;
                }
            }
            // Commit remaining nodes
            if (currentCommit.length() > 0) {
                mk.commit("", currentCommit.toString(), null, null);
            }
        }
    }
}