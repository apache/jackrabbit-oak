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
package org.apache.jackrabbit.oak.plugins.index.old.mk.large;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl;

/**
 * A utility to create a number of nodes in a random tree structure. Each level
 * has a random (but fixed for the level) number of nodes with at most maxWidth
 * nodes.
 */
public class RandomNodeCreator {

    private static final String DEFAULT_TEST_ROOT = "testRandom";
    private static final int DEFAULT_COUNT = 200;
    private static final int DEFAULT_WIDTH = 30;

    private final MicroKernel mk;
    private final Random random;

    private String testRoot = DEFAULT_TEST_ROOT;
    private int totalCount = DEFAULT_COUNT;
    private int maxWidth = DEFAULT_WIDTH;
    private boolean logToSystemOut;
    private String head;
    private int count;
    private Queue<String> queue = new LinkedList<String>();

    public RandomNodeCreator(MicroKernel mk, int seed) {
        this.mk = mk;
        head = mk.getHeadRevision();
        random = new Random(seed);
    }

    public void setTestRoot(String testRoot) {
        this.testRoot = testRoot;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public void setMaxWidth(int maxWidth) {
        this.maxWidth = maxWidth;
    }

    public void setLogToSystemOut(boolean logToSystemOut) {
        this.logToSystemOut = logToSystemOut;
    }

    public void create() {
      log("Implementation: " + mk);
      log("Creating " + totalCount + " nodes under " + testRoot);
      head = mk.commit("/", "+\"" + testRoot + "\":{}", head, "");
      count = 0;
      createNodes(testRoot);
    }

    public void traverse() {
        count = 0;
        queue.clear();
        log("Traversing " + totalCount + " nodes");
        traverse(testRoot);
    }

    private void createNodes(String parent) {
        if (count >= totalCount) {
            return;
        }

        int width = random.nextInt(maxWidth) + 1;

        head = mk.commit("/" + parent, "^ \"width\":" + width, head, null);

        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < width; i++) {
            if (count >= totalCount) {
                break;
            }

            String p = parent + "/node" + count;
            queue.add(p);
            buff.append("+ \"" + p + "\": {");
            buff.append("}\n");
            count++;
        }
        head = mk.commit("/", buff.toString(), head, "");
        log("Committed with width: " + width + "\n" + buff.toString());

        while (!queue.isEmpty()) {
            createNodes(queue.poll());
        }
    }

    private void traverse(String parent) {
        if (count >= totalCount) {
            return;
        }

        String parentJson = JsopBuilder.prettyPrint(mk.getNodes("/" + parent, mk.getHeadRevision(), 1, 0, -1, null));
        NodeImpl parentNode = NodeImpl.parse(parentJson);
        int width = Integer.parseInt(parentNode.getProperty("width"));

        for (int i = 0; i < width; i++) {
            if (count >= totalCount) {
                break;
            }

            String p = parent + "/node" + count;
            log("Traversed: " + p);
            if (!mk.nodeExists("/" + p, head)) {
                break;
            }
            mk.getNodes("/" + p, head, 1, 0, -1, null);
            queue.add(p);
            count++;
        }

        while (!queue.isEmpty()) {
            traverse(queue.poll());
        }
    }

    private void log(String s) {
        if (logToSystemOut) {
            System.out.println(s);
        }
    }

}