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
package org.apache.jackrabbit.mk.util;

import org.apache.jackrabbit.mk.api.MicroKernel;

/**
 * A utility to create a (large) number of nodes in a tree structure.
 */
public class NodeCreator {

    private final MicroKernel mk;
    private String head;
    private int totalCount = 200;
    private int width = 30, count;
    private StopWatch timer;
    private String nodeName = "test";
    private String data = "Hello World";
    private boolean logToSystemOut;

    public NodeCreator(MicroKernel mk) {
        this.mk = mk;
        head = mk.getHeadRevision();
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setData(String data) {
        this.data = data;
    }

    public void create() {
        log("implementation: " + mk);
        log("creating " + totalCount + " nodes");
        head = mk.commit("/", "+\"" + nodeName + "\":{}", head, "");
        timer = new StopWatch();
        count = 0;
        int depth = (int) Math.ceil(Math.log(totalCount) / Math.log(width));
        log("depth: " + depth);
        createNodes(nodeName, depth);
        log("created  " + count + " nodes in " + timer.operationsPerSecond(count));
        log("");
    }

    public void traverse() {
        timer = new StopWatch();
        count = 0;
        int depth = (int) Math.ceil(Math.log(totalCount) / Math.log(width));
        log("depth: " + depth);
        traverse(nodeName, depth);
        log("read  " + count + " nodes in " + timer.operationsPerSecond(count));
    }

    private void createNodes(String parent, int depth) {
        if (count >= totalCount) {
            return;
        }
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < width; i++) {
            if (count >= totalCount && depth == 0) {
                break;
            }
            String p = parent + "/node" + depth + i;
            buff.append("+ \"" + p + "\": {");
            if (data != null) {
                buff.append("\"data\":\"").append(data).append(" ").
                    append(count).append("\"");
            }
            count++;
            buff.append("}\n");
            if (count % 1000 == 0 && timer.log()) {
                log("  " + count + " nodes in " + timer.operationsPerSecond(count));
            }
        }
        head = mk.commit("/", buff.toString(), head, "");
        if (depth > 0) {
            for (int i = 0; i < width; i++) {
                String p = parent + "/node" + depth + i;
                createNodes(p, depth - 1);
            }
        }
    }

    private void traverse(String parent, int depth) {
        if (count >= totalCount) {
            return;
        }
        for (int i = 0; i < width; i++) {
            if (count >= totalCount && depth == 0) {
                break;
            }
            String p = parent + "/node" + depth + i;
            if (!mk.nodeExists("/" + p, head)) {
                break;
            }
            mk.getNodes("/" + p, head);
            count++;
            if (count % 1000 == 0 && timer.log()) {
                log("  " + count + " nodes in " + timer.operationsPerSecond(count));
            }
            if (depth > 0) {
                traverse(p, depth - 1);
            }
        }
    }

    private void log(String s) {
        if (logToSystemOut) {
            System.out.println(s);
        }
    }

    public void setLogToSystemOut(boolean logToSystemOut) {
        this.logToSystemOut = logToSystemOut;
    }

}
