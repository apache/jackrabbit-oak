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
package org.apache.jackrabbit.mongomk.perf;

import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.util.NodeBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;

public class RandomJsopGenerator {

    private static final int OP_ADD_NODE = 0;
    private static final int OP_SET_PROP = 1;

    private Node[] descendants;
    private String path;
    private Random random;

    public RandomJsopGenerator() throws Exception {
        this.setSeed("", "{ \"/\" : {} }");
    }

    public RandomJsop nextRandom() {
        JsopBuilder jsopBuilder = new JsopBuilder();

        int numOps = random.nextInt(10) + 1;
        for (int i = 0; i < numOps; ++i) {
            if (this.createRandomOp(jsopBuilder)) {
                jsopBuilder.newline();
            } else {
                --i;
            }
        }

        return new RandomJsop(path, jsopBuilder.toString(), UUID.randomUUID().toString());
    }

    public void setSeed(String path, String json) throws Exception {
        this.path = path;
        String all = String.format("{ \"%s\" : %s }", PathUtils.getName(path), json);
        Node node = NodeBuilder.build(all, path);
        descendants = new Node[node.getChildNodeCount()];
        int i = 0;
        for (Iterator<Node> it = node.getChildNodeEntries(0, -1); it.hasNext(); ) {
            descendants[i++] = it.next();
        }
        random = new Random();
    }

    private boolean createRandomAddNodeOp(JsopBuilder jsopBuilder) {
        Node random = selectRandom();

        String childName = createRandomString();
        String newPath = PathUtils.concat(random.getPath(), childName);
        String addPath = newPath;
        if (!"".equals(path)) {
            addPath = PathUtils.relativize(path, newPath);
        }

        jsopBuilder.tag('+');
        jsopBuilder.key(addPath);
        jsopBuilder.object();
        jsopBuilder.endObject();

        return true;
    }

    private boolean createRandomSetPropOp(JsopBuilder jsopBuilder) {
        int next = random.nextInt(this.descendants.length);
        Node node = descendants[next];

        String addPath = PathUtils.relativize(path, node.getPath());
        if ("".equals(addPath)) {
            addPath = "/";
        }

        int numProps = random.nextInt(10) + 1;
        for (int i = 0; i < numProps; ++i) {
            String propName = createRandomString();
            String propValue = createRandomString();

            jsopBuilder.tag('^');
            jsopBuilder.key(PathUtils.concat(addPath, propName));
            jsopBuilder.value(propValue);
        }

        return true;
    }

    private boolean createRandomOp(JsopBuilder jsopBuilder) {
        boolean performed = false;

        int op = random.nextInt(2);

        switch (op) {
            case OP_ADD_NODE: {
                performed = createRandomAddNodeOp(jsopBuilder);
                break;
            }
            case OP_SET_PROP: {
                performed = createRandomSetPropOp(jsopBuilder);
                break;
            }
        }

        return performed;
    }

    private String createRandomString() {
        int length = random.nextInt(6) + 5;
        char[] chars = new char[length];
        for (int i = 0; i < length; ++i) {
            char rand = (char) (random.nextInt(65) + 59);
            if (Character.isLetterOrDigit(rand)) {
                chars[i] = rand;
            } else {
                --i;
            }
        }

        return new String(chars);
    }

    private Node selectRandom() {
        int next = random.nextInt(this.descendants.length);
        Node randomNode = descendants[next];
        return randomNode;
    }

    public static void main(String[] args) throws Exception {
        RandomJsopGenerator gen = new RandomJsopGenerator();
        for (int i = 0; i < 10; ++i) {
            RandomJsop rand = gen.nextRandom();
            System.out.println(rand.path);
            System.out.println(rand.jsop);
            System.out.println();
        }
    }

    public static class RandomJsop {

        private final String jsop;
        private final String message;
        private final String path;

        public RandomJsop(String path, String jsop, String message) {
            this.path = path;
            this.jsop = jsop;
            this.message = message;
        }

        public String getJsop() {
            return jsop;
        }

        public String getMessage() {
            return message;
        }

        public String getPath() {
            return path;
        }
    }
}