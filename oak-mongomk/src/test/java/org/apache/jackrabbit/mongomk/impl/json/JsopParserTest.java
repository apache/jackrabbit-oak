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
package org.apache.jackrabbit.mongomk.impl.json;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.mongomk.impl.json.DefaultJsopHandler;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
@SuppressWarnings("javadoc")
public class JsopParserTest {

    private static class CountingHandler extends DefaultJsopHandler {

        private static class Node {
            private final String jsop;
            private final String path;

            Node(String jsop, String path) {
                this.jsop = jsop;
                this.path = path;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (this.getClass() != obj.getClass()) {
                    return false;
                }
                Node other = (Node) obj;
                if (this.jsop == null) {
                    if (other.jsop != null) {
                        return false;
                    }
                } else if (!this.jsop.equals(other.jsop)) {
                    return false;
                }
                if (this.path == null) {
                    if (other.path != null) {
                        return false;
                    }
                } else if (!this.path.equals(other.path)) {
                    return false;
                }
                return true;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = (prime * result) + ((this.jsop == null) ? 0 : this.jsop.hashCode());
                result = (prime * result) + ((this.path == null) ? 0 : this.path.hashCode());
                return result;
            }
        }

        private static class NodeMoved {
            private final String newPath;
            private final String oldPath;
            private final String rootPath;

            NodeMoved(String rootPath, String oldPath, String newPath) {
                this.rootPath = rootPath;
                this.oldPath = oldPath;
                this.newPath = newPath;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (this.getClass() != obj.getClass()) {
                    return false;
                }
                NodeMoved other = (NodeMoved) obj;
                if (this.newPath == null) {
                    if (other.newPath != null) {
                        return false;
                    }
                } else if (!this.newPath.equals(other.newPath)) {
                    return false;
                }
                if (this.oldPath == null) {
                    if (other.oldPath != null) {
                        return false;
                    }
                } else if (!this.oldPath.equals(other.oldPath)) {
                    return false;
                }
                if (this.rootPath == null) {
                    if (other.rootPath != null) {
                        return false;
                    }
                } else if (!this.rootPath.equals(other.rootPath)) {
                    return false;
                }
                return true;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = (prime * result) + ((this.newPath == null) ? 0 : this.newPath.hashCode());
                result = (prime * result) + ((this.oldPath == null) ? 0 : this.oldPath.hashCode());
                result = (prime * result) + ((this.rootPath == null) ? 0 : this.rootPath.hashCode());
                return result;
            }

        }

        private static class Property {
            private final String key;
            private final String path;
            private final Object value;

            Property(String path, String key, Object value) {
                this.path = path;
                this.key = key;
                this.value = value;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (obj == null) {
                    return false;
                }
                if (this.getClass() != obj.getClass()) {
                    return false;
                }
                Property other = (Property) obj;
                if (this.key == null) {
                    if (other.key != null) {
                        return false;
                    }
                } else if (!this.key.equals(other.key)) {
                    return false;
                }
                if (this.path == null) {
                    if (other.path != null) {
                        return false;
                    }
                } else if (!this.path.equals(other.path)) {
                    return false;
                }
                if (this.value == null) {
                    if (other.value != null) {
                        return false;
                    }
                } else if (this.value instanceof Object[]) {
                    return Arrays.deepEquals((Object[]) this.value, (Object[]) other.value);
                } else if (!this.value.equals(other.value)) {
                    return false;
                }
                return true;
            }

            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = (prime * result) + ((this.key == null) ? 0 : this.key.hashCode());
                result = (prime * result) + ((this.path == null) ? 0 : this.path.hashCode());
                result = (prime * result) + ((this.value == null) ? 0 : this.value.hashCode());
                return result;
            }
        }

        private final List<Node> nodesAdded;
        private final List<NodeMoved> nodesCopied;
        private final List<NodeMoved> nodesMoved;
        private final List<Node> nodesRemoved;
        private final List<Property> propertiesAdded;
        private final List<Property> propertiesSet;

        CountingHandler() {
            this.nodesAdded = new LinkedList<Node>();
            this.nodesCopied = new LinkedList<NodeMoved>();
            this.nodesMoved = new LinkedList<NodeMoved>();
            this.nodesRemoved = new LinkedList<Node>();
            this.propertiesAdded = new LinkedList<Property>();
            this.propertiesSet = new LinkedList<Property>();
        }

        public void assertNodeCopied(String parentPath, String oldPath, String newPath) {
            NodeMoved expected = new NodeMoved(parentPath, oldPath, newPath);

            int firstIndex = this.nodesCopied.indexOf(expected);
            int lastIndex = this.nodesCopied.lastIndexOf(expected);

            Assert.assertTrue(firstIndex != -1);
            Assert.assertEquals(firstIndex, lastIndex);
        }

        public void assertNodeMoved(String parentPath, String oldPath, String newPath) {
            NodeMoved expected = new NodeMoved(parentPath, oldPath, newPath);

            int firstIndex = this.nodesMoved.indexOf(expected);
            int lastIndex = this.nodesMoved.lastIndexOf(expected);

            Assert.assertTrue(firstIndex != -1);
            Assert.assertEquals(firstIndex, lastIndex);
        }

        public void assertNodeRemoved(String path, String name) {
            Node expected = new Node(path, name);

            int firstIndex = this.nodesRemoved.indexOf(expected);
            int lastIndex = this.nodesRemoved.lastIndexOf(expected);

            Assert.assertTrue(firstIndex != -1);
            Assert.assertEquals(firstIndex, lastIndex);
        }

        public void assertNoOfNodesCopied(int num) {
            Assert.assertEquals(num, this.nodesCopied.size());
        }

        public void assertNoOfNodesMoved(int num) {
            Assert.assertEquals(num, this.nodesMoved.size());
        }

        public void assertNoOfNodesRemoved(int num) {
            Assert.assertEquals(num, this.nodesRemoved.size());
        }

        public void assertNoOfPropertiesSet(int num) {
            Assert.assertEquals(num, this.propertiesSet.size());
        }

        public void assertPropertiesAdded(int num) {
            Assert.assertEquals(num, this.propertiesAdded.size());
        }

        @Override
        public void nodeAdded(String path, String name) {
            this.nodesAdded.add(new Node(path, name));
        }

        @Override
        public void nodeCopied(String rootPath, String oldPath, String newPath) {
            this.nodesCopied.add(new NodeMoved(rootPath, oldPath, newPath));
        }

        @Override
        public void nodeMoved(String rootPath, String oldPath, String newPath) {
            this.nodesMoved.add(new NodeMoved(rootPath, oldPath, newPath));
        }

        @Override
        public void nodeRemoved(String path, String name) {
            this.nodesRemoved.add(new Node(path, name));
        }

        @Override
        public void propertyAdded(String path, String key, Object value) {
            this.propertiesAdded.add(new Property(path, key, value));
        }

        @Override
        public void propertySet(String path, String key, Object value) {
            this.propertiesSet.add(new Property(path, key, value));
        }

        void assertNodeAdded(String path, String name) {
            Node expected = new Node(path, name);

            int firstIndex = this.nodesAdded.indexOf(expected);
            int lastIndex = this.nodesAdded.lastIndexOf(expected);

            Assert.assertTrue(firstIndex != -1);
            Assert.assertEquals(firstIndex, lastIndex);
        }

        void assertPropertyAdded(String path, String key, Object value) {
            Property expected = new Property(path, key, value);

            int firstIndex = this.propertiesAdded.indexOf(expected);
            int lastIndex = this.propertiesAdded.lastIndexOf(expected);

            Assert.assertTrue(firstIndex != -1);
            Assert.assertEquals(firstIndex, lastIndex);
        }

        void assertPropertySet(String path, String key, Object value) {
            Property expected = new Property(path, key, value);

            int firstIndex = this.propertiesSet.indexOf(expected);
            int lastIndex = this.propertiesSet.lastIndexOf(expected);

            Assert.assertTrue(firstIndex != -1);
            Assert.assertEquals(firstIndex, lastIndex);
        }

        void assetNoOfNodesAdded(int num) {
            Assert.assertEquals(num, this.nodesAdded.size());
        }

    }

    @Test
    public void testAddNestedNodes() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append("+\"a\" : { \"integer\" : 123 ,\"b\" : { \"double\" : 123.456 , \"d\" : {} } , \"c\" : { \"string\" : \"string\" }}");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);

        jsopParser.parse();

        countingHandler.assetNoOfNodesAdded(4);
        countingHandler.assertNodeAdded("/", "a");
        countingHandler.assertNodeAdded("/a", "b");
        countingHandler.assertNodeAdded("/a/b", "d");
        countingHandler.assertNodeAdded("/a", "c");

        countingHandler.assertPropertiesAdded(3);
        countingHandler.assertPropertyAdded("/a", "integer", 123);
        countingHandler.assertPropertyAdded("/a/b", "double", 123.456);
        countingHandler.assertPropertyAdded("/a/c", "string", "string");
    }

    @Test
    public void testAddNodesAndProperties() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append("+\"a\" : { \"int\" : 1 } \n");
        sb.append("+\"a/b\" : { \"string\" : \"foo\" } \n");
        sb.append("+\"a/c\" : { \"bool\" : true }");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);

        jsopParser.parse();

        countingHandler.assetNoOfNodesAdded(3);
        countingHandler.assertNodeAdded("/", "a");
        countingHandler.assertNodeAdded("/a", "b");
        countingHandler.assertNodeAdded("/a", "c");

        countingHandler.assertPropertiesAdded(3);
        countingHandler.assertPropertyAdded("/a", "int", Integer.valueOf(1));
        countingHandler.assertPropertyAdded("/a/b", "string", "foo");
        countingHandler.assertPropertyAdded("/a/c", "bool", Boolean.TRUE);
    }

    @Test
    public void testAddNodesAndPropertiesSeparately() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append("+\"a\" : {} \n");
        sb.append("+\"a\" : { \"int\" : 1 } \n");
        sb.append("+\"a/b\" : {} \n");
        sb.append("+\"a/b\" : { \"string\" : \"foo\" } \n");
        sb.append("+\"a/c\" : {} \n");
        sb.append("+\"a/c\" : { \"bool\" : true }");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);

        jsopParser.parse();

        countingHandler.assetNoOfNodesAdded(6);

        countingHandler.assertPropertiesAdded(3);
        countingHandler.assertPropertyAdded("/a", "int", Integer.valueOf(1));
        countingHandler.assertPropertyAdded("/a/b", "string", "foo");
        countingHandler.assertPropertyAdded("/a/c", "bool", Boolean.TRUE);
    }

    @Test
    public void testAddPropertiesWithComplexArray() throws Exception {
        String rootPath = "/";
        String jsop = "+ \"a\" : { \"array_complex\" : [ 123, 123.456, true, false, null, \"string\", [1,2,3,4,5] ] }";

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, jsop, countingHandler);

        jsopParser.parse();

        countingHandler.assertPropertiesAdded(1);
        countingHandler.assertPropertyAdded(
                "/a",
                "array_complex",
                Arrays.asList(new Object[] { 123, 123.456, true, false, null, "string",
                        Arrays.asList(new Object[] { 1, 2, 3, 4, 5 }) }));
    }

    @Test
    public void testAddWithEmptyPath() throws Exception {
        String rootPath = "";
        StringBuilder sb = new StringBuilder();
        sb.append("+\"/\" : { \"int\" : 1 } \n");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);

        jsopParser.parse();

        countingHandler.assetNoOfNodesAdded(1);
        countingHandler.assertNodeAdded("", "/");

        countingHandler.assertPropertiesAdded(1);
        countingHandler.assertPropertyAdded("/", "int", Integer.valueOf(1));
    }

    @Test
    public void testSimpleAddNodes() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append("+\"a\" : {} \n");
        sb.append("+\"a/b\" : {} \n");
        sb.append("+\"a/c\" : {}");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);

        jsopParser.parse();

        countingHandler.assetNoOfNodesAdded(3);
        countingHandler.assertNodeAdded("/", "a");
        countingHandler.assertNodeAdded("/a", "b");
        countingHandler.assertNodeAdded("/a", "c");
    }

    @Test
    public void testSimpleAddProperties() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append("+ \"a\" : {}");
        sb.append("+ \"a\" : { \"integer\" : 123, \"double\" : 123.456, \"true\" : true, \"false\" : false, \"null\" : null, \"string\" : \"string\", \"array\" : [1,2,3,4,5] }");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);

        jsopParser.parse();

        countingHandler.assertPropertiesAdded(7);
        countingHandler.assertPropertyAdded("/a", "integer", 123);
        countingHandler.assertPropertyAdded("/a", "double", 123.456);
        countingHandler.assertPropertyAdded("/a", "true", true);
        countingHandler.assertPropertyAdded("/a", "false", false);
        countingHandler.assertPropertyAdded("/a", "null", null);
        countingHandler.assertPropertyAdded("/a", "string", "string");
        countingHandler.assertPropertyAdded("/a", "array", Arrays.asList(new Object[] { 1, 2, 3, 4, 5 }));
    }

    @Test
    public void testSimpleCopyNodes() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append("*\"a\" : \"b\"\n");
        sb.append("*\"a/b\" : \"a/c\"\n");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);
        jsopParser.parse();

        countingHandler.assertNoOfNodesCopied(2);
        countingHandler.assertNodeCopied("/", "/a", "/b");
        countingHandler.assertNodeCopied("/", "/a/b", "/a/c");
    }

    @Test
    public void testSimpleMoveNodes() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append(">\"a\" : \"b\"\n");
        sb.append(">\"a/b\" : \"a/c\"\n");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);
        jsopParser.parse();

        countingHandler.assertNoOfNodesMoved(2);
        countingHandler.assertNodeMoved("/", "/a", "/b");
        countingHandler.assertNodeMoved("/", "/a/b", "/a/c");
    }

    @Test
    public void testSimpleRemoveNodes() throws Exception {
        String rootPath = "/";
        String jsop = "-\"a\"";

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, jsop, countingHandler);

        jsopParser.parse();

        countingHandler.assertNoOfNodesRemoved(1);
        countingHandler.assertNodeRemoved("/", "a");
    }

    @Test
    public void testSimpleSetNodes() throws Exception {
        String rootPath = "/";
        StringBuilder sb = new StringBuilder();
        sb.append("^\"a\" : \"b\"");

        CountingHandler countingHandler = new CountingHandler();
        JsopParser jsopParser = new JsopParser(rootPath, sb.toString(), countingHandler);
        jsopParser.parse();

        countingHandler.assertNoOfPropertiesSet(1);
        // TODO - Is this correct?
        countingHandler.assertPropertySet("/", "a", "b");
    }
}
