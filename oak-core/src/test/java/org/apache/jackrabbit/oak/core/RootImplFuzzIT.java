/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.core;

import java.util.Iterator;
import java.util.Random;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.Rebase;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.AddNode;
import static org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.CopyNode;
import static org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.MoveNode;
import static org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.RemoveNode;
import static org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.RemoveProperty;
import static org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.Save;
import static org.apache.jackrabbit.oak.core.RootImplFuzzIT.Operation.SetProperty;
import static org.junit.Assert.assertEquals;

/**
 * Fuzz test running random sequences of operations on {@link Tree}.
 * Run with -DKernelRootFuzzIT-seed=42 to set a specific seed (i.e. 42);
 */
public class RootImplFuzzIT {

    static final Logger log = LoggerFactory.getLogger(RootImplFuzzIT.class);

    private static final int OP_COUNT = 5000;

    private static final int SEED = Integer.getInteger(
            RootImplFuzzIT.class.getSimpleName() + "-seed",
            new Random().nextInt());

    private static final Random random = new Random(SEED);

    private KernelNodeStore store1;
    private RootImpl root1;

    private KernelNodeStore store2;
    private RootImpl root2;

    private int counter;

    @Before
    public void setup() {
        counter = 0;

        MicroKernel mk1 = new MicroKernelImpl("./target/mk1/" + random.nextInt());
        store1 = new KernelNodeStore(mk1);
        mk1.commit("", "+\"/root\":{}", mk1.getHeadRevision(), "");
        root1 = new RootImpl(store1);

        MicroKernel mk2 = new MicroKernelImpl("./target/mk2/" + random.nextInt());
        store2 = new KernelNodeStore(mk2);
        mk2.commit("", "+\"/root\":{}", mk2.getHeadRevision(), "");
        root2 = new RootImpl(store2);
    }

    @Test
    public void fuzzTest() throws Exception {
        for (Operation op : operations(OP_COUNT)) {
            log.info("{}", op);
            op.apply(root1);
            op.apply(root2);
            checkEqual(root1.getTree("/"), root2.getTree("/"));

            root1.commit();
            checkEqual(root1.getTree("/"), root2.getTree("/"));
            if (op instanceof Save) {
                root2.commit();
                assertEquals("seed " + SEED, store1.getRoot(), store2.getRoot());
            }
        }
    }

    private Iterable<Operation> operations(final int count) {
        return new Iterable<Operation>() {
            int k = count;

            @Override
            public Iterator<Operation> iterator() {
                return new Iterator<Operation>() {
                    @Override
                    public boolean hasNext() {
                        return k-- > 0;
                    }

                    @Override
                    public Operation next() {
                        return createOperation();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    abstract static class Operation {
        abstract void apply(RootImpl root);

        static class AddNode extends Operation {
            private final String parentPath;
            private final String name;

            AddNode(String parentPath, String name) {
                this.parentPath = parentPath;
                this.name = name;
            }

            @Override
            void apply(RootImpl root) {
                root.getTree(parentPath).addChild(name);
            }

            @Override
            public String toString() {
                return '+' + PathUtils.concat(parentPath, name) + ":{}";
            }
        }

        static class RemoveNode extends Operation {
            private final String path;

            RemoveNode(String path) {
                this.path = path;
            }

            @Override
            void apply(RootImpl root) {
                String parentPath = PathUtils.getParentPath(path);
                String name = PathUtils.getName(path);
                root.getTree(parentPath).getChild(name).remove();
            }

            @Override
            public String toString() {
                return '-' + path;
            }
        }

        static class MoveNode extends Operation {
            private final String source;
            private final String destination;

            MoveNode(String source, String destParent, String destName) {
                this.source = source;
                destination = PathUtils.concat(destParent, destName);
            }

            @Override
            void apply(RootImpl root) {
                root.move(source, destination);
            }

            @Override
            public String toString() {
                return '>' + source + ':' + destination;
            }
        }

        static class CopyNode extends Operation {
            private final String source;
            private final String destination;

            CopyNode(String source, String destParent, String destName) {
                this.source = source;
                destination = PathUtils.concat(destParent, destName);
            }

            @Override
            void apply(RootImpl root) {
                root.copy(source, destination);
            }

            @Override
            public String toString() {
                return '*' + source + ':' + destination;
            }
        }

        static class SetProperty extends Operation {
            private final String parentPath;
            private final String propertyName;
            private final String propertyValue;

            SetProperty(String parentPath, String name, String value) {
                this.parentPath = parentPath;
                this.propertyName = name;
                this.propertyValue = value;
            }

            @Override
            void apply(RootImpl root) {
                root.getTree(parentPath).setProperty(propertyName, propertyValue);
            }

            @Override
            public String toString() {
                return '^' + PathUtils.concat(parentPath, propertyName) + ':'
                        + propertyValue;
            }
        }

        static class RemoveProperty extends Operation {
            private final String parentPath;
            private final String name;

            RemoveProperty(String parentPath, String name) {
                this.parentPath = parentPath;
                this.name = name;
            }

            @Override
            void apply(RootImpl root) {
                root.getTree(parentPath).removeProperty(name);
            }

            @Override
            public String toString() {
                return '^' + PathUtils.concat(parentPath, name) + ":null";
            }
        }

        static class Save extends Operation {
            @Override
            void apply(RootImpl root) {
                // empty
            }

            @Override
            public String toString() {
                return "save";
            }
        }

        static class Rebase extends Operation {
            @Override
            void apply(RootImpl root) {
                root.rebase();
            }

            @Override
            public String toString() {
                return "rebase";
            }
        }
    }

    private Operation createOperation() {
        Operation op;
        do {
            switch (random.nextInt(11)) {
                case 0:
                case 1:
                case 2:
                    op = createAddNode();
                    break;
                case 3:
                    op = createRemoveNode();
                    break;
                case 4:
                    op = createMoveNode();
                    break;
                case 5:
                    // Too many copy ops make the test way slow
                    op = random.nextInt(10) == 0 ? createCopyNode() : null;
                    break;
                case 6:
                    op = createAddProperty();
                    break;
                case 7:
                    op = createSetProperty();
                    break;
                case 8:
                    op = createRemoveProperty();
                    break;
                case 9:
                    op = new Save();
                    break;
                case 10:
                    op = new Rebase();
                    break;
                default:
                    throw new IllegalStateException();
            }
        } while (op == null);
        return op;
    }

    private Operation createAddNode() {
        String parentPath = chooseNodePath();
        String name = createNodeName();
        return new AddNode(parentPath, name);
    }

    private Operation createRemoveNode() {
        String path = chooseNodePath();
        return "/root".equals(path) ? null : new RemoveNode(path);
    }

    private Operation createMoveNode() {
        String source = chooseNodePath();
        String destParent = chooseNodePath();
        String destName = createNodeName();
        return "/root".equals(source) || destParent.startsWith(source)
                ? null
                : new MoveNode(source, destParent, destName);
    }

    private Operation createCopyNode() {
        String source = chooseNodePath();
        String destParent = chooseNodePath();
        String destName = createNodeName();
        return "/root".equals(source)
                ? null
                : new CopyNode(source, destParent, destName);
    }

    private Operation createAddProperty() {
        String parent = chooseNodePath();
        String name = createPropertyName();
        String value = createValue();
        return new SetProperty(parent, name, value);
    }

    private Operation createSetProperty() {
        String path = choosePropertyPath();
        if (path == null) {
            return null;
        }
        String value = createValue();
        return new SetProperty(PathUtils.getParentPath(path), PathUtils.getName(path), value);
    }

    private Operation createRemoveProperty() {
        String path = choosePropertyPath();
        if (path == null) {
            return null;
        }
        return new RemoveProperty(PathUtils.getParentPath(path), PathUtils.getName(path));
    }

    private String createNodeName() {
        return "N" + counter++;
    }

    private String createPropertyName() {
        return "P" + counter++;
    }

    private String chooseNodePath() {
        String path = "/root";

        String next;
        while ((next = chooseNode(path)) != null) {
            path = next;
        }

        return path;
    }

    private String choosePropertyPath() {
        return chooseProperty(chooseNodePath());
    }

    private String chooseNode(String parentPath) {
        Tree state = root1.getTree(parentPath);

        int k = random.nextInt((int) (state.getChildrenCount() + 1));
        int c = 0;
        for (Tree child : state.getChildren()) {
            if (c++ == k) {
                return PathUtils.concat(parentPath, child.getName());
            }
        }

        return null;
    }

    private String chooseProperty(String parentPath) {
        Tree state = root1.getTree(parentPath);
        int k = random.nextInt((int) (state.getPropertyCount() + 1));
        int c = 0;
        for (PropertyState entry : state.getProperties()) {
            if (c++ == k) {
                return PathUtils.concat(parentPath, entry.getName());
            }
        }
        return null;
    }

    private String createValue() {
        return ("V" + counter++);
    }

    private static void checkEqual(Tree tree1, Tree tree2) {
        String message =
                tree1.getPath() + "!=" + tree2.getPath()
                + " (seed " + SEED + ')';
        assertEquals(message, tree1.getPath(), tree2.getPath());
        assertEquals(message, tree1.getChildrenCount(), tree2.getChildrenCount());
        assertEquals(message, tree1.getPropertyCount(), tree2.getPropertyCount());

        for (PropertyState property1 : tree1.getProperties()) {
            PropertyState property2 = tree2.getProperty(property1.getName());
            assertEquals(message, property1, property2);
        }

        for (Tree child1 : tree1.getChildren()) {
            checkEqual(child1, tree2.getChild(child1.getName()));
        }
    }

}
