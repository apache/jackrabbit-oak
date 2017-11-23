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

import static org.apache.jackrabbit.oak.core.RootFuzzIT.Operation.AddNode;
import static org.apache.jackrabbit.oak.core.RootFuzzIT.Operation.MoveNode;
import static org.apache.jackrabbit.oak.core.RootFuzzIT.Operation.RemoveNode;
import static org.apache.jackrabbit.oak.core.RootFuzzIT.Operation.RemoveProperty;
import static org.apache.jackrabbit.oak.core.RootFuzzIT.Operation.Save;
import static org.apache.jackrabbit.oak.core.RootFuzzIT.Operation.SetProperty;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.RootFuzzIT.Operation.Rebase;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fuzz test running random sequences of operations on {@link Tree}.
 * Run with -DRootFuzzIT-seed=42 to set a specific seed (i.e. 42);
 */
@RunWith(value = Parameterized.class)
public class RootFuzzIT {
    static final Logger log = LoggerFactory.getLogger(RootFuzzIT.class);

    @Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        return NodeStoreFixtures.asJunitParameters(EnumSet.of(Fixture.DOCUMENT_NS, Fixture.SEGMENT_TAR));
    }

    private static final int OP_COUNT = 5000;

    private static final int SEED = Integer.getInteger(
            RootFuzzIT.class.getSimpleName() + "-seed",
            new Random().nextInt());

    private static final Random random = new Random();

    private final NodeStoreFixture fixture;

    private NodeStore store1;
    private Root root1;

    private NodeStore store2;
    private Root root2;

    private int counter;

    public RootFuzzIT(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Before
    public void setup() throws CommitFailedException {
        log.info("Running " + getClass().getSimpleName() + " with " +
                fixture + " and seed " + SEED);

        random.setSeed(SEED);
        counter = 0;

        store1 = fixture.createNodeStore();
        root1 = RootFactory.createSystemRoot(store1, null, null, null, null);
        root1.getTree("/").addChild("root");
        root1.commit();

        store2 = fixture.createNodeStore();
        root2 = RootFactory.createSystemRoot(store2, null, null, null, null);
        root2.getTree("/").addChild("root");
        root2.commit();
    }

    @After
    public void teardown() {
        fixture.dispose(store1);
        fixture.dispose(store2);
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
                checkEqual(root1.getTree("/"), root2.getTree("/"));
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
        abstract void apply(Root root);

        static class AddNode extends Operation {
            private final String parentPath;
            private final String name;

            AddNode(String parentPath, String name) {
                this.parentPath = parentPath;
                this.name = name;
            }

            @Override
            void apply(Root root) {
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
            void apply(Root root) {
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
            void apply(Root root) {
                root.move(source, destination);
            }

            @Override
            public String toString() {
                return '>' + source + ':' + destination;
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
            void apply(Root root) {
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
            void apply(Root root) {
                root.getTree(parentPath).removeProperty(name);
            }

            @Override
            public String toString() {
                return '^' + PathUtils.concat(parentPath, name) + ":null";
            }
        }

        static class Save extends Operation {
            @Override
            void apply(Root root) {
                // empty
            }

            @Override
            public String toString() {
                return "save";
            }
        }

        static class Rebase extends Operation {
            @Override
            void apply(Root root) {
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
            switch (random.nextInt(10)) {
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
                    op = createAddProperty();
                    break;
                case 6:
                    op = createSetProperty();
                    break;
                case 7:
                    op = createRemoveProperty();
                    break;
                case 8:
                    op = new Save();
                    break;
                case 9:
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

        int k = random.nextInt((int) (state.getChildrenCount(Long.MAX_VALUE) + 1));
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
        assertEquals(message, tree1.getChildrenCount(Long.MAX_VALUE), tree2.getChildrenCount(Long.MAX_VALUE));
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
