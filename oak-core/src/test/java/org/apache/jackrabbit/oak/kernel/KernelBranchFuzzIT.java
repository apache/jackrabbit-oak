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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.simple.SimpleKernelImpl;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.api.ContentTree;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Scalar;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.apache.jackrabbit.oak.kernel.KernelBranchFuzzIT.Operation.AddNode;
import static org.apache.jackrabbit.oak.kernel.KernelBranchFuzzIT.Operation.MoveNode;
import static org.apache.jackrabbit.oak.kernel.KernelBranchFuzzIT.Operation.CopyNode;
import static org.apache.jackrabbit.oak.kernel.KernelBranchFuzzIT.Operation.RemoveNode;
import static org.apache.jackrabbit.oak.kernel.KernelBranchFuzzIT.Operation.RemoveProperty;
import static org.apache.jackrabbit.oak.kernel.KernelBranchFuzzIT.Operation.Save;
import static org.apache.jackrabbit.oak.kernel.KernelBranchFuzzIT.Operation.SetProperty;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class KernelBranchFuzzIT {
    static final Logger log = LoggerFactory.getLogger(KernelBranchFuzzIT.class);

    private static final int OP_COUNT = 5000;

    private final Random random;

    private final MicroKernel mk1 = new SimpleKernelImpl("mem:");
    private final MicroKernel mk2 = new SimpleKernelImpl("mem:");

    @Parameters
    public static List<Object[]> seeds() {
        return Arrays.asList(new Object[][] {
                {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9},
                {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19},
                {20}, {21}, {22}, {23}, {24}, {25}, {26}, {27}, {28}, {29},
                {30}, {31}, {32}, {33}, {34}, {35}, {36}, {37}, {38}, {39},
        });
    }

    public KernelBranchFuzzIT(int seed) {
        log.info("Seed = {}", seed);
        random = new Random(seed);
    }

    @Before
    public void setup() {
        mk1.commit("", "+\"/root\":{}", mk1.getHeadRevision(), "");
        mk2.commit("", "+\"/root\":{}", mk2.getHeadRevision(), "");
    }

    @Test
    public void fuzzTest() throws Exception {
        KernelNodeState state1 = new KernelNodeState(mk1, "/", mk1.getHeadRevision());
        KernelBranch branch1 = new KernelBranch(state1);

        KernelNodeState state2 = new KernelNodeState(mk2, "/", mk2.getHeadRevision());
        KernelBranch branch2 = new KernelBranch(state2);

        for (Operation op : operations(OP_COUNT)) {
            log.info("{}", op);
            op.apply(branch1);
            op.apply(branch2);
            checkEqual(branch1.getContentTree("/"), branch2.getContentTree("/"));

            state1 = branch1.mergeInto(mk1, state1);
            branch1 = new KernelBranch(state1);
            if (op instanceof Save) {
                state2 = branch2.mergeInto(mk2, state2);
                branch2 = new KernelBranch(state2);
                assertEquals(state1, state2);
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
        abstract void apply(KernelBranch branch);

        static class AddNode extends Operation {
            private final String parentPath;
            private final String name;

            AddNode(String parentPath, String name) {
                this.parentPath = parentPath;
                this.name = name;
            }

            @Override
            void apply(KernelBranch branch) {
                branch.getContentTree(parentPath).addChild(name);
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
            void apply(KernelBranch branch) {
                String parentPath = PathUtils.getParentPath(path);
                String name = PathUtils.getName(path);
                branch.getContentTree(parentPath).removeChild(name);
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
            void apply(KernelBranch branch) {
                branch.move(source.substring(1), destination.substring(1));
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
            void apply(KernelBranch branch) {
                branch.copy(source.substring(1), destination.substring(1));
            }

            @Override
            public String toString() {
                return '*' + source + ':' + destination;
            }
        }

        static class SetProperty extends Operation {
            private final String parentPath;
            private String propertyName;
            private Scalar propertyValue;

            SetProperty(String parentPath, String name, Scalar value) {
                this.parentPath = parentPath;
                this.propertyName = name;
                this.propertyValue = value;
            }

            @Override
            void apply(KernelBranch branch) {
                branch.getContentTree(parentPath).setProperty(propertyName, propertyValue);
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
            void apply(KernelBranch branch) {
                branch.getContentTree(parentPath).removeProperty(name);
            }

            @Override
            public String toString() {
                return '^' + PathUtils.concat(parentPath, name) + ":null";
            }
        }

        static class Save extends Operation {
            @Override
            void apply(KernelBranch branch) {
                // empty
            }

            @Override
            public String toString() {
                return "save";
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
        Scalar value = createValue();
        return new SetProperty(parent, name, value);
    }

    private Operation createSetProperty() {
        String path = choosePropertyPath();
        if (path == null) {
            return null;
        }
        Scalar value = createValue();
        return new SetProperty(PathUtils.getParentPath(path), PathUtils.getName(path), value);
    }

    private Operation createRemoveProperty() {
        String path = choosePropertyPath();
        if (path == null) {
            return null;
        }
        return new RemoveProperty(PathUtils.getParentPath(path), PathUtils.getName(path));
    }

    private int counter;

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
        KernelNodeState state = new KernelNodeState(mk1, parentPath, mk1.getHeadRevision());
        int k = random.nextInt((int) (state.getChildNodeCount() + 1));
        int c = 0;
        for (ChildNodeEntry entry : state.getChildNodeEntries(0, -1)) {
            if (c++ == k) {
                return PathUtils.concat(parentPath, entry.getName());
            }
        }
        return null;
    }

    private String chooseProperty(String parentPath) {
        KernelNodeState state = new KernelNodeState(mk1, parentPath, mk1.getHeadRevision());
        int k = random.nextInt((int) (state.getPropertyCount() + 1));
        int c = 0;
        for (PropertyState entry : state.getProperties()) {
            if (c++ == k) {
                return PathUtils.concat(parentPath, entry.getName());
            }
        }
        return null;
    }

    private Scalar createValue() {
        return ScalarImpl.stringScalar("V" + counter++);
    }

    private static void checkEqual(ContentTree child2, ContentTree tree2) {
        assertEquals(child2.getPath(), tree2.getPath());
        assertEquals(child2.getChildrenCount(), tree2.getChildrenCount());
        assertEquals(child2.getPropertyCount(), tree2.getPropertyCount());

        for (PropertyState property1 : child2.getProperties()) {
            assertEquals(property1, tree2.getProperty(property1.getName()));
        }

        for (ContentTree child1 : child2.getChildren()) {
            checkEqual(child1, tree2.getChild(child1.getName()));
        }
    }

}
