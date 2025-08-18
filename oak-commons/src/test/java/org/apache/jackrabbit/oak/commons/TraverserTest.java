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

package org.apache.jackrabbit.oak.commons;

import org.apache.jackrabbit.guava.common.collect.TreeTraverser;
import org.apache.commons.collections4.FluentIterable;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit cases for {@link Traverser}
 */
public class TraverserTest {

    @Test
    public void testPreOrderTraversalWithNormalTree() {
        // Create a simple tree structure:
        //       1
        //     /   \
        //    2     3
        //   / \   / \
        //  4   5 6   7
        Node root = new Node(1,
                new Node(2,
                        new Node(4),
                        new Node(5)),
                new Node(3,
                        new Node(6),
                        new Node(7)));

        List<Integer> result = Traverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In pre-order: visit root, then left subtree, then right subtree
        Assert.assertEquals(Arrays.asList(1, 2, 4, 5, 3, 6, 7), result);
    }

    @Test
    public void testPreOrderTraversalWithNullRoot() {
        FluentIterable<Node> result = Traverser.preOrderTraversal(null, Node::getChildren);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testPreOrderTraversalWithSingleNode() {
        Node root = new Node(1);
        List<Integer> result = Traverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        Assert.assertEquals(Collections.singletonList(1), result);
    }

    @Test
    public void testPreOrderTraversalWithAsymmetricTree() {
        // Create an asymmetric tree:
        //       1
        //     /   \
        //    2     3
        //   /       \
        //  4         7
        //   \
        //    5
        Node root = new Node(1,
                new Node(2,
                        new Node(4,
                                new Node(5))),
                new Node(3,
                        new Node(7)));

        List<Integer> result = Traverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In pre-order: visit nodes as they're encountered depth-first
        Assert.assertEquals(Arrays.asList(1, 2, 4, 5, 3, 7), result);
    }

    @Test
    public void testPreOrderTraversalWithNullChildExtractor() {
        Node root = new Node(1);
        Assert.assertThrows(NullPointerException.class, () -> Traverser.preOrderTraversal(root, null));
    }

    @Test
    public void testPreOrderTraversalWithDeepTree() {
        // Create a deep tree with many levels (linked-list-like)
        Node n1 = new Node(1);
        Node n2 = new Node(2);
        Node n3 = new Node(3);
        Node n4 = new Node(4);
        Node n5 = new Node(5);

        n1.addChild(n2);
        n2.addChild(n3);
        n3.addChild(n4);
        n4.addChild(n5);

        List<Integer> result = Traverser.preOrderTraversal(n1, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // Should visit in depth-first order
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), result);
    }

    @Test
    public void testPreOrderTraversalWithBinarySearchTree() {
        // Create a binary search tree structure
        //        4
        //      /   \
        //     2     6
        //    / \   / \
        //   1   3 5   7
        Node root = new Node(4,
                new Node(2,
                        new Node(1),
                        new Node(3)),
                new Node(6,
                        new Node(5),
                        new Node(7)));

        List<Integer> result = Traverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In pre-order: root, left subtree, right subtree
        Assert.assertEquals(Arrays.asList(4, 2, 1, 3, 6, 5, 7), result);
    }

    @Test(expected = NullPointerException.class)
    public void testPreOrderTraversalWithNullChildren() {
        // A tree with some null children
        Node root = new Node(1,
                null,
                new Node(3));

        Traverser.preOrderTraversal(root, Node::getChildren).transform(Node::getValue).forEach(System.out::println);

        Assert.fail("Shouldn't reach here");
    }

    @Test
    public void testBreadthFirstTraversalWithNormalTree() {
        // Create a simple tree structure:
        //       1
        //     /   \
        //    2     3
        //   / \   / \
        //  4   5 6   7
        Node root = new Node(1,
                new Node(2,
                        new Node(4),
                        new Node(5)),
                new Node(3,
                        new Node(6),
                        new Node(7)));

        List<Integer> result = Traverser.breadthFirstTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7), result);
    }

    @Test
    public void testPreOrderTraversalWithTree() {
        // Create a tree structure
        //        4
        //      /   \
        //   0,2     6
        //    /       \
        //  1,3,5   7,8,9
        Node root = new Node(4,
                new Node(0),
                new Node(2,
                        new Node(1),
                        new Node(3),
                        new Node(5)),
                new Node(6,
                        new Node(7),
                        new Node(8),
                        new Node(9)));

        List<Integer> result = Traverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In post-order: left subtree, right subtree, root
        Assert.assertEquals(Arrays.asList(4, 0, 2, 1, 3, 5, 6, 7, 8, 9), result);
    }

    // TODO remove this test when we remove guava dependency
    @Test
    public void testPreOrderTraversalWithRandomTree() {

        final Node root = getRoot(10000);

        List<Integer> result = Traverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In post-order: left subtree, right subtree, root
        Assert.assertEquals(result, traverser.preOrderTraversal(root).transform(Node::getValue).toList());
    }

    @Test
    public void testBreadthFirstTraversalWithNullRoot() {
        FluentIterable<Node> result = Traverser.breadthFirstTraversal(null, Node::getChildren);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testBreadthFirstTraversalWithSingleNode() {
        Node root = new Node(1);
        List<Integer> result = Traverser.breadthFirstTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        Assert.assertEquals(Collections.singletonList(1), result);
    }

    @Test
    public void testBreadthFirstTraversalWithAsymmetricTree() {
        // Create an asymmetric tree:
        //       1
        //     /   \
        //    2     3
        //   /       \
        //  4         7
        //   \
        //    5
        Node root = new Node(1,
                new Node(2,
                        new Node(4,
                                new Node(5))),
                new Node(3,
                        new Node(7)));

        List<Integer> result = Traverser.breadthFirstTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 7, 5), result);
    }

    @Test(expected = NullPointerException.class)
    public void testBreadthFirstTraversalWithNullChildren() {
        // A tree with some null children
        Node root = new Node(1,
                null,
                new Node(3));

        Traverser.breadthFirstTraversal(root, Node::getChildren).transform(Node::getValue).forEach(System.out::println);

        Assert.fail("Shouldn't reach here");
    }

    @Test
    public void testBreadthFirstTraversalWithNullChildExtractor() {
        Node root = new Node(1);
        Assert.assertThrows(NullPointerException.class, () -> Traverser.breadthFirstTraversal(root, null));
    }

    @Test
    public void testBreadthFirstTraversalWithDeepTree() {
        // Create a deep tree with many levels
        Node n1 = new Node(1);
        Node n2 = new Node(2);
        Node n3 = new Node(3);
        Node n4 = new Node(4);
        Node n5 = new Node(5);

        n1.addChild(n2);
        n2.addChild(n3);
        n3.addChild(n4);
        n4.addChild(n5);

        List<Integer> result = Traverser.breadthFirstTraversal(n1, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), result);
    }

    @Test
    public void testBreadthFirstOrderTraversalWithTree() {
        // Create a tree structure
        //        4
        //      /   \
        //   0,2     6
        //    /       \
        //  1,3,5   7,8,9
        Node root = new Node(4,
                new Node(0),
                new Node(2,
                        new Node(1),
                        new Node(3),
                        new Node(5)),
                new Node(6,
                        new Node(7),
                        new Node(8),
                        new Node(9)));

        List<Integer> result = Traverser.breadthFirstTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In post-order: left subtree, right subtree, root
        Assert.assertEquals(Arrays.asList(4, 0, 2, 6, 1, 3, 5, 7, 8, 9), result);
    }

    // TODO remove this test when we remove guava dependency
    @Test
    public void testBreadthFirstOrderTraversalWithRandomTree() {

        final Node root = getRoot(10000);

        List<Integer> result = Traverser.breadthFirstTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In post-order: left subtree, right subtree, root
        Assert.assertEquals(result, traverser.breadthFirstTraversal(root).transform(Node::getValue).toList());
    }

    // Helper class for testing tree traversal
    private static class Node {
        private final int value;
        private final List<Node> children = new ArrayList<>();
        public Node(int value, Node... children) {
            this.value = value;
            this.children.addAll(Arrays.asList(children));
        }

        public int getValue() {
            return value;
        }

        public Iterable<Node> getChildren() {
            return children;
        }

        public void addChild(Node child) {
            children.add(child);
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }

    }
    private @NotNull Node getRoot(final int count) {
        final Node root = new Node(4);

        List<Node> parents = new ArrayList<>();
        parents.add(root);  // Start with root as the only parent

        java.util.Random random = new java.util.Random();
        int nodesCreated = 1;  // Start at 1 to account for the root

        while (nodesCreated < count && !parents.isEmpty()) {
            List<Node> nextParents = new ArrayList<>();

            // For each current parent
            for (Node parent : parents) {
                // Randomly determine how many children to add (0-5)
                int numChildren = random.nextInt(6);

                // Make sure we don't exceed the total count
                numChildren = Math.min(numChildren, count - nodesCreated);

                // Add the random number of children
                for (int i = 0; i < numChildren; i++) {
                    Node child = new Node(nodesCreated + 4);  // Unique value
                    parent.addChild(child);
                    nodesCreated++;

                    // Each child has a chance to become a parent in the next round
                    if (random.nextBoolean()) {
                        nextParents.add(child);
                    }
                }
            }
            // Update parents for the next round
            parents = nextParents;
        }
        return root;
    }

    final TreeTraverser<Node> traverser = new TreeTraverser<>() {

        @Override
        public Iterable<Node> children(Node root) {
            return root.getChildren();
        }
    };
}