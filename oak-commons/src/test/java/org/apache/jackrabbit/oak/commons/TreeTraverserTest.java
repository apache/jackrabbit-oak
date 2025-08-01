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

import org.apache.commons.collections4.FluentIterable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit cases for {@link TreeTraverser}
 */
public class TreeTraverserTest {

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

        List<Integer> result = TreeTraverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In pre-order: visit root, then left subtree, then right subtree
        Assert.assertEquals(Arrays.asList(1, 2, 4, 5, 3, 6, 7), result);
    }

    @Test
    public void testPreOrderTraversalWithNullRoot() {
        FluentIterable<Node> result = TreeTraverser.preOrderTraversal(null, Node::getChildren);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testPreOrderTraversalWithSingleNode() {
        Node root = new Node(1);
        List<Integer> result = TreeTraverser.preOrderTraversal(root, Node::getChildren)
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

        List<Integer> result = TreeTraverser.preOrderTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        // In pre-order: visit nodes as they're encountered depth-first
        Assert.assertEquals(Arrays.asList(1, 2, 4, 5, 3, 7), result);
    }

    @Test
    public void testPreOrderTraversalWithNullChildExtractor() {
        Node root = new Node(1);
        Assert.assertThrows(NullPointerException.class, () -> TreeTraverser.preOrderTraversal(root, null));
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

        List<Integer> result = TreeTraverser.preOrderTraversal(n1, Node::getChildren)
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

        List<Integer> result = TreeTraverser.preOrderTraversal(root, Node::getChildren)
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

        TreeTraverser.preOrderTraversal(root, Node::getChildren).transform(Node::getValue).forEach(System.out::println);

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

        List<Integer> result = TreeTraverser.breadthFirstTraversal(root, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7), result);
    }

    @Test
    public void testBreadthFirstTraversalWithNullRoot() {
        FluentIterable<Node> result = TreeTraverser.breadthFirstTraversal(null, Node::getChildren);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testBreadthFirstTraversalWithSingleNode() {
        Node root = new Node(1);
        List<Integer> result = TreeTraverser.breadthFirstTraversal(root, Node::getChildren)
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

        List<Integer> result = TreeTraverser.breadthFirstTraversal(root, Node::getChildren)
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

        TreeTraverser.breadthFirstTraversal(root, Node::getChildren).transform(Node::getValue).forEach(System.out::println);

        Assert.fail("Shouldn't reach here");
    }

    @Test
    public void testBreadthFirstTraversalWithNullChildExtractor() {
        Node root = new Node(1);
        Assert.assertThrows(NullPointerException.class, () -> TreeTraverser.breadthFirstTraversal(root, null));
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

        List<Integer> result = TreeTraverser.breadthFirstTraversal(n1, Node::getChildren)
                .transform(Node::getValue)
                .toList();

        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5), result);
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
}