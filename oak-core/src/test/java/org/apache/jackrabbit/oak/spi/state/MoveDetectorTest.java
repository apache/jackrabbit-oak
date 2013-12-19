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

package org.apache.jackrabbit.oak.spi.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.DefaultMoveValidator;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.junit.Before;
import org.junit.Test;

public class MoveDetectorTest {
    private NodeState root;

    @Before
    public void setup() {
        NodeBuilder rootBuilder = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder test = rootBuilder.child("test");
        test.setProperty("a", 1);
        test.setProperty("b", 2);
        test.setProperty("c", 3);
        test.child("x");
        test.child("y");
        test.child("z").child("zz");
        root = rootBuilder.getNodeState();
    }

    /**
     * Test whether we can detect a single move
     * @throws CommitFailedException
     */
    @Test
    public void simpleMove() throws CommitFailedException {
        NodeState moved1 = move(root.builder(), "/test/x", "/test/y/xx").getNodeState();
        MoveExpectation moveExpectation1 = new MoveExpectation(
                ImmutableMap.of("/test/x", "/test/y/xx"));
        MoveDetector moveDetector1 = new MoveDetector(moveExpectation1);
        CommitFailedException exception1 = EditorDiff.process(moveDetector1, root, moved1);
        if (exception1 != null) {
            throw exception1;
        }
        moveExpectation1.assertAllFound();

        // Test whether we can also detect the move back on top of the previous, persisted move
        NodeState moved2 = move(moved1.builder(), "/test/y/xx", "/test/x").getNodeState();
        MoveExpectation moveExpectation2 = new MoveExpectation(
                ImmutableMap.of("/test/y/xx", "/test/x"));
        MoveDetector moveDetector2 = new MoveDetector(moveExpectation2);
        CommitFailedException exception2 = EditorDiff.process(moveDetector2, moved1, moved2);
        if (exception2 != null) {
            throw exception2;
        }
        moveExpectation2.assertAllFound();
    }

    /**
     * Moving a moved node is reported as a single move from the original source
     * to the final destination.
     * @throws CommitFailedException
     */
    @Test
    public void moveMoved() throws CommitFailedException {
        NodeBuilder rootBuilder = root.builder();
        move(rootBuilder, "/test/x", "/test/y/xx");
        NodeState moved = move(rootBuilder, "/test/y/xx", "/test/z/xxx").getNodeState();
        MoveExpectation moveExpectation = new MoveExpectation(
                ImmutableMap.of("/test/x", "/test/z/xxx"));
        MoveDetector moveDetector = new MoveDetector(moveExpectation);
        CommitFailedException exception = EditorDiff.process(moveDetector, root, moved);
        if (exception != null) {
            throw exception;
        }
        moveExpectation.assertAllFound();
    }

    /**
     * Moving a transiently added node doesn't generate a move event
     * @throws CommitFailedException
     */
    @Test
    public void moveAddedNode() throws CommitFailedException {
        NodeBuilder rootBuilder = root.builder();
        rootBuilder.getChildNode("test").setChildNode("added");
        NodeState moved = move(rootBuilder, "/test/added", "/test/y/added").getNodeState();
        MoveExpectation moveExpectation = new MoveExpectation(
                ImmutableMap.<String, String>of());
        MoveDetector moveDetector = new MoveDetector(moveExpectation);
        CommitFailedException exception = EditorDiff.process(moveDetector, root, moved);
        if (exception != null) {
            throw exception;
        }
        moveExpectation.assertAllFound();
    }

    /**
     * Moving a node from a moved subtree doesn't generate a move event.
     * @throws CommitFailedException
     */
    @Test
    public void moveFromMovedSubtree() throws CommitFailedException {
        NodeBuilder rootBuilder = root.builder();
        move(rootBuilder, "/test/z", "/test/y/z");
        NodeState moved = move(rootBuilder, "/test/y/z/zz", "/test/x/zz").getNodeState();
        MoveExpectation moveExpectation = new MoveExpectation(
                ImmutableMap.of("/test/z", "/test/y/z", "/test/z/zz", "/test/x/zz"));
        MoveDetector moveDetector = new MoveDetector(moveExpectation);
        CommitFailedException exception = EditorDiff.process(moveDetector, root, moved);
        if (exception != null) {
            throw exception;
        }
        moveExpectation.assertAllFound();
    }

    /**
     * Moving a transiently added node from a moved subtree doesn't generate a move event.
     * @throws CommitFailedException
     */
    @Test
    public void moveAddedFromMovedSubtree() throws CommitFailedException {
        NodeBuilder rootBuilder = root.builder();
        rootBuilder.getChildNode("test").getChildNode("z").setChildNode("added");
        move(rootBuilder, "/test/z", "/test/y/z");
        NodeState moved = move(rootBuilder, "/test/y/z/added", "/test/x/added").getNodeState();
        MoveExpectation moveExpectation = new MoveExpectation(
                ImmutableMap.of("/test/z", "/test/y/z"));
        MoveDetector moveDetector = new MoveDetector(moveExpectation);
        CommitFailedException exception = EditorDiff.process(moveDetector, root, moved);
        if (exception != null) {
            throw exception;
        }
        moveExpectation.assertAllFound();
    }

    /**
     * Moving a node forth and back again should not generate a move event.
     * @throws CommitFailedException
     */
    @Test
    public void moveForthAndBack() throws CommitFailedException {
        NodeBuilder rootBuilder = root.builder();
        move(rootBuilder, "/test/x", "/test/y/xx");
        NodeState moved = move(rootBuilder, "/test/y/xx", "/test/x").getNodeState();
        MoveExpectation moveExpectation = new MoveExpectation(
                ImmutableMap.<String, String>of());
        MoveDetector moveDetector = new MoveDetector(moveExpectation);
        CommitFailedException exception = EditorDiff.process(moveDetector, root, moved);
        if (exception != null) {
            throw exception;
        }
        moveExpectation.assertAllFound();
    }

    //------------------------------------------------------------< private >---

    private static NodeBuilder move(NodeBuilder builder, String source, String dest) {
        NodeBuilder sourceBuilder = getBuilder(builder, source);
        NodeBuilder destParentBuilder = getBuilder(builder, PathUtils.getParentPath(dest));
        assertTrue(sourceBuilder.moveTo(destParentBuilder, PathUtils.getName(dest)));
        return builder;
    }

    private static NodeBuilder getBuilder(NodeBuilder builder, String path) {
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    private static class MoveExpectation extends DefaultMoveValidator {
        private final Map<String, String> moves;
        private final String path;

        private MoveExpectation(Map<String, String> moves, String path) {
            this.moves = moves;
            this.path = path;
        }

        public MoveExpectation(Map<String, String> moves) {
            this(Maps.newHashMap(moves), "/");
        }

        @Override
        public void move(String name, String sourcePath, NodeState moved) throws CommitFailedException {
            String actualDestPath = PathUtils.concat(path, name);
            String expectedDestPath = moves.remove(sourcePath);
            assertEquals("Unexpected move. " +
                    "Expected: " + (expectedDestPath == null
                            ? "None"
                            : '>' + sourcePath + ':' + expectedDestPath) +
                    " Found: >" + sourcePath + ':' + actualDestPath,
                    expectedDestPath, actualDestPath);
        }

        @Override
        public MoveValidator childNodeChanged(String name, NodeState before, NodeState after) {
            return new MoveExpectation(moves, PathUtils.concat(path, name));
        }

        public void assertAllFound() {
            assertTrue("Missing moves: " + moves, moves.isEmpty());
        }
    }

}
