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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test case for asserting large moves don't run out of memory.
 * See OAK-463, OAK-464
 */
public class LargeMoveTest {
    private ContentSession session;
    private String treeAPath;
    private String treeBPath;

    @Before
    public void setUp() throws CommitFailedException {
        session = new Oak().createContentSession();

        // Add test content
        Root root = session.getLatestRoot();

        Tree tree = root.getTree("/");
        Tree treeA = tree.addChild("tree-a");
        this.treeAPath = treeA.getPath();
        Tree treeB = tree.addChild("tree-b");
        this.treeBPath = treeB.getPath();

        createNodes(treeA, 10, 5);  // 111111 nodes in treeA
        root.commit();

    }

    private static void createNodes(Tree tree, int count, int depth) {
        for (int c = 0; c < count; c++) {
            Tree child = tree.addChild("n-" + depth + '-' + c);
            if (depth > 1) {
                createNodes(child, count, depth - 1);
            }
        }
    }

    @Test
    @Ignore
    public void moveTest() throws CommitFailedException {
        Root root1 = session.getLatestRoot();

        // Concurrent changes to trunk: enforce rebase
        Root root2 = session.getLatestRoot();
        root2.getTree("/").addChild("any");
        root2.commit();

        root1.move(treeAPath, PathUtils.concat(treeBPath, "tree-a-moved"));
        root1.commit();
    }

}
