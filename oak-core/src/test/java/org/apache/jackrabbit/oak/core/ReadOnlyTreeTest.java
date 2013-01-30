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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReadOnlyTreeTest {

    private Root root;

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = new Oak().createContentSession();

        // Add test content
        root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        Tree x = tree.addChild("x");
        Tree y = x.addChild("y");
        Tree z = y.addChild("z");
        root.commit();

        // Acquire a fresh new root to avoid problems from lingering state
        root = session.getLatestRoot();
    }

    @After
    public void tearDown() {
        root = null;
    }

    @Test
    public void testGetPath() {
        TreeImpl tree = (TreeImpl) root.getTree("/");

        ReadOnlyTree readOnly = new ReadOnlyTree(tree.getNodeState());
        assertEquals("/", readOnly.getPath());

        readOnly = readOnly.getChild("x");
        assertEquals("/x", readOnly.getPath());

        readOnly = readOnly.getChild("y");
        assertEquals("/x/y", readOnly.getPath());

        readOnly = readOnly.getChild("z");
        assertEquals("/x/y/z", readOnly.getPath());
    }
}
