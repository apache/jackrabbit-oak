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
package org.apache.jackrabbit.oak.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Before;
import org.junit.Test;

public class ImmutableRootTest extends OakBaseTest {

    private ImmutableRoot root;

    public ImmutableRootTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = createContentSession();

        // Add test content
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        Tree x = tree.addChild("x");
        Tree y = x.addChild("y");
        Tree z = y.addChild("z");
        root.commit();

        // Acquire a fresh new root to avoid problems from lingering state
        this.root = new ImmutableRoot(session.getLatestRoot());
    }

    // TODO: add more tests

    @Test
    public void testHasPendingChanges() {
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testImmutable() {

        try {
            root.commit();
            fail();
        } catch (UnsupportedOperationException e) {
            // success
        }

        try {
            root.rebase();
            fail();
        } catch (UnsupportedOperationException e) {
            // success
        }

        try {
            root.refresh();
            fail();
        } catch (UnsupportedOperationException e) {
            // success
        }

        try {
            root.createBlob(new ByteArrayInputStream(new byte[0]));
            fail();
        } catch (UnsupportedOperationException e) {
            // success
        }

        try {
            root.move("/x", "/b");
            fail();
        } catch (UnsupportedOperationException e) {
            // success
        }
    }
}
