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
package org.apache.jackrabbit.oak.api;

import java.io.IOException;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ContentSessionTest extends OakBaseTest {

    private ContentRepository repository;

    public ContentSessionTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() {
        repository = createContentRepository();
    }

    @After
    public void tearDown() {
        repository = null;
    }

    @Test(expected = IllegalStateException.class)
    public void throwOnClosedSession() throws LoginException, NoSuchWorkspaceException, IOException {
        ContentSession session = repository.login(null, null);
        session.close();
        session.getLatestRoot();
    }

    @Test(expected = IllegalStateException.class)
    public void throwOnClosedRoot() throws LoginException, NoSuchWorkspaceException, IOException {
        ContentSession session = repository.login(null, null);
        Root root = session.getLatestRoot();
        session.close();
        root.getTree("/");
    }

    @Test(expected = IllegalStateException.class)
    public void throwOnClosedTree() throws LoginException, NoSuchWorkspaceException, IOException {
        ContentSession session = repository.login(null, null);
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        session.close();
        tree.getChild("any");
    }
}
