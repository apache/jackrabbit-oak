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
package org.apache.jackrabbit.oak.jcr;

import java.util.Collection;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.MEMORY_NS;
import static org.junit.Assert.assertTrue;

public class TransientMoveTest extends AbstractRepositoryTest {

    public TransientMoveTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> memoryFixture() {
        return NodeStoreFixtures.asJunitParameters(singleton(MEMORY_NS));
    }

    @Ignore("OAK-9660")
    @Test
    public void transientMove() throws Exception {
        // setup
        Session session = login();
        try {
            // Create the node tree we'll be updating/moving
            JcrUtils.getOrCreateByPath("/var/oak-bug/parent/child",
                    JcrConstants.NT_UNSTRUCTURED, JcrConstants.NT_UNSTRUCTURED,
                    session, false);
            session.save();
        } finally {
            session.logout();
        }

        // move parent
        session = login();
        try {
            // Create the new node to move the parent tree onto
            JcrUtils.getOrCreateByPath("/var/oak-bug/test",
                    JcrConstants.NT_UNSTRUCTURED, JcrConstants.NT_UNSTRUCTURED,
                    session, false);
            // Move the parent tree - this operation in this session, i believe, eventually causes the error later.
            session.move("/var/oak-bug/parent", "/var/oak-bug/test/parent");
            session.save();
        } finally {
            session.logout();
        }

        // generate NPE
        session = login();
        try {
            // We rename the parent to a new node, as we're going to replace it with new content that needs to be under the original name.
            session.move("/var/oak-bug/test/parent", "/var/oak-bug/test/tmp-1234");

            // Create the new node using the original name "parent"
            Node parent = JcrUtils.getOrCreateByPath("/var/oak-bug/test/parent",
                    JcrConstants.NT_UNSTRUCTURED, JcrConstants.NT_UNSTRUCTURED,
                    session, false);

            // We need to preserve the original parent's children, so grab the child from the previous parent that was renamed,
            Node child = session.getNode("/var/oak-bug/test/tmp-1234/child");

            // What we really want to do is copy the child, but creating a new node with the same name is sufficient.
            // In the real world, this would have content on it, so all the properties and children would be copied - for testing it doesn't matter.
            // JcrUtil.copy(child, parent, "child", false);
            parent.addNode(child.getName(), child.getPrimaryNodeType().getName());

            assertTrue(session.hasPendingChanges()); // None of these changes have been persisted yet. This is to verify that no auto-saves have occurred.

            // Now, we need to actually process the child, so we need to move it so a new node can be created in its place, with the name "child".
            session.move("/var/oak-bug/test/parent/child", "/var/oak-bug/test/parent/tmp-4321"); // NPE On this Call.

            session.save();
        } finally {
            session.logout();
        }
    }

    private Session login() throws RepositoryException {
        return createAdminSession();
    }
}
