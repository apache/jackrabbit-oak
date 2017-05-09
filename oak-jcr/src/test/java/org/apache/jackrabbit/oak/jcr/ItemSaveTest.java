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

package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.junit.Before;
import org.junit.Test;

/**
 * See OAK-993
 */
public class ItemSaveTest extends AbstractRepositoryTest {

    public ItemSaveTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    private Session session;
    private Node root;
    private Node foo;
    private Property prop0;
    private Property prop1;
    private Property prop2;

    @Before
    public void setup() throws RepositoryException {
        session = getAdminSession();
        root = session.getRootNode();
        foo = root.addNode("foo").addNode("child0");
        prop0 = root.setProperty("p0", "v0");
        prop1 = foo.setProperty("p1", "v1");
        prop2 = foo.setProperty("p2", "v2");
        session.save();
    }

    @Test
    public void noChangesAtAll() throws RepositoryException {
        foo.save();
    }

    @Test
    public void saveContainsAllChanges() throws RepositoryException {
        foo.addNode("child");
        foo.save();
    }

    @Test
    public void saveOnRoot() throws RepositoryException {
        root.addNode("child");
        root.save();
    }

    @Test
    public void saveMissesNode() throws RepositoryException {
        assumeTrue(!NodeImpl.SAVE_SESSION);
        try {
            root.addNode("child1");
            foo.addNode("child2");
            foo.save();
            fail("Expected UnsupportedRepositoryOperationException");
        } catch (UnsupportedRepositoryOperationException e) {
            assertTrue(e.getCause() instanceof CommitFailedException);
        }
    }

    @Test
    public void saveOnNewNode() throws RepositoryException {
        assumeTrue(!NodeImpl.SAVE_SESSION);
        try {
            foo.addNode("child").save();
            fail("Expected UnsupportedRepositoryOperationException");
        } catch (UnsupportedRepositoryOperationException e) {
            assertTrue(e.getCause() instanceof CommitFailedException);
        }
    }

    @Test
    public void saveOnChangedProperty() throws RepositoryException {
        // Property on root
        prop0.setValue("changed");
        prop0.save();

        // Property on child node
        prop1.setValue("changed");
        prop1.save();
    }

    @Test
    public void saveMissesProperty() throws RepositoryException {
        assumeTrue(!NodeImpl.SAVE_SESSION);
        try {
            prop1.setValue("changed");
            prop2.setValue("changed");
            prop1.save();
            fail("Expected UnsupportedRepositoryOperationException");
        } catch (UnsupportedRepositoryOperationException e) {
            assertTrue(e.getCause() instanceof CommitFailedException);
        } finally {
            session.refresh(false);
        }
    }

    @Test
    public void saveOnNewProperty() throws RepositoryException {
        assumeTrue(!NodeImpl.SAVE_SESSION);
        try {
            foo.setProperty("p3", "v3").save();
            fail("Expected UnsupportedRepositoryOperationException");
        } catch (UnsupportedRepositoryOperationException e) {
            assertTrue(e.getCause() instanceof CommitFailedException);
        } finally {
            session.refresh(false);
        }
    }

}
