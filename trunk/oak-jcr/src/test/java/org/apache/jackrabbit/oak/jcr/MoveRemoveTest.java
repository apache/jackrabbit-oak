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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

public class MoveRemoveTest extends AbstractRepositoryTest {

    public MoveRemoveTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void removeExistingNode() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("new");
        session.save();

        Node n = session.getNode("/new");
        n.remove();

        try {
            n.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("new");
        assertEquals("/new", n.getPath());
    }

    @Test
    public void removeNewNode() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("new");

        Node n = session.getNode("/new");
        n.remove();

        try {
            n.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("new");
        assertEquals("/new", n.getPath());
    }

    @Test
    public void removeExistingNodeRefresh() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("new");
        session.save();

        Session session2 = createAdminSession();
        Node n2 = session2.getNode("/new");

        Node n = session.getNode("/new");
        n.remove();
        session.save();

        session2.refresh(false);
        try {
            n2.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("new");
        session.save();

        session2.refresh(false);
        assertEquals("/new", n2.getPath());

        session2.logout();
    }

    @Test
    public void removeExistingNodeParent() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("parent").addNode("new");
        session.save();

        Node n = session.getNode("/parent/new");
        n.getParent().remove();

        try {
            n.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("parent").addNode("new");
        assertEquals("/parent/new", n.getPath());
    }

    @Test
    public void removeNewNodeParent() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("parent").addNode("new");

        Node n = session.getNode("/parent/new");
        n.getParent().remove();

        try {
            n.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("parent").addNode("new");
        assertEquals("/parent/new", n.getPath());
    }

    @Test
    public void removeExistingNodeRefreshParent() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("parent").addNode("new");
        session.save();

        Session session2 = createAdminSession();
        Node n2 = session2.getNode("/parent/new");

        Node n = session.getNode("/parent/new");
        n.getParent().remove();
        session.save();

        session2.refresh(false);
        try {
            n2.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("parent").addNode("new");
        session.save();

        session2.refresh(false);
        assertEquals("/parent/new", n2.getPath());

        session2.logout();
    }

    @Test
    public void moveExistingNode() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("new");
        session.save();

        Node n = session.getNode("/new");
        session.move("/new", "/moved");

        assertEquals("/moved", n.getPath());
    }

    @Test
    public void moveNewNode() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("new");

        Node n = session.getNode("/new");
        session.move("/new", "/moved");

        assertEquals("/moved", n.getPath());
    }

    @Test
    public void moveExistingNodeRefresh() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("new");
        session.save();

        Session session2 = createAdminSession();
        Node n2 = session2.getNode("/new");

        Node n = session.getNode("/new");
        session.move("/new", "/moved");
        session.save();

        session2.refresh(false);
        try {
            n2.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("new");
        session.save();

        session2.refresh(false);
        assertEquals("/new", n2.getPath());

        session2.logout();
    }

    @Test
    public void moveExistingParent() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("parent").addNode("new");
        session.save();

        Node p = session.getNode("/parent");
        Node n = session.getNode("/parent/new");
        session.move("/parent", "/moved");

        assertEquals("/moved", p.getPath());
        assertEquals("/moved/new", n.getPath());
    }

    @Test
    public void moveNewNodeParent() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("parent").addNode("new");

        Node n = session.getNode("/parent/new");
        session.move("/parent", "/moved");

        assertEquals("/moved/new", n.getPath());
    }

    @Test
    public void moveExistingNodeRefreshParent() throws RepositoryException {
        Session session = getAdminSession();
        session.getRootNode().addNode("parent").addNode("new");
        session.save();

        Session session2 = createAdminSession();
        Node n2 = session2.getNode("/parent/new");

        Node n = session.getNode("/parent/new");
        session.move("/parent", "/moved");
        session.save();

        session2.refresh(false);
        try {
            n2.getPath();
            fail();
        } catch (InvalidItemStateException e) {}

        session.getRootNode().addNode("parent").addNode("new");
        session.save();

        session2.refresh(false);
        assertEquals("/parent/new", n2.getPath());

        session2.logout();
    }
}
