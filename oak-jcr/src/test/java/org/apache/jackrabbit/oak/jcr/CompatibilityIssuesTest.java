/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class contains test cases which demonstrate changes in behaviour wrt. to Jackrabbit 2.
 * See OAK-14: Identify and document changes in behaviour wrt. Jackrabbit 2
 */
public class CompatibilityIssuesTest extends AbstractRepositoryTest {

    /**
     * Trans-session isolation differs from Jackrabbit 2. Snapshot isolation can
     * result in write skew as this test demonstrates: the check method enforces
     * an application logic constraint which says that the sum of the properties
     * p1 and p2 must not be negative. While session1 and session2 each enforce
     * this constraint before saving, the constraint might not hold globally as
     * can be seen in session3.
     *
     * @see <a href="http://wiki.apache.org/jackrabbit/Transactional%20model%20of%20the%20Microkernel%20based%20Jackrabbit%20prototype">
     *     Transactional model of the Microkernel based Jackrabbit prototype</a>
     */
    @Test
    public void sessionIsolation() throws RepositoryException {
        Session session0 = createAdminSession();
        Session session1 = null;
        Session session2 = null;
        try {
            Node testNode = session0.getNode("/").addNode("testNode");
            testNode.setProperty("p1", 1);
            testNode.setProperty("p2", 1);
            session0.save();
            check(getAdminSession());

            session1 = createAdminSession();
            session2 = createAdminSession();
            session1.getNode("/testNode").setProperty("p1", -1);
            check(session1);
            session1.save();

            session2.getNode("/testNode").setProperty("p2", -1);
            check(session2);      // Throws on JR2, not on Oak
            session2.save();

            Session session3 = createAnonymousSession();
            try {
                check(session3);  // Throws on Oak
                fail();
            } catch (AssertionError e) {
                // expected
            } finally {
                session3.logout();
            }

        } finally {
            session0.logout();
            if (session1 != null) {
                session1.logout();
            }
            if (session2 != null) {
                session2.logout();
            }
        }
    }

    private static void check(Session session) throws RepositoryException {
        if (session.getNode("/testNode").getProperty("p1").getLong() +
                session.getNode("/testNode").getProperty("p2").getLong() < 0) {
            fail("p1 + p2 < 0");
        }
    }

    @Test
    public void move() throws RepositoryException {
        Session session = getAdminSession();

        Node node = session.getNode("/");
        node.addNode("source").addNode("node");
        node.addNode("target");
        session.save();

        session.refresh(true);
        Node sourceNode = session.getNode("/source/node");
        session.move("/source/node", "/target/moved");
        // assertEquals("/target/moved", sourceNode.getPath());  // passes on JR2, fails on Oak
        try {
            sourceNode.getPath();
        }
        catch (InvalidItemStateException expected) {
            // sourceNode is stale
        }
    }

    @Test
    public void move2() throws CommitFailedException {
        ContentSession session = new Oak().createContentSession();
        Root root = session.getLatestRoot();
        root.getTree("/").addChild("x");
        root.getTree("/").addChild("y");
        root.commit();

        Tree r = root.getTree("/");
        Tree x = r.getChild("x");
        Tree y = r.getChild("y");

        assertFalse(y.hasChild("x"));
        assertEquals("", x.getParent().getName());
        root.move("/x", "/y/x");
        assertTrue(y.hasChild("x"));
        // assertEquals("y", x.getParent().getName());  // passed on JR2, fails on Oak
        assertEquals("", x.getParent().getName());      // fails on JR2, passes on Oak
    }

}
