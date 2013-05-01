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

import static org.junit.Assert.fail;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;

import org.junit.Test;

/**
 * This class contains test cases which demonstrate changes in behaviour wrt. to Jackrabbit 2.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/OAK-14">OAK-14: Identify and document changes in behaviour wrt. Jackrabbit 2</a>
 */
public class CompatibilityIssuesTest extends AbstractRepositoryTest {

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

    /**
     * Type checks are deferred to the Session#save call instead of the
     * Node#addNode method like in Jackrabbit2.
     * <p>Stacktrace in JR2:</p>
     * <pre>
     * {@code
     * javax.jcr.nodetype.ConstraintViolationException: No child node definition for fail found in node /f1362578560413
     *     at org.apache.jackrabbit.core.NodeImpl.addNode(NodeImpl.java:1276)
     *     at org.apache.jackrabbit.core.session.AddNodeOperation.perform(AddNodeOperation.java:111)
     *     at org.apache.jackrabbit.core.session.AddNodeOperation.perform(AddNodeOperation.java:1)
     *     at org.apache.jackrabbit.core.session.SessionState.perform(SessionState.java:216)
     *     at org.apache.jackrabbit.core.ItemImpl.perform(ItemImpl.java:91)
     *     at org.apache.jackrabbit.core.NodeImpl.addNodeWithUuid(NodeImpl.java:1814)
     *     at org.apache.jackrabbit.core.NodeImpl.addNode(NodeImpl.java:1774)
     * }
     * <pre>
     * <p>Stacktrace in Oak:</p>
     * <pre>
     * {@code
     *javax.jcr.nodetype.ConstraintViolationException
     *    at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
     *    at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:39)
     *    at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:27)
     *    at java.lang.reflect.Constructor.newInstance(Constructor.java:513)
     *    at org.apache.jackrabbit.oak.api.CommitFailedException.throwRepositoryException(CommitFailedException.java:57)
     *    at org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate.save(SessionDelegate.java:258)
     *    at org.apache.jackrabbit.oak.jcr.SessionImpl.save(SessionImpl.java:277)
     *    ...
     *Caused by: org.apache.jackrabbit.oak.api.CommitFailedException: Cannot add node 'f1362578685631' at /
     *    at org.apache.jackrabbit.oak.plugins.nodetype.TypeValidator.childNodeAdded(TypeValidator.java:128)
     *    at org.apache.jackrabbit.oak.spi.commit.CompositeValidator.childNodeAdded(CompositeValidator.java:68)
     *    at org.apache.jackrabbit.oak.spi.commit.ValidatingHook$ValidatorDiff.childNodeAdded(ValidatingHook.java:159)
     *    at org.apache.jackrabbit.oak.core.RootImpl.commit(RootImpl.java:250)
     *    at org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate.save(SessionDelegate.java:255)
     *    ...
     *Caused by: javax.jcr.nodetype.ConstraintViolationException: Node 'jcr:content' in 'nt:file' is mandatory
     *    at org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeImpl.checkMandatoryItems(EffectiveNodeTypeImpl.java:288)
     *    at org.apache.jackrabbit.oak.plugins.nodetype.TypeValidator.childNodeAdded(TypeValidator.java:125)
     *    ...
     * }
     * <pre>
     */
    @Test(expected = ConstraintViolationException.class)
    public void typeChecksOnSave() throws RepositoryException {
        Session session = getAdminSession();
        Node f = session.getNode("/").addNode("f" + System.currentTimeMillis(), "nt:file");
        f.addNode("fail", "nt:unstructured"); // this is where JR2 throws ConstraintViolationException
        session.save(); // // this is where OAK throws ConstraintViolationException
    }

}
