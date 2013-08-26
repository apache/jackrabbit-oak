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

import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static org.apache.jackrabbit.oak.jcr.RepositoryImpl.REFRESH_INTERVAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jcr.Credentials;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This class contains test cases which demonstrate changes in behaviour wrt. to Jackrabbit 2.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/OAK-14">OAK-14: Identify and document changes in behaviour wrt. Jackrabbit 2</a>
 */
@RunWith(Parameterized.class)
public class CompatibilityIssuesTest extends AbstractRepositoryTest {

    public CompatibilityIssuesTest(NodeStoreFixture fixture) {
        super(fixture);
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


    /**
     * OAK-939 - Change in behaviour from JR2. Following test case leads to
     * CommitFailedException but it passes in JR2
     */
    @Test
    public void removeNodeInDifferentSession() throws Throwable {
        final String testNode = "test_node";
        final String testNodePath = '/' + testNode;

        // Create the test node
        Session session = getAdminSession();
        session.getRootNode().addNode(testNode);
        session.save();

        // Test case would pass if the sessionRefreshInterval is set to zero
        boolean refreshIntervalZero = false;
        Session s3 = newSession(refreshIntervalZero);
        Session s2 = newSession(refreshIntervalZero);

        s2.getNode(testNodePath).setProperty("foo", "bar");
        s2.save();

        s3.getNode(testNodePath).remove();
        try {
            s3.save();
        } catch (InvalidItemStateException e) {
            assertTrue(e.getCause() instanceof CommitFailedException);
        }
    }

    private Session newSession(boolean refreshIntervalZero) throws RepositoryException {
        Credentials creds = new SimpleCredentials("admin", "admin".toCharArray());
        if (refreshIntervalZero){
            return ((JackrabbitRepository) getRepository())
                    .login(creds, null, Collections.<String, Object>singletonMap(REFRESH_INTERVAL, 0));
        } else{
            return getRepository().login(creds);
        }
    }

    /**
     * OAK-948 - JR2 generates propertyChange event for touched properties while Oak does not
     */
    @Test
    public void noEventsForTouchedProperties() throws RepositoryException, InterruptedException {
        final String testNodeName = "test_touched_node";
        final String testNodePath = '/' + testNodeName;

        // Create the test node
        Session session = getAdminSession();
        Node testNode = session.getRootNode().addNode(testNodeName);
        testNode.setProperty("foo", "bar");
        testNode.setProperty("foo2", "bar0");
        session.save();

        Session observingSession = createAdminSession();
        ObservationManager observationManager = observingSession.getWorkspace().getObservationManager();

        try{
            final List<Event> events = new ArrayList<Event>();
            final CountDownLatch latch = new CountDownLatch(1);
            EventListener listener = new EventListener() {
                @Override
                public void onEvent(EventIterator eventIt) {
                    while(eventIt.hasNext()){
                        events.add(eventIt.nextEvent());
                    }
                    if(!events.isEmpty()){
                        latch.countDown();
                    }
                }
            };

            observationManager.addEventListener(listener, PROPERTY_CHANGED, testNodePath, true, null, null, false);

            //Now touch foo and modify foo2
            session.getNode(testNodePath).setProperty("foo","bar");
            session.getNode(testNodePath).setProperty("foo2","bar2");
            session.save();

            latch.await(10,TimeUnit.SECONDS);

            //Only one event is recorded for foo2 modification
            assertEquals(1,events.size());
        }finally{
            observingSession.logout();
        }
    }
    
    @Test
    public void noSNSSupport() throws RepositoryException{
        Session session = getAdminSession();
        Node testNode = session.getRootNode().addNode("test", "nt:unstructured");
        session.save();

        testNode.addNode("foo");
        try {
            testNode.addNode("foo");
            // This would fail on JR2 since there SNSs are supported
            fail("Expected ItemExistsException");
        } catch (ItemExistsException e){
            //ItemExistsException is expected to be thrown
        }
        session.save();
        try {
            testNode.addNode("foo");
            // This would fail on JR2 since there SNSs are supported
            fail("Expected ItemExistsException");
        } catch (ItemExistsException e){
            //ItemExistsException is expected to be thrown
        }
    }

}
