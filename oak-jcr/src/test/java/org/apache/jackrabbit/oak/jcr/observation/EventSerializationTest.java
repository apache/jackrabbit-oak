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
package org.apache.jackrabbit.oak.jcr.observation;

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.observation.Event;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.observation.ExpectationListener.Expectation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EventSerializationTest extends AbstractRepositoryTest {

    public static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    public static final String TEST_NODE= "testnode";
    public static final int TIME_OUT = 12;
    public static final String TEST_TYPE="mix:test";

    public static final String BINARY_1 = "binary_String_1";
    public static final Binary BINARY_VALUE_1 = binaryForString(BINARY_1);
    public static final String BINARY_2 = "binary_string_2_";
    public static final Binary BINARY_VALUE_2 = binaryForString(BINARY_2);


    Session observingSession;
    ObservationManager observationManager;

    public EventSerializationTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();

        NodeTypeManager ntMgr = session.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate mixTest = ntMgr.createNodeTypeTemplate();
        mixTest.setName(TEST_TYPE);
        mixTest.setMixin(true);
        ntMgr.registerNodeType(mixTest, false);

        Node n = session.getRootNode().addNode(TEST_NODE);
        n.addMixin(TEST_TYPE);
        n.setProperty("test_property1", 42);
        n.setProperty("test_property2", "forty_two");
        session.save();

        observingSession = createAdminSession();
        observationManager = observingSession.getWorkspace().getObservationManager();
    }

    @After
    public void tearDown() {
        if (observingSession != null) {
            observingSession.logout();
        }
    }

    @Test
    public void checkSerializationOfBinaryProperties() throws RepositoryException, ExecutionException, InterruptedException {
        ExpectationListener listener = new ExpectationListener();
        observationManager.addEventListener(listener, ALL_EVENTS, "/", true, null, null, false);
        Session adminSession = getAdminSession();
        try {
            Node n = adminSession.getNode("/" + TEST_NODE);

            listener.expectEvent("check for added binary property",e -> {
                try {
                    return isEqual("/" + TEST_NODE+"/binary", e.getPath())
                           && isEqual(Event.PROPERTY_ADDED, e.getType()) 
                           && isEqual(BINARY_1, e.getInfo().get("afterValue").toString())
                           && isFalse(e.toString().contains(BINARY_1));
                } catch (Exception ex) {
                    return false;
                }
            });
            Property binary = n.setProperty("binary", BINARY_VALUE_1);
            adminSession.save();
            assertEquals(BINARY_1,binary.getString());

            // modify the property
            listener.expectEvent("check for modified binary property",e -> {
                try {
                    return isEqual("/" + TEST_NODE+"/binary", e.getPath())
                           && isEqual(Event.PROPERTY_CHANGED, e.getType())
                           && isEqual(BINARY_1, e.getInfo().get("beforeValue").toString())
                           && isEqual(BINARY_2, e.getInfo().get("afterValue").toString())
                           && isFalse(e.toString().contains(BINARY_1))
                           && isFalse(e.toString().contains(BINARY_2));
                } catch (Exception ex) {
                    return false;
                }
            });
            Property binary2 = n.setProperty("binary", BINARY_VALUE_2);
            adminSession.save();
            assertEquals(BINARY_2, binary2.getString());

            // remove the property
            listener.expectEvent("check for removed binary property",e -> {
                try {
                    return isEqual("/" + TEST_NODE+"/binary", e.getPath())
                           && isEqual(Event.PROPERTY_REMOVED, e.getType())
                           && isEqual(BINARY_2, e.getInfo().get("beforeValue").toString())
                           && isFalse(e.toString().contains(BINARY_2));
                } catch (Exception|AssertionError ex) {
                    return false;
                }
            });
            binary2.remove();
            adminSession.save();

            List<Expectation> missing = listener.getMissing(TIME_OUT, TimeUnit.SECONDS);
            assertTrue("Missing events: " + missing, missing.isEmpty());
            List<Event> unexpected = listener.getUnexpected();
            assertTrue("Unexpected events: " + unexpected, unexpected.isEmpty());
        }
        finally {
            observationManager.removeEventListener(listener);
        }
    }

    private static Binary binaryForString(String s) {
        return new BinaryImpl(s);
    }

    // A very simple memory-based implementation of a binary property
    private static class BinaryImpl implements Binary {

        byte[] buffer;

        public BinaryImpl(String s) {
            buffer = s.getBytes();
        }

        @Override
        public InputStream getStream() throws RepositoryException {
            return new ByteArrayInputStream(buffer);
        }

        @Override
        public int read(byte[] b, long position) throws IOException, RepositoryException {
            int length = Math.min(b.length, buffer.length - (int) position);
            if (length > 0) {
                System.arraycopy(buffer, (int) position, b, 0, length);
                return length;
            } else {
                return -1;
            }
        }

        @Override
        public long getSize() throws RepositoryException {
            return buffer.length;
        }

        @Override
        public void dispose() {
            // nothing to do
        }
    }

    private static boolean isEqual(Object o1, Object o2) {
        return o1.equals(o2);
    }

    private static boolean isFalse(boolean b) {
        return b == false;
    }
}
