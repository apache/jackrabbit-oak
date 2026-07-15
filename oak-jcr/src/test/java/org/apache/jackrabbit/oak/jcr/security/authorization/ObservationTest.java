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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import java.util.ArrayList;
import java.util.List;
import javax.jcr.observation.Event;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.test.api.observation.EventResult;
import org.junit.Test;

/**
 * Permission evaluation tests related to observation.
 */
public class ObservationTest extends AbstractEvaluationTest {

    private static final long DEFAULT_WAIT_TIMEOUT = 5000;

    @Test
    public void testEventGeneration() throws Exception {
        // withdraw the READ privilege
        deny(path, readPrivileges);

        // testUser registers a event listener for 'path
        ObservationManager obsMgr = testSession.getWorkspace().getObservationManager();
        EventResult listener = new EventResult(this.log);
        try {
            obsMgr.addEventListener(listener, Event.NODE_REMOVED, testRoot, true, null, null, true);

            // superuser removes the node with childNPath & siblingPath in
            // order to provoke events being generated
            superuser.getItem(childNPath).remove();
            superuser.getItem(siblingPath).remove();
            superuser.save();

            // since the testUser does not have read-permission on the removed
            // childNPath, no corresponding event must be generated.
            Event[] evts = listener.getEvents(DEFAULT_WAIT_TIMEOUT);
            for (Event evt : evts) {
                if (evt.getType() == Event.NODE_REMOVED &&
                        evt.getPath().equals(childNPath)) {
                    fail("TestUser does not have READ permission below " + path + " -> events below must not show up.");
                }
            }
        } finally {
            obsMgr.removeEventListener(listener);
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4196">OAK-4196</a>
     */
    @Test
    public void testEventRemovedNodeWhenDenyEntryIsRemoved() throws Exception {
        // withdraw the READ privilege on childNPath
        deny(childNPath, readPrivileges);

        assertFalse(testSession.nodeExists(childNPath));
        assertTrue(testSession.nodeExists(childNPath2));

        // testUser registers a event listener for changes under testRoot
        ObservationManager obsMgr = testSession.getWorkspace().getObservationManager();
        EventResult listener = new EventResult(this.log);
        try {
            obsMgr.addEventListener(listener, Event.NODE_REMOVED, testRoot, true, null, null, true);

            // superuser removes the node with childNPath & childNPath2 in
            // order to provoke events being generated
            superuser.getItem(childNPath).remove();
            superuser.getItem(childNPath2).remove();
            superuser.save();

            // since the events are generated _after_ persisting all the changes
            // and the removal also removes the permission entries denying access
            // testUser will be notified about the removal because the remaining
            // permission setup after the removal grants read access.
            Event[] evts = listener.getEvents(DEFAULT_WAIT_TIMEOUT);
            List<String> eventPaths = new ArrayList<String>();
            for (Event evt : evts) {
                if (evt.getType() == Event.NODE_REMOVED) {
                    eventPaths.add(evt.getPath());
                }
            }
            assertTrue(eventPaths.contains(childNPath));
            assertTrue(eventPaths.contains(childNPath2));
        } finally {
            obsMgr.removeEventListener(listener);
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4196">OAK-4196</a>
     */
    @Test
    public void testEventRemovedNode() throws Exception {
        // withdraw the READ privilege on childNPath
        deny(path, readPrivileges);

        assertFalse(testSession.nodeExists(childNPath));

        // testUser registers a event listener for changes under testRoot
        ObservationManager obsMgr = testSession.getWorkspace().getObservationManager();
        EventResult listener = new EventResult(this.log);
        try {
            obsMgr.addEventListener(listener, Event.NODE_REMOVED, testRoot, true, null, null, true);

            // superuser removes the node with childNPath order to provoke events being generated
            superuser.getItem(childNPath).remove();
            superuser.save();

            // since the testUser does not have read-permission on the removed
            // childNPath, no corresponding event must be generated.
            Event[] evts = listener.getEvents(DEFAULT_WAIT_TIMEOUT);
            for (Event evt : evts) {
                if (evt.getType() == Event.NODE_REMOVED &&
                        evt.getPath().equals(childNPath)) {
                    fail("TestUser does not have READ permission on " + childNPath);
                }
            }
        } finally {
            obsMgr.removeEventListener(listener);
        }
    }
}