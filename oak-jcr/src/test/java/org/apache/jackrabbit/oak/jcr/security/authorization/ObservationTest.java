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

import javax.jcr.observation.Event;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.test.api.observation.EventResult;
import org.junit.Ignore;
import org.junit.Test;

/**
 * ObservationTest... TODO
 */
@Ignore("OAK-51")
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
            obsMgr.addEventListener(listener, Event.NODE_REMOVED, path, true, new String[0], new String[0], true);

            // superuser removes the node with childNPath in order to provoke
            // events being generated
            superuser.getItem(childNPath).remove();
            superuser.save();

            obsMgr.removeEventListener(listener);
            // since the testUser does not have read-permission on the removed
            // node, no corresponding event must be generated.
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
}