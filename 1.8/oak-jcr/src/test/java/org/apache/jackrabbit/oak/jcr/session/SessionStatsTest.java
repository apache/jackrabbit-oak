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
package org.apache.jackrabbit.oak.jcr.session;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.test.AbstractJCRTest;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.ArrayList;
import java.util.List;

public class SessionStatsTest extends AbstractJCRTest {

    /**
     * Tests if the init stack trace is not recorded by default.
     */
    public void testInitStackTraceDisabledByDefault() throws IllegalAccessException {
        assertTrue("initStackTrace is not empty", getInitStackTrace(superuser).isEmpty());
    }

    /**
     * Tests if the init stack trace is recorded after opening a lot of sessions.
     */
    public void testInitStackTraceEnabledAfterOpeningManySessions()
            throws IllegalAccessException, RepositoryException {
        final int sessionCount = SessionStats.INIT_STACK_TRACE_THRESHOLD + 1;
        final List<Session> sessions = new ArrayList<Session>(sessionCount);
        for (int i = 0; i < sessionCount; i++) {
            sessions.add(createSession());
        }

        // Stack trace should be recorded by now
        Session lastSession = sessions.get(sessionCount - 1);
        assertFalse("initStackTrace is empty", getInitStackTrace(lastSession).isEmpty());

        for (Session session : sessions) {
            session.logout();
        }

        // Stack trace should not be recorded anymore
        Session afterSession = createSession();
        assertTrue("initStackTrace is not empty", getInitStackTrace(afterSession).isEmpty());
        afterSession.logout();
    }

    private Session createSession() throws RepositoryException {
        return getHelper().getReadWriteSession();
    }

    private String getInitStackTrace(Session session) throws IllegalAccessException {
        SessionDelegate sessionDelegate = (SessionDelegate) FieldUtils.readDeclaredField(session, "sd", true);
        SessionStats sessionStats = sessionDelegate.getSessionStats();
        return sessionStats.getInitStackTrace();
    }

}