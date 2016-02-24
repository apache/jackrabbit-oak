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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.security.Principal;
import javax.annotation.Nonnull;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.test.NotExecutableException;

public class PrincipalLookupTest extends AbstractUserTest {

    private static PrincipalManager getPrincipalManager(@Nonnull Session session) throws Exception {
        if (!(session instanceof JackrabbitSession)) {
            throw new NotExecutableException("JackrabbitSession expected");
        }
        return ((JackrabbitSession) session).getPrincipalManager();
    }

    public void testPrincipalManager() throws Exception {
        Principal p = getPrincipalManager(superuser).getPrincipal(group.getPrincipal().getName());
        assertNotNull(p);
        assertEquals(group.getPrincipal(), p);
    }

    public void testPrincipalManagerOtherSession() throws Exception {
        Session s2 = null;
        try {
            s2 = getHelper().getSuperuserSession();

            Principal p = getPrincipalManager(s2).getPrincipal(group.getPrincipal().getName());
            assertNotNull(p);
            assertEquals(group.getPrincipal(), p);
        } finally {
            if (s2 != null) {
                s2.logout();
            }
        }
    }
}