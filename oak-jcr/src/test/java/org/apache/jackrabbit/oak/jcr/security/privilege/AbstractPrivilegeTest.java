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
package org.apache.jackrabbit.oak.jcr.security.privilege;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Workspace;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * Base class for privilege management tests.
 */
abstract class AbstractPrivilegeTest extends AbstractJCRTest {

    static PrivilegeManager getPrivilegeManager(Session session) throws RepositoryException {
        Workspace workspace = session.getWorkspace();
        return ((JackrabbitWorkspace) workspace).getPrivilegeManager();
    }

    static String[] getAggregateNames(String... names) {
        return names;
    }

    static void assertContainsDeclared(Privilege privilege, String aggrName) {
        boolean found = false;
        for (Privilege p : privilege.getDeclaredAggregatePrivileges()) {
            if (aggrName.equals(p.getName())) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    void assertPrivilege(Privilege priv, String name, boolean isAggregate, boolean isAbstract) {
        assertNotNull(priv);
        String privName = priv.getName();
        assertEquals(privName, privName, name);
        assertEquals(privName, isAggregate, priv.isAggregate());
        assertEquals(privName,isAbstract, priv.isAbstract());
    }
}