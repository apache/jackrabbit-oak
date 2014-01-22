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

import java.security.Principal;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ImportBesteffortTest extends ImportIgnoreTest {

    protected String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    @Test
    public void testImportUnknownPrincipal() throws Exception {
        runImport();

        AccessControlManager acMgr = adminSession.getAccessControlManager();
        AccessControlPolicy[] policies = acMgr.getPolicies(target.getPath());

        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof JackrabbitAccessControlList);

        AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
        assertEquals(1, entries.length);

        AccessControlEntry entry = entries[0];
        assertEquals("unknownprincipal", entry.getPrincipal().getName());
        assertEquals(1, entry.getPrivileges().length);
        assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

        if (entry instanceof JackrabbitAccessControlEntry) {
            assertTrue(((JackrabbitAccessControlEntry) entry).isAllow());
        }
    }

    @Test
    public void testAddEntry() throws RepositoryException {
        Principal unknown = new Principal() {
            @Override
            public String getName() {
                return "anotherUnknown";
            }
        };
        AccessControlUtils.addAccessControlEntry(adminSession, target.getPath(), unknown, new String[] {Privilege.JCR_READ}, true);
    }
}
