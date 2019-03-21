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
package org.apache.jackrabbit.oak.benchmark.authorization;

import static javax.jcr.security.Privilege.JCR_READ;
import static org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils.addAccessControlEntry;
import static org.junit.Assert.assertFalse;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.benchmark.AbstractTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;

/**
 * Tests the behavior of the permission cache when faced with lots of paths that
 * have no relevant policies for the current session (but may have other
 * policies). For more info see OAK-7860.
 */
public class CanReadNonExisting extends AbstractTest {

    static final String uid = "u0";

    static final int contentNodes = 10000;
    
    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();

        //PermissionEntryProviderImpl#DEFAULT_SIZE + delta
        int groupCount = 255;

        Session s = loginAdministrative();
        addAccessControlEntry(s, "/", EveryonePrincipal.getInstance(), new String[] { Privilege.JCR_READ }, false);

        // PermissionCacheBuilder#MAX_PATHS_SIZE + 1
        int extraPolicies = 11;
        Node extras = s.getNode("/").addNode("extras");
        for (int i = 0; i < extraPolicies; i++) {
            extras.addNode(i + "");
        }
        s.save();

        try {
            UserManager userManager = ((JackrabbitSession) s).getUserManager();

            User eye = userManager.createUser("eye", "eye");
            User u = userManager.createUser(uid, uid);
            addAccessControlEntry(s, u.getPath(), u.getPrincipal(), new String[] { JCR_READ }, true);
            for (int i = 0; i < extraPolicies; i++) {
                addAccessControlEntry(s, "/extras/" + i, u.getPrincipal(), new String[] { JCR_READ }, true);
            }

            for (int i = 1; i <= groupCount; i++) {
                Group g = userManager.createGroup(new PrincipalImpl("g" + i));
                g.addMember(u);
                addAccessControlEntry(s, g.getPath(), g.getPrincipal(), new String[] { JCR_READ }, true);
                for (int j = 0; j < extraPolicies; j++) {
                    addAccessControlEntry(s, "/extras/" + j, g.getPrincipal(), new String[] { JCR_READ }, true);
                }
                s.save();
            }

            Node content = s.getNode("/").addNode("content");
            for (int i = 0; i < contentNodes; i++) {
                String p = content.addNode(i + "").getPath();
                addAccessControlEntry(s, p, eye.getPrincipal(), new String[] { JCR_READ }, true);
            }
            s.save();

        } finally {
            s.save();
            s.logout();
        }
        System.out.println("setup done.");
    }

    @Override
    public void runTest() throws Exception {
        Session s = null;
        try {
            s = login(new SimpleCredentials(uid, uid.toCharArray()));
            for (int i = 0; i < contentNodes; i++) {
                assertFalse(s.nodeExists("/content/" + i));
            }
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }
}
