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

import java.util.UUID;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.api.util.Text;

/**
 * Testing group import with default {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 */
public class GroupImportWithoutAdminTest extends GroupImportTest {

    private String uid = "testUser" + UUID.randomUUID();
    private Session testSession;

    @Override
    public void before() throws Exception {
        super.before();

        User u = userMgr.createUser(uid, "pw");
        adminSession.save();

        AccessControlUtils.addAccessControlEntry(adminSession, Text.getRelativeParent(getTargetPath(), 1), u.getPrincipal(), new String[] {Privilege.JCR_ALL}, true);
        AccessControlUtils.addAccessControlEntry(adminSession, null, u.getPrincipal(), new String[] {PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT}, true);
        adminSession.save();

        testSession = adminSession.getRepository().login(new SimpleCredentials(uid, "pw".toCharArray()));
    }

    @Override
    public void after() throws Exception {
        try {
            testSession.logout();

            Authorizable testUser = userMgr.getAuthorizable(uid);
            testUser.remove();
            adminSession.save();
        } finally {
            super.after();
        }
    }

    @Override
    protected Session getImportSession() {
        return testSession;
    }
}
