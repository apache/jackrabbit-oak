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
package org.apache.jackrabbit.oak.benchmark;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;

/**
 * Test the performance of adding a configured number of members to groups. The
 * following parameters can be used to run the benchmark:
 *
 * - numberOfMembers : the number of members that should be added in the test run
 *
 * In contrast to {@link AddMembersTest}, this benchmark will always call
 * {@link Group#addMember(Authorizable)}.
 */
public class AddMemberTest extends AddMembersTest {

    private final List<String> userPaths;

    public AddMemberTest(int numberOfMembers) {
        super(numberOfMembers, 1, ImportBehavior.NAME_ABORT);
        userPaths = new ArrayList<String>(numberOfMembers);
    }

    protected void createUsers(@Nonnull UserManager userManager) throws Exception {
        for (int i = 0; i <= numberOfMembers; i++) {
            String id = USER + i;
            User u = userManager.createUser(id, null, new PrincipalImpl(id), REL_TEST_PATH);
            userPaths.add(u.getPath());
        }
    }

    @Override
    public void runTest() throws Exception {
        Session s = null;
        try {
            // use system session login to avoid measuring the login-performance here
            s = Subject.doAsPrivileged(SystemSubject.INSTANCE, new PrivilegedExceptionAction<Session>() {
                @Override
                public Session run() throws Exception {
                    return getRepository().login(null, null);
                }
            }, null);
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            String groupId = GROUP + random.nextInt(GROUP_CNT);
            Group g = userManager.getAuthorizable(groupId, Group.class);

        } catch (RepositoryException e) {
            System.out.println(e.getMessage());
            if (s.hasPendingChanges()) {
                s.refresh(false);
            }
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    @Override
    protected void addMembers(@Nonnull UserManager userManager, @Nonnull Group group, @Nonnull Session s) throws Exception {
        for (int i = 0; i <= numberOfMembers; i++) {
            String userPath = userPaths.get(random.nextInt(numberOfMembers));
            group.addMember(userManager.getAuthorizableByPath(userPath));
            s.save();
        }
    }
}
