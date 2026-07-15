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

import java.util.ArrayList;
import java.util.List;
import javax.jcr.Session;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;

/**
 * Test the performance of adding a configured number of members to groups. The
 * following parameters can be used to run the benchmark:
 *
 * - numberOfMembers : the number of members that should be added in the test run
 * - batchSize : the number of users to be added as members before {@link Session#save()} is called.
 *
 * In contrast to {@link AddMembersTest}, this benchmark will always call
 * {@link Group#addMember(Authorizable)}.
 */
public class AddMemberTest extends AddMembersTest {

    private final List<String> userPaths;

    public AddMemberTest(int numberOfMembers, int batchSize) {
        super(numberOfMembers, batchSize, ImportBehavior.NAME_ABORT);
        userPaths = new ArrayList<String>(numberOfMembers);
    }

    @Override
    protected void createUsers(@NotNull UserManager userManager) throws Exception {
        for (int i = 0; i <= numberOfMembers; i++) {
            String id = USER + i;
            User u = userManager.createUser(id, null, new PrincipalImpl(id), REL_TEST_PATH);
            userPaths.add(u.getPath());
        }
    }

    @Override
    protected void addMembers(@NotNull UserManager userManager, @NotNull Group group, @NotNull Session s) throws Exception {
        int j = 1;
        for (int i = 0; i <= numberOfMembers; i++) {
            String userPath = userPaths.get(random.nextInt(numberOfMembers));
            group.addMember(userManager.getAuthorizableByPath(userPath));
            if (j == batchSize) {
                s.save();
                j = 1;
            } else {
                j++;
            }
        }
    }
}
