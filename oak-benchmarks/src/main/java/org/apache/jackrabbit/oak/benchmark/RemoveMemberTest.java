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
import javax.annotation.Nonnull;
import javax.jcr.Session;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;

/**
 * Test the performance of removing members from groups. The
 * following parameters can be used to run the benchmark:
 *
 * - numberOfMembers : numberOfMembers : the number of members that should be
 *   added in the test setup to each group and later on removed during the test-run.
 *
 * Note the members to be removed are picked randomly and may or may not/no longer
 * be member of the target group.
 */
public class RemoveMemberTest extends RemoveMembersTest {

    private final List<String> userPaths;

    public RemoveMemberTest(int numberOfMembers, int batchSize) {
        super(numberOfMembers, batchSize);
        userPaths = new ArrayList<String>(numberOfMembers);
    }

    protected void createUsers(@Nonnull UserManager userManager) throws Exception {
        for (int i = 0; i <= numberOfMembers; i++) {
            String id = USER + i;
            User u = userManager.createUser(id, null, new PrincipalImpl(id), REL_TEST_PATH);
            userPaths.add(u.getPath());
        }
    }

    protected void removeMembers(@Nonnull UserManager userManger, @Nonnull Group group, @Nonnull Session s) throws Exception {
        int j = 1;
        for (int i = 0; i <= numberOfMembers; i++) {
            Authorizable member = userManger.getAuthorizable(USER + random.nextInt(numberOfMembers));
            if (group.removeMember(member)) {
                if (j == batchSize) {
                    s.save();
                    j = 1;
                } else {
                    j++;
                }
            }
        }
    }
}
