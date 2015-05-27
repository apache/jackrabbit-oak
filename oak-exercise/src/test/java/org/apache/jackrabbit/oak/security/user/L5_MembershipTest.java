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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.security.ExerciseUtility;
import org.junit.Test;

/**
 * <pre>
 * Module: TODO
 * =============================================================================
 *
 * Title: Group Membership
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * TODO
 *
 * Exercises:
 *
 * - {@link #TODO}
 *
 * -
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * TODO
 *
 * </pre>
 *
 * @see TODO
 */
public class L5_MembershipTest extends AbstractSecurityTest {

    private User user;
    private Group group;

    @Override
    public void before() throws Exception {
        super.before();

        user = getTestUser();

        group = ExerciseUtility.createTestGroup(getUserManager(root));
        group.addMember(user);

        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (group != null) {
                group.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testInheritedMembership() {
        // TODO
    }

    @Test
    public void testDeclaredMembership() {
        // TODO
    }

    @Test
    public void testMembersContentStructure() {
        // TODO
    }
}