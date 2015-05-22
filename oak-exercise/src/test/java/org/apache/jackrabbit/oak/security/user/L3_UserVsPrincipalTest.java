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
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;

/**
 * <pre>
 * Module: User Management | Principal Management
 * =============================================================================
 *
 * Title: User vs. Principal
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the difference between {@link org.apache.jackrabbit.api.security.user.User}
 * and {@link java.security.Principal}.
 *
 * Exercises:
 *
 * - {@link #TODO}
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
public class L3_UserVsPrincipalTest extends AbstractSecurityTest {

    private static final String USER_ID = "testUser";
    private static final String PRINCIPAL_NAME = "testPrincipal";

    private static final String GROUP_ID = "testGroup";
    private static final String GROUP_PRINCIPAL_NAME = "testGroupPrincipal";

    private User testUser;
    private Group testGroup;

    @Override
    public void before() throws Exception {
        super.before();

        UserManager userMgr = getUserManager(root);
        testUser = userMgr.createUser(USER_ID, "pw", new PrincipalImpl(PRINCIPAL_NAME), null);
        testGroup = userMgr.createGroup(GROUP_ID, new PrincipalImpl(GROUP_PRINCIPAL_NAME), null);

        testGroup.addMember(testUser);
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            testUser.remove();
            testGroup.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    // TODO: look at authoriable node -> what properties contain which value?
    // TODO: lookup authoriable by id -> OK
    // TODO: lookup authorizable by principal name -> FAILS
    // TODO: lookup authorizable by principal -> OK
    // TODO: lookup principal by id -> FAILS
    // TODO: lookup principal by principalName -> OK
    // TODO: login with principalName -> FAILS
    // TODO: login with id -> OK
    // TODO: login -> look at AuthInfo -> what's in there?
    // TODO: ace -> create ace -> what's in there


}