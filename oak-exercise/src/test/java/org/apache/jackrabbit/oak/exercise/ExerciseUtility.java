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
package org.apache.jackrabbit.oak.exercise;

import java.security.Principal;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;

public final class ExerciseUtility {

    public static final String TEST_USER_HINT = "testUser";
    public static final String TEST_GROUP_HINT = "testGroup";
    public static final String TEST_PRINCIPAL_HINT = "testPrincipal";
    public static final String TEST_GROUP_PRINCIPAL_HINT = "testGroupPrincipal";
    public static final String TEST_PW = "pw";

    private ExerciseUtility() {}

    public static String getTestId(@Nonnull String hint) {
        return hint + UUID.randomUUID().toString();
    }

    public static Principal getTestPrincipal(@Nonnull String hint) {
        String name = hint  + UUID.randomUUID().toString();
        return new PrincipalImpl(name);
    }

    public static User createTestUser(@Nonnull UserManager userMgr) throws RepositoryException {
        return userMgr.createUser(getTestId(TEST_USER_HINT), TEST_PW, getTestPrincipal(TEST_PRINCIPAL_HINT), null);
    }

    public static Group createTestGroup(@Nonnull UserManager userMgr) throws RepositoryException {
        return userMgr.createGroup(getTestId(TEST_GROUP_HINT), getTestPrincipal(TEST_GROUP_PRINCIPAL_HINT), null);
    }

    public static SimpleCredentials getTestCredentials(@Nonnull String userID) {
        return new SimpleCredentials(userID, TEST_PW.toCharArray());
    }
}