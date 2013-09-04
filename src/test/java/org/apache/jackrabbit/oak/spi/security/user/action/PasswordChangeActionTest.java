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
package org.apache.jackrabbit.oak.spi.security.user.action;

import java.util.UUID;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class PasswordChangeActionTest extends AbstractSecurityTest {

    private PasswordChangeAction pwChangeAction;

    @Before
    public void before() throws Exception {
        super.before();
        pwChangeAction = new PasswordChangeAction();
        pwChangeAction.init(getSecurityProvider(), ConfigurationParameters.EMPTY);
    }

    @Test
    public void testNullPassword() throws Exception {
        try {
            pwChangeAction.onPasswordChange(getTestUser(), null, root, getNamePathMapper());
            fail("ConstraintViolationException expected.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testSamePassword() throws Exception {
        try {
            User user = getTestUser();
            String pw = user.getID();
            pwChangeAction.onPasswordChange(user, pw, root, getNamePathMapper());
            fail("ConstraintViolationException expected.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testPasswordChange() throws Exception {
        pwChangeAction.onPasswordChange(getTestUser(), "changedPassword", root, getNamePathMapper());
    }

    @Test
    public void testUserWithoutPassword() throws Exception {
        String uid = "testUser" + UUID.randomUUID();
        User user = getUserManager(root).createUser(uid, null);

        pwChangeAction.onPasswordChange(user, "changedPassword", root, getNamePathMapper());
    }
}