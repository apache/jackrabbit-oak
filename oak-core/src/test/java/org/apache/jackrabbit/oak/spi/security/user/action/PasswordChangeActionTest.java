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

import javax.annotation.Nullable;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class PasswordChangeActionTest {

    private static final String USER_PATH = "/userpath";

    private final NamePathMapper namePathMapper = Mockito.mock(NamePathMapper.class);

    private PasswordChangeAction pwChangeAction;

    private User user;

    @Before
    public void before() throws Exception {
        pwChangeAction = new PasswordChangeAction();
        pwChangeAction.init(Mockito.mock(SecurityProvider.class), ConfigurationParameters.EMPTY);

        user = Mockito.mock(User.class);
        when(user.getPath()).thenReturn(USER_PATH);
    }

    private static Root createRoot(@Nullable String pw) throws Exception {
        Tree userTree = Mockito.mock(Tree.class);
        if (pw != null) {
            String pwHash = PasswordUtil.buildPasswordHash(pw);
            when(userTree.getProperty(UserConstants.REP_PASSWORD)).thenReturn(PropertyStates.createProperty(UserConstants.REP_PASSWORD, pwHash));
        }
        Root root = Mockito.mock(Root.class);
        when(root.getTree(USER_PATH)).thenReturn(userTree);
        return root;
    }

    @Test(expected = ConstraintViolationException.class)
    public void testNullPassword() throws Exception {
        pwChangeAction.onPasswordChange(user, null, createRoot(null), namePathMapper);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testSamePassword() throws Exception {
        pwChangeAction.onPasswordChange(user, "pw", createRoot("pw"), namePathMapper);
    }

    @Test
    public void testPasswordChange() throws Exception {
        pwChangeAction.onPasswordChange(user, "changedPassword", createRoot("pw"), namePathMapper);
    }

    @Test
    public void testUserWithoutPassword() throws Exception {
        try {
            pwChangeAction.onPasswordChange(user, "changedPassword", createRoot(null), namePathMapper);
        } finally {
            user.remove();
        }
    }
}