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

import java.util.ArrayList;
import java.util.List;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;

public class PasswordValidationActionTest {

    private final SecurityProvider securityProvider = Mockito.mock(SecurityProvider.class);
    private final Root root = Mockito.mock(Root.class);
    private final NamePathMapper namePathMapper = Mockito.mock(NamePathMapper.class);
    private final PasswordValidationAction pwAction = new PasswordValidationAction();

    private User user;

    @Before
    public void before() throws Exception {
        user = Mockito.mock(User.class);
        pwAction.init(securityProvider, ConfigurationParameters.of(
                PasswordValidationAction.CONSTRAINT, "^.*(?=.{8,})(?=.*[a-z])(?=.*[A-Z]).*"));

    }

    @Test
    public void testOnCreateNullPw() throws Exception {
        pwAction.onCreate(user, null, root, namePathMapper);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testOnCreateInvalidPw() throws Exception {
        pwAction.onCreate(user, "pw", root, namePathMapper);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testOnCreateEmptyPw() throws Exception {
        pwAction.onCreate(user, "", root, namePathMapper);
    }

    @Test
    public void testOnCreateValidPw() throws Exception {
        pwAction.onCreate(user, "abCDefGH", root, namePathMapper);
    }

    @Test
    public void testPasswordValidationActionInvalid() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add("pw1");
        invalid.add("only6C");
        invalid.add("12345678");
        invalid.add("WITHOUTLOWERCASE");
        invalid.add("withoutuppercase");

        for (String pw : invalid) {
            try {
                pwAction.onPasswordChange(user, pw, root, namePathMapper);
                fail("should throw constraint violation");
            } catch (ConstraintViolationException e) {
                // success
            }
        }
    }

    @Test
    public void testPasswordValidationActionValid() throws Exception {
        List<String> valid = new ArrayList();
        valid.add("abCDefGH");
        valid.add("Abbbbbbbbbbbb");
        valid.add("cDDDDDDDDDDDDDDDDD");
        valid.add("gH%%%%%%%%%%%%%%%%^^");
        valid.add("&)(*&^%23qW");

        for (String pw : valid) {
            pwAction.onPasswordChange(user, pw, root, namePathMapper);
        }
    }

    @Test(expected = ConstraintViolationException.class)
    public void testPasswordValidationActionOnChange() throws Exception {
        pwAction.init(securityProvider, ConfigurationParameters.of(PasswordValidationAction.CONSTRAINT, "abc"));

        String hashed = PasswordUtil.buildPasswordHash("abc");
        pwAction.onPasswordChange(user, hashed, Mockito.mock(Root.class), Mockito.mock(NamePathMapper.class));
    }
}
