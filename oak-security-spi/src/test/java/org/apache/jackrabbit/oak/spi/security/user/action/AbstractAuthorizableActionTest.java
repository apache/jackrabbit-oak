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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class AbstractAuthorizableActionTest {

    private final AuthorizableAction action = new AbstractAuthorizableAction() {};

    private final Root root = mock(Root.class);
    private final NamePathMapper namePathMapper = mock(NamePathMapper.class);

    @Test
    public void testInit() {
        SecurityProvider securityProvider = mock(SecurityProvider.class);
        action.init(securityProvider, ConfigurationParameters.EMPTY);
        verifyNoInteractions(securityProvider);
    }

    @Test
    public void testOnCreateGroup() throws Exception {
        Group gr = mock(Group.class);
        action.onCreate(gr, root, namePathMapper);
        verifyNoInteractions(gr, root, namePathMapper);
    }

    @Test
    public void testOnCreateUser() throws Exception {
        User user = mock(User.class);
        action.onCreate(user, null, root, namePathMapper);
        verifyNoInteractions(user, root, namePathMapper);
    }

    @Test
    public void testOnCreateSystemUser() throws Exception {
        User user = when(mock(User.class).isSystemUser()).thenReturn(true).getMock();
        action.onCreate(user, root, namePathMapper);
        verifyNoInteractions(user, root, namePathMapper);
    }

    @Test
    public void testOnRemove() throws Exception {
        Authorizable authorizable = mock(Authorizable.class);
        action.onRemove(authorizable, root, namePathMapper);
        verifyNoInteractions(authorizable, root, namePathMapper);
    }

    @Test
    public void testOnPasswordChange() throws Exception {
        User user = mock(User.class);
        action.onPasswordChange(user, null, root, namePathMapper);
        verifyNoInteractions(user, root, namePathMapper);
    }
}