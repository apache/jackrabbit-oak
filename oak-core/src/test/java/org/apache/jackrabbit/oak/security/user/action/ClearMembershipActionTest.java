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
package org.apache.jackrabbit.oak.security.user.action;

import java.util.UUID;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.action.ClearMembershipAction;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link ClearMembershipAction} including a complete
 * security setup.
 *
 * @see {@link org.apache.jackrabbit.oak.spi.security.user.action.ClearMembershipActionTest}
 */
public class ClearMembershipActionTest extends AbstractSecurityTest {

    private ClearMembershipAction action;

    private Group gr;

    @Override
    public void before() throws Exception {
        super.before();

        gr = getUserManager(root).createGroup("gr" + UUID.randomUUID().toString());
        root.commit();

        action = new ClearMembershipAction();
        action.init(getSecurityProvider(), ConfigurationParameters.EMPTY);
    }

    @Override
    public void after() throws Exception {
        try {
            if (gr != null) {
                gr.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testOnCreateUser() throws Exception {
        action.onCreate(getTestUser(), "pw", root, NamePathMapper.DEFAULT);
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testOnCreateGroup() throws Exception {
        action.onCreate(gr, root, NamePathMapper.DEFAULT);
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testOnPwChange() throws Exception {
        action.onPasswordChange(getTestUser(), "newPw", root, NamePathMapper.DEFAULT);
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testOnRemoveUserNoMembership() throws Exception {
        action.onRemove(getTestUser(), root, NamePathMapper.DEFAULT);
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testOnRemoveGroupNoMembership() throws Exception {
        action.onRemove(gr, root, NamePathMapper.DEFAULT);
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testOnRemoveUserWithMembership() throws Exception {
        User u = getTestUser();
        gr.addMember(u);
        root.commit();

        action.onRemove(u, root, NamePathMapper.DEFAULT);
        assertTrue(root.hasPendingChanges());
        assertFalse(gr.isDeclaredMember(u));
    }
}