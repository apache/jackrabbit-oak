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
package org.apache.jackrabbit.oak.security.user.whiteboard;

import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.AbstractAuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WhiteboardAuthorizableActionProviderTest {

    private final Whiteboard whiteboard = new DefaultWhiteboard();
    private final WhiteboardAuthorizableActionProvider actionProvider = new WhiteboardAuthorizableActionProvider();

    @After
    public void after() {
        actionProvider.stop();
    }

    @Test
    public void testDefault() {
        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(Mockito.mock(SecurityProvider.class));
        assertNotNull(actions);
        assertTrue(actions.isEmpty());
    }

    @Test
    public void testStarted() {
        actionProvider.start(whiteboard);

        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(Mockito.mock(SecurityProvider.class));
        assertNotNull(actions);
        assertTrue(actions.isEmpty());
    }

    @Test
    public void testRegisteredImplementation() {
        actionProvider.start(whiteboard);

        AuthorizableActionProvider registered = new AuthorizableActionProvider() {
            @Nonnull
            @Override
            public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
                return ImmutableList.of(new TestAction());
            }
        };
        whiteboard.register(AuthorizableActionProvider.class, registered, ImmutableMap.of());

        List<? extends AuthorizableAction> actions = actionProvider.getAuthorizableActions(Mockito.mock(SecurityProvider.class));
        assertNotNull(actions);
        assertEquals(1, actions.size());
        assertTrue(actions.get(0) instanceof TestAction);
    }

    private static final class TestAction extends AbstractAuthorizableAction {}

}