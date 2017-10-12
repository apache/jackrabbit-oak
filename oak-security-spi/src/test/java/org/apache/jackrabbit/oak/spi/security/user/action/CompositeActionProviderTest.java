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

import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompositeActionProviderTest {

    private final SecurityProvider securityProvider = Mockito.mock(SecurityProvider.class);

    @Test
    public void testEmpty() {
        CompositeActionProvider cap = new CompositeActionProvider();
        assertTrue(cap.getAuthorizableActions(securityProvider).isEmpty());
    }

    @Test
    public void testSingle() {
        AuthorizableActionProvider aap = new TestAuthorizableActionProvider();
        assertEquals(aap.getAuthorizableActions(securityProvider), new CompositeActionProvider(aap).getAuthorizableActions(securityProvider));
    }

    @Test
    public void testMultiple() {
        AuthorizableActionProvider aap = new TestAuthorizableActionProvider();
        AuthorizableActionProvider aap2 = new TestAuthorizableActionProvider();

        assertEquals(ImmutableList.of(TestAction.INSTANCE, TestAction.INSTANCE), new CompositeActionProvider(aap, aap2).getAuthorizableActions(securityProvider));
    }

    @Test
    public void testMultiple2() {
        AuthorizableActionProvider aap = new TestAuthorizableActionProvider();
        AuthorizableActionProvider aap2 = new TestAuthorizableActionProvider();

        assertEquals(ImmutableList.of(TestAction.INSTANCE, TestAction.INSTANCE), new CompositeActionProvider(ImmutableList.of(aap, aap2)).getAuthorizableActions(securityProvider));
    }


    private final class TestAuthorizableActionProvider implements AuthorizableActionProvider {

        @Nonnull
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
            return ImmutableList.of(TestAction.INSTANCE);
        }
    }

    private static final class TestAction extends AbstractAuthorizableAction {

        private static final AuthorizableAction INSTANCE = new TestAction();
    }
}