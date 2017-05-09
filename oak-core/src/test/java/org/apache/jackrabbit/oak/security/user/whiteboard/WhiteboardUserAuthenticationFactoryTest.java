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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardUserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class WhiteboardUserAuthenticationFactoryTest {

    private final Root root = Mockito.mock(Root.class);
    private final UserConfiguration userConfiguration = Mockito.mock(UserConfiguration.class);

    private WhiteboardUserAuthenticationFactory createFactory(@Nullable final UserAuthenticationFactory defaultFactory,
                                                              @Nonnull final String... userIds) {
        return new WhiteboardUserAuthenticationFactory(defaultFactory) {
            @Override
            protected List<UserAuthenticationFactory> getServices() {
                List<UserAuthenticationFactory> factories = new ArrayList<UserAuthenticationFactory>(userIds.length);
                for (String uid : userIds) {
                    factories.add(new TestUserAuthenticationFactory(uid));
                }
                return factories;
            }
        };
    }

    @Nonnull
    private UserConfiguration getUserConfiguration() {
        return userConfiguration;
    }

    @Test
    public void testSingleService() throws Exception {
        WhiteboardUserAuthenticationFactory factory = createFactory(null, "test");

        assertNotNull(factory.getAuthentication(getUserConfiguration(), root, "test"));
        assertNull(factory.getAuthentication(getUserConfiguration(), root, "another"));
    }

    @Test
    public void testMultipleService() throws Exception {
        WhiteboardUserAuthenticationFactory factory = createFactory(null, "test", "test2");

        assertNotNull(factory.getAuthentication(getUserConfiguration(), root, "test"));
        assertNotNull(factory.getAuthentication(getUserConfiguration(), root, "test2"));
        assertNull(factory.getAuthentication(getUserConfiguration(), root, "another"));
    }

    @Test
    public void testDefault() throws Exception {
        WhiteboardUserAuthenticationFactory factory = createFactory(new TestUserAuthenticationFactory("abc"));

        assertNotNull(factory.getAuthentication(getUserConfiguration(), root, "abc"));
        assertNull(factory.getAuthentication(getUserConfiguration(), root, "test"));
        assertNull(factory.getAuthentication(getUserConfiguration(), root, "test2"));
        assertNull(factory.getAuthentication(getUserConfiguration(), root, "another"));
    }

    @Test
    public void testMultipleServiceAndDefault() throws Exception {
        WhiteboardUserAuthenticationFactory factory = createFactory(new TestUserAuthenticationFactory("abc"), "test", "test2");

        assertNull(factory.getAuthentication(getUserConfiguration(), root, "abc"));

        assertNotNull(factory.getAuthentication(getUserConfiguration(), root, "test"));
        assertNotNull(factory.getAuthentication(getUserConfiguration(), root, "test2"));

        assertNull(factory.getAuthentication(getUserConfiguration(), root, "another"));
    }

    private static class TestUserAuthenticationFactory implements UserAuthenticationFactory {

        private final String userId;

        private TestUserAuthenticationFactory(@Nonnull String userId) {
            this.userId = userId;
        }

        @Override
        public Authentication getAuthentication(@Nonnull UserConfiguration configuration, @Nonnull Root root, @Nullable String userId) {
            if (this.userId.equals(userId)) {
                return Mockito.mock(Authentication.class);
            } else {
                return null;
            }
        }
    }
}
