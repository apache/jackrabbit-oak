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

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WhiteboardAuthorizableNodeNameTest {

    private static final String TEST_ID = "testID";

    private final Whiteboard whiteboard = new DefaultWhiteboard();
    private final WhiteboardAuthorizableNodeName authorizableNodeName = new WhiteboardAuthorizableNodeName();

    @After
    public void after() {
        authorizableNodeName.stop();
    }

    @Test
    public void testDefault() {
        assertEquals(AuthorizableNodeName.DEFAULT.generateNodeName(TEST_ID), authorizableNodeName.generateNodeName(TEST_ID));
    }

    @Test
    public void testStarted() {
        authorizableNodeName.start(whiteboard);
        assertEquals(AuthorizableNodeName.DEFAULT.generateNodeName(TEST_ID), authorizableNodeName.generateNodeName(TEST_ID));
    }

    @Test
    public void testRegisteredImplementation() {
        authorizableNodeName.start(whiteboard);

        AuthorizableNodeName registered = new AuthorizableNodeName() {
            @Nonnull
            @Override
            public String generateNodeName(@Nonnull String authorizableId) {
                return "generated";
            }
        };
        whiteboard.register(AuthorizableNodeName.class, registered, ImmutableMap.of());
        assertEquals(registered.generateNodeName(TEST_ID), authorizableNodeName.generateNodeName(TEST_ID));
    }
}