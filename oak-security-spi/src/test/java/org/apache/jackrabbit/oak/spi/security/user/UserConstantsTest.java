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
package org.apache.jackrabbit.oak.spi.security.user;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UserConstantsTest implements UserConstants {

    @Test
    public void testGroupPropertyNames() {
        assertEquals(ImmutableSet.of(REP_PRINCIPAL_NAME,
                REP_AUTHORIZABLE_ID,
                REP_MEMBERS), GROUP_PROPERTY_NAMES);
    }

    @Test
    public void testUserPropertyNames() {
        assertEquals(ImmutableSet.of(REP_PRINCIPAL_NAME,
                    REP_AUTHORIZABLE_ID,
                    REP_PASSWORD,
                    REP_DISABLED,
                    REP_IMPERSONATORS), UserConstants.USER_PROPERTY_NAMES);
    }

    @Test
    public void testPasswordPropertyNames() {
        assertEquals(ImmutableSet.of(REP_PASSWORD_LAST_MODIFIED), PWD_PROPERTY_NAMES);
    }

    @Test
    public void testNtNames() {
        assertEquals(ImmutableSet.of(NT_REP_USER, NT_REP_GROUP, NT_REP_PASSWORD,
                    NT_REP_MEMBERS, NT_REP_MEMBER_REFERENCES, NT_REP_MEMBER_REFERENCES_LIST), NT_NAMES);
    }
}