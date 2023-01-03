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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class EmptyAutoMembershipConfigTest {

    @Test
    public void testGetName() {
        assertTrue(AutoMembershipConfig.EMPTY.getName().isEmpty());
    }
    
    @Test
    public void testGetAutoMembership() {
        Authorizable authorizable = mock(Authorizable.class);
        assertTrue(AutoMembershipConfig.EMPTY.getAutoMembership(authorizable).isEmpty());
        verifyNoInteractions(authorizable);
    }
    
    @Test
    public void testGetAutoMembers() {
        UserManager userManager = mock(UserManager.class);
        Group group = mock(Group.class);
        assertFalse(AutoMembershipConfig.EMPTY.getAutoMembers(userManager, group).hasNext());
        verifyNoInteractions(userManager, group);
    }
}