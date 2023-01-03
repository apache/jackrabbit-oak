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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.junit.Test;

import javax.jcr.RepositoryException;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class EmptyDynamicMembershipProviderTest {

    private final Group group = mock(Group.class);
    private final Authorizable authorizable = mock(Authorizable.class);
    
    @Test
    public void testCoversAllMembers() {
        assertFalse(DynamicMembershipProvider.EMPTY.coversAllMembers(group));
        verifyNoInteractions(group);
    }

    @Test
    public void testGetMembers() throws RepositoryException {
        assertFalse(DynamicMembershipProvider.EMPTY.getMembers(group, true).hasNext());
        assertFalse(DynamicMembershipProvider.EMPTY.getMembers(group, false).hasNext());
        verifyNoInteractions(group);
    }

    @Test
    public void testIsMember() throws RepositoryException {
        assertFalse(DynamicMembershipProvider.EMPTY.isMember(group, authorizable, true));
        assertFalse(DynamicMembershipProvider.EMPTY.isMember(group, authorizable,false));
        verifyNoInteractions(group, authorizable);
    }

    @Test
    public void testGetMembership() throws RepositoryException {
        assertFalse(DynamicMembershipProvider.EMPTY.getMembership(authorizable, true).hasNext());
        assertFalse(DynamicMembershipProvider.EMPTY.getMembership(authorizable,false).hasNext());
        verifyNoInteractions(authorizable);
    }
}