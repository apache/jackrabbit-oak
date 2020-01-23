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
package org.apache.jackrabbit.oak.spi.security.principal;

import org.junit.Before;
import org.junit.Test;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupPrincipalWrapperTest {

    private Group g;
    private GroupPrincipalWrapper wrapper;

    @Before
    public void before() {
        g = when(mock(Group.class).getName()).thenReturn("name").getMock();
        when(g.members()).thenReturn(Collections.emptyEnumeration());
        wrapper = new GroupPrincipalWrapper(g);
    }

    @Test
    public void testGetName() {
        assertEquals("name", wrapper.getName());
        verify(g, times(1)).getName();
    }

    @Test
    public void testIsMember() {
        Principal p = mock(Principal.class);
        wrapper.isMember(p);
        verify(g, times(1)).isMember(p);
    }

    @Test
    public void testMembers() {
        wrapper.members();
        verify(g, times(1)).members();
    }
}