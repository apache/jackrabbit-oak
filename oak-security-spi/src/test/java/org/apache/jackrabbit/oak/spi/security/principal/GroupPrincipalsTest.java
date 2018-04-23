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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Enumeration;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class GroupPrincipalsTest {

    @Test
    public void testIsGroup() {
        Principal p0 = new PrincipalImpl("test");
        assertFalse(GroupPrincipals.isGroup(p0));

        Group g = new Group() {

            @Override
            public String getName() {
                return "testG";
            }

            @Override
            public boolean removeMember(Principal user) {
                return false;
            }

            @Override
            public Enumeration<? extends Principal> members() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isMember(Principal member) {
                return false;
            }

            @Override
            public boolean addMember(Principal user) {
                return false;
            }
        };

        assertTrue(GroupPrincipals.isGroup(g));
        assertTrue(GroupPrincipals.isGroup(new GroupPrincipalWrapper(g)));
    }

    @Test
    public void testTransform() {
        Group g = new Group() {

            @Override
            public String getName() {
                return "testG";
            }

            @Override
            public boolean removeMember(Principal user) {
                return false;
            }

            @Override
            public Enumeration<? extends Principal> members() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isMember(Principal member) {
                return false;
            }

            @Override
            public boolean addMember(Principal user) {
                return false;
            }
        };

        Set<Principal> t = GroupPrincipals.transform(ImmutableSet.of(g));
        assertEquals(1, t.size());
        Principal p = t.iterator().next();
        assertEquals(p.getName(), g.getName());

        Enumeration<? extends Principal> e = GroupPrincipals
                .transform(Iterators.asEnumeration(ImmutableSet.of(g).iterator()));
        Set<Principal> t2 = Sets.newHashSet(Iterators.forEnumeration(e));
        assertEquals(1, t2.size());
        Principal p2 = t2.iterator().next();
        assertEquals(p2.getName(), g.getName());
    }
}
