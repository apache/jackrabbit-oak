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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CugExcludeDefaultTest {

    CugExclude exclude;

    @Before
    public void before() {
        exclude = createInstance();
    }

    CugExclude createInstance() {
        return new CugExclude.Default();
    }

    @Test
    public void testEmptyPrincipalSet() {
        assertTrue(exclude.isExcluded(ImmutableSet.<Principal>of()));
    }

    @Test
    public void testSystemPrincipal() {
        Set<Principal> principals = ImmutableSet.<Principal>of(SystemPrincipal.INSTANCE);
        assertTrue(exclude.isExcluded(principals));
    }

    @Test
    public void testAdminPrincipal() {
        Set<Principal> principals = ImmutableSet.<Principal>of(new AdminPrincipal() {
            @Override
            public String getName() {
                return "admin";
            }
        });
        assertTrue(exclude.isExcluded(principals));
    }

    @Test
    public void testSystemUserPrincipal() {
        Set<Principal> principals = ImmutableSet.<Principal>of(new SystemUserPrincipal() {
            @Override
            public String getName() {
                return "test";
            }
        });
        assertTrue(exclude.isExcluded(principals));
    }

    @Test
    public void testPrincipals() {
        Set<Principal> principals = new HashSet<Principal>();
        principals.add(new PrincipalImpl("test"));
        principals.add(new ItemBasedPrincipal() {
            @Override
            public String getPath() {
                return "/path";
            }

            @Override
            public String getName() {
                return "test";
            }
        });

        assertFalse(exclude.isExcluded(principals));
        for (Principal p : principals) {
            assertFalse(exclude.isExcluded(ImmutableSet.of(p)));
        }
    }

    @Test
    public void testMixedPrincipals() {
        Set<Principal> principals = new HashSet<Principal>();
        principals.add(new PrincipalImpl("test"));
        principals.add(new SystemUserPrincipal() {
            @Override
            public String getName() {
                return "test";
            }
        });

        assertTrue(exclude.isExcluded(principals));
    }
}
