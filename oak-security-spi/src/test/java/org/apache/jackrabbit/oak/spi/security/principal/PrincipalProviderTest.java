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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class PrincipalProviderTest {

    private final PrincipalProvider pp = new PrincipalProvider() {
        @Override
        public @Nullable Principal getPrincipal(@NotNull String principalName) {
            throw new RuntimeException();
        }

        @Override
        public @NotNull Set<? extends Principal> getPrincipals(@NotNull String userID) {
            throw new RuntimeException();
        }

        @Override
        public @NotNull Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
            throw new RuntimeException();
        }

        @Override
        public @NotNull Iterator<? extends Principal> findPrincipals(int searchType) {
            throw new RuntimeException();
        }
    };

    @Test
    public void testGetItemBasedPrincipal() {
        assertNull(pp.getItemBasedPrincipal("/some/path"));
    }

    @Test
    public void testGetMembershipPrincipals() {
        assertTrue(pp.getMembershipPrincipals(mock(Principal.class)).isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() {
        pp.findPrincipals("hint", true, PrincipalManager.SEARCH_TYPE_GROUP, -1, 12);
    }
}
