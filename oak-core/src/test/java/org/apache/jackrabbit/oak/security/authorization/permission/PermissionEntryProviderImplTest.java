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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class PermissionEntryProviderImplTest {

    private final String GROUP_LONG_MAX = "groupLongMax";
    private final String GROUP_LONG_MAX_MINUS_10 = "groupLongMaxMinus10";
    private final String GROUP_50 = "group50";

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2465">OAK-2465</a>
     */
    @Test
    public void testInitLongOverflow() throws Exception {
        MockPermissionStore store = new MockPermissionStore();
        Set<String> principalNames = ImmutableSet.of(GROUP_LONG_MAX);

        /*
        create a new PermissionEntryProviderImpl to have it's #init() method
        called, which may trigger the cache to be pre-filled if the max number
        if entries is not exceeded -> in case of PermissionStore#getNumEntries
        return Long.MAX_VALUE the cache should not be filled (-> the mock-cache
        implementation will fail.
        */
        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, principalNames, ConfigurationParameters.EMPTY);

        // test that PermissionEntryProviderImpl.noExistingNames nevertheless is
        // properly set
        assertFalse(getNoExistingNames(provider));
        assertNotSame(Collections.emptyIterator(), provider.getEntryIterator(EntryPredicate.create()));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2465">OAK-2465</a>
     */
    @Test
    public void testInitLongOverflow2() throws Exception {
        MockPermissionStore store = new MockPermissionStore();
        Set<String> principalNames = ImmutableSet.of(GROUP_LONG_MAX_MINUS_10, GROUP_50);

        /*
        create a new PermissionEntryProviderImpl to have it's #init() method
        called, which may trigger the cache to be pre-filled if the max number
        if entries is not exceeded -> still counting up the number of permission
        entries must deal with the fact that the counter may become bigger that
        Long.MAX_VALUE
        */
        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, principalNames, ConfigurationParameters.EMPTY);
        assertFalse(getNoExistingNames(provider));

        assertNotSame(Collections.emptyIterator(), provider.getEntryIterator(EntryPredicate.create()));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2465">OAK-2465</a>
     */
    @Test
    public void testExistingNamesAndLongOverFlow() throws Exception {
        MockPermissionStore store = new MockPermissionStore();
        Set<String> principalNames = Sets.newHashSet(GROUP_LONG_MAX_MINUS_10, GROUP_50, "noEntries");

        /*
        same as before but principal-set contains a name for which not entries exist
        */
        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, principalNames, ConfigurationParameters.EMPTY);
        assertFalse(getNoExistingNames(provider));
    }

    @Test
    public void testNoExistingName() throws Exception {
        MockPermissionStore store = new MockPermissionStore();
        Set<String> principalNames = Sets.newHashSet("noEntries", "noEntries2", "noEntries3");

        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, principalNames, ConfigurationParameters.EMPTY);

        assertTrue(getNoExistingNames(provider));
    }

    /**
     * Use reflection to access the private "existingNames" field storing the
     * names of those principals associated with the entry provider for which
     * any permission entries exist.
     *
     * @param provider The permission entry provider
     * @return the existingNames set.
     * @throws Exception
     */
    private static boolean getNoExistingNames(@NotNull PermissionEntryProviderImpl provider) throws Exception {
        Field noExistingNamesField = provider.getClass().getDeclaredField("noExistingNames");
        noExistingNamesField.setAccessible(true);
        return (boolean) noExistingNamesField.get(provider);
    }

    // Inner Classes
    private class MockPermissionStore implements PermissionStore {

        @Nullable
        @Override
        public Collection<PermissionEntry> load(
                @NotNull String principalName,
                @NotNull String path) {
            return null;
        }

        @NotNull
        @Override
        public PrincipalPermissionEntries load(@NotNull String principalName) {
            return new PrincipalPermissionEntries();
        }

        @NotNull
        @Override
        public NumEntries getNumEntries(@NotNull String principalName, long max) {
            long cnt = 0;
            switch (principalName) {
                case GROUP_LONG_MAX_MINUS_10:
                    cnt = Long.MAX_VALUE - 10;
                    break;
                case GROUP_50:
                    cnt = 50;
                    break;
                case GROUP_LONG_MAX:
                    cnt = Long.MAX_VALUE;
                    break;
            }
            return NumEntries.valueOf(cnt, true);
        }

        public void flush(@NotNull Root root) {
        }
    }
}
