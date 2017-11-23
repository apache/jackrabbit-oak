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
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        PermissionEntryCache cache = new MockPermissionEntryCache();
        Set<String> principalNames = ImmutableSet.of(GROUP_LONG_MAX);

        /*
        create a new PermissionEntryProviderImpl to have it's #init() method
        called, which may trigger the cache to be pre-filled if the max number
        if entries is not exceeded -> in case of PermissionStore#getNumEntries
        return Long.MAX_VALUE the cache should not be filled (-> the mock-cache
        implementation will fail.
        */
        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, cache, principalNames, ConfigurationParameters.EMPTY);
        Field existingNamesField = provider.getClass().getDeclaredField("existingNames");
        existingNamesField.setAccessible(true);

        // test that PermissionEntryProviderImpl.existingNames nevertheless is
        // properly filled with all principal names for which permission entries exist
        assertEquals(principalNames, existingNamesField.get(provider));
        assertNotSame(Iterators.emptyIterator(), provider.getEntryIterator(new EntryPredicate()));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2465">OAK-2465</a>
     */
    @Test
    public void testInitLongOverflow2() throws Exception {
        MockPermissionStore store = new MockPermissionStore();
        PermissionEntryCache cache = new MockPermissionEntryCache();
        Set<String> principalNames = ImmutableSet.of(GROUP_LONG_MAX_MINUS_10, GROUP_50);

        /*
        create a new PermissionEntryProviderImpl to have it's #init() method
        called, which may trigger the cache to be pre-filled if the max number
        if entries is not exceeded -> still counting up the number of permission
        entries must deal with the fact that the counter may become bigger that
        Long.MAX_VALUE
        */
        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, cache, principalNames, ConfigurationParameters.EMPTY);
        Set<String> existingNames = getExistingNames(provider);

        assertEquals(principalNames, existingNames);
        assertNotSame(Iterators.emptyIterator(), provider.getEntryIterator(new EntryPredicate()));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2465">OAK-2465</a>
     */
    @Test
    public void testExistingNamesAndLongOverFlow() throws Exception {
        MockPermissionStore store = new MockPermissionStore();
        PermissionEntryCache cache = new MockPermissionEntryCache();
        Set<String> principalNames = Sets.newHashSet(GROUP_LONG_MAX_MINUS_10, GROUP_50, "noEntries");

        /*
        same as before but principal-set contains a name for which not entries
        exist -> the 'existingNames' set must properly reflect that
        */
        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, cache, principalNames, ConfigurationParameters.EMPTY);
        Set<String> existingNames = getExistingNames(provider);
        
        assertFalse(principalNames.equals(existingNames));
        assertEquals(2, existingNames.size());

        principalNames.remove("noEntries");
        assertEquals(principalNames, existingNames);
    }

    @Test
    public void testNoExistingName() throws Exception {
        MockPermissionStore store = new MockPermissionStore();
        PermissionEntryCache cache = new MockPermissionEntryCache();
        Set<String> principalNames = Sets.newHashSet("noEntries", "noEntries2", "noEntries3");

        PermissionEntryProviderImpl provider = new PermissionEntryProviderImpl(store, cache, principalNames, ConfigurationParameters.EMPTY);
        Set<String> existingNames = getExistingNames(provider);

        assertTrue(existingNames.isEmpty());

        Field pathMapField = provider.getClass().getDeclaredField("pathEntryMap");
        pathMapField.setAccessible(true);
        assertNull(pathMapField.get(provider));
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
    private static Set<String> getExistingNames(@Nonnull PermissionEntryProviderImpl provider) throws Exception {
        Field existingNamesField = provider.getClass().getDeclaredField("existingNames");
        existingNamesField.setAccessible(true);
        return (Set<String>) existingNamesField.get(provider);
    }

    // Inner Classes
    private class MockPermissionStore implements PermissionStore {

        @Override
        public Collection<PermissionEntry> load(
                Collection<PermissionEntry> entries, @Nonnull String principalName,
                @Nonnull String path) {
            return null;
        }

        @Nonnull
        @Override
        public PrincipalPermissionEntries load(@Nonnull String principalName) {
            return new PrincipalPermissionEntries();
        }

        @Override
        public long getNumEntries(@Nonnull String principalName, long max) {
            long cnt = 0;
            if (GROUP_LONG_MAX_MINUS_10.equals(principalName)) {
                cnt = Long.MAX_VALUE - 10;
            } else if (GROUP_50.equals(principalName)) {
                cnt = 50;
            } else if (GROUP_LONG_MAX.equals(principalName)) {
                cnt = Long.MAX_VALUE;
            }
            return cnt;
        }

        public void flush(@Nonnull Root root) {
        }

    }

    private class MockPermissionEntryCache extends PermissionEntryCache {
        @Override
        public void load(@Nonnull PermissionStore store,
                @Nonnull Map<String, Collection<PermissionEntry>> pathEntryMap,
                @Nonnull String principalName) {
            fail("The number of  entries exceeds the max cache size");
        }
    }
}
