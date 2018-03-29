/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createCompositeDataStore;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDataStoreProvider;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDelegate;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDelegates;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createEmptyCompositeDataStore;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.twoRoles;
import static org.junit.Assert.assertNotNull;

// Test the delegate management capabilities of the CompositeDataStore.
public class CompositeDataStoreDelegateManagementTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Set<DataStore> delegatesToDataStores(List<DelegateDataStore> delegates) {
        Set<DataStore> result = Sets.newHashSet();
        for (DelegateDataStore delegate : delegates) {
            result.add(delegate.getDataStore().getDataStore());
        }
        return result;
    }

    private void testAddDelegates(List<String> roles) {
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);

        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);
    }

    private void verifyDelegatesInCompositeDataStore(List<DelegateDataStore> delegates, CompositeDataStore cds) {
        Set<DataStore> expectedDataStores = delegatesToDataStores(delegates);
        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        int delegateCount = 0;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            assertNotNull(ds);
            assertTrue(expectedDataStores.contains(ds));
            delegateCount++;
        }
        assertEquals(delegates.size(), delegateCount);
    }

    @Test
    public void testAddZeroDelegates() {
        testAddDelegates(Lists.newArrayList());
    }

    @Test
    public void testAddOneDelegate() {
        testAddDelegates(Lists.newArrayList("local1"));
    }

    @Test
    public void testAddTwoDelegates() {
        testAddDelegates(Lists.newArrayList("local1", "local2"));
    }

    @Test
    public void testAddThreeDelegates() {
        testAddDelegates(Lists.newArrayList("local1", "local2", "local3"));
    }

    @Test
    public void testAddDelegateWithMissingRole() {
        List<DelegateDataStore> delegates = Lists.newArrayList(
                new MissingRoleCompositeDataStoreDelegate(
                        createDataStoreProvider("local1"),
                        Maps.newHashMap()
                ),
                new EmptyRoleCompositeDataStoreDelegate(
                        createDataStoreProvider("local2"),
                        Maps.newHashMap()
                )
        );


        Map<String, Object> config = Maps.newHashMap();
        config.put(CompositeDataStore.ROLES, "local1,local2");
        Properties properties = new Properties();
        properties.putAll(config);

        CompositeDataStore cds = new CompositeDataStore(properties);

        for (DelegateDataStore delegate : delegates) {
            assertFalse(cds.addDelegate(delegate));

            Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testAddDelegateWithNonMatchingRole() {
        DelegateDataStore delegate =
                DelegateDataStore.builder(createDataStoreProvider("otherRole"))
                .withConfig(Maps.newHashMap())
                .build();

        Map<String, Object> config = Maps.newHashMap();
        config.put(CompositeDataStore.ROLES, "thisRole");
        Properties properties = new Properties();
        properties.putAll(config);

        CompositeDataStore cds = new CompositeDataStore(properties);
        assertFalse(cds.addDelegate(delegate));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveDelegate() {
        List<String> roles = Lists.newArrayList("local1");
        DelegateDataStore delegate = createDelegate(roles.get(0));
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        assertTrue(cds.removeDelegate(delegate.getDataStore()));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveOneOfManyDelegates() {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        assertTrue(cds.removeDelegate(delegates.get(1).getDataStore()));
        delegates.remove(1);

        Set<DataStore> dsps = delegatesToDataStores(delegates);

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertTrue(iter.hasNext());
        int delegateCount = 0;
        while (iter.hasNext()) {
            delegateCount++;
            DataStore ds = iter.next();
            assertTrue(dsps.contains(ds));
        }
        assertEquals(dsps.size(), delegateCount);
    }

    @Test
    public void testRemoveAllDelegates() {
        List<String> roles = Lists.newArrayList("local1", "local2", "remote1");
        List<DelegateDataStore> delegates = createDelegates(roles);
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.addDelegate(delegate));
        }

        verifyDelegatesInCompositeDataStore(delegates, cds);

        for (DelegateDataStore delegate : delegates) {
            assertTrue(cds.removeDelegate(delegate.getDataStore()));
        }

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRemoveNonMatchingDelegate() {
        List<String> roles = Lists.newArrayList("local1");
        DelegateDataStore delegate = createDelegate(roles.get(0));
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        assertTrue(cds.addDelegate(delegate));

        verifyDelegatesInCompositeDataStore(Lists.newArrayList(delegate), cds);

        DataStoreProvider otherDsp = createDataStoreProvider("local1");
        assertFalse(cds.removeDelegate(otherDsp));

        Iterator<DataStore> iter = cds.delegateHandler.getAllDelegatesIterator();
        assertTrue(iter.hasNext());
    }

    @Test
    public void testCreatesDataStoreIdentifierForAllDelegates() throws IOException, RepositoryException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        String homedir = folder.newFolder().getAbsolutePath();
        cds.init(homedir);
        for (String role : twoRoles) {
            DelegateDataStore ds = createDelegate(role);
            ds.getDataStore().getDataStore().init(FilenameUtils.concat(homedir, role));
            cds.addDelegate(ds);
        }

        Set<String> dsids = Sets.newHashSet();
        for (DataRecord record : cds.getAllMetadataRecords(
                SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getType())) {
            dsids.add(SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.
                    getIdFromName(record.getIdentifier().toString()));
        }
        assertEquals(2, dsids.size());
    }

    @Test
    public void testCreateDSIDBeforeInitWorks() throws IOException, RepositoryException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, folder.newFolder().getAbsolutePath());

        Set<String> dsids = Sets.newHashSet();
        for (DataRecord record : cds.getAllMetadataRecords(
                SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getType())) {
            dsids.add(SharedDataStoreUtils.SharedStoreRecordType.
                    DATA_STORE_ID.getIdFromName(record.getIdentifier().toString()));
        }
        assertEquals(2, dsids.size());
    }

    @Test
    public void testUsesExistingDataStoreIdentifierIfExists() throws IOException, RepositoryException {
        String homedir = folder.newFolder().getAbsolutePath();
        Set<String> dsids = Sets.newHashSet();
        for (String role : twoRoles) {
            String rolePath = FilenameUtils.concat(homedir, role);
            String dsDir = FilenameUtils.concat(rolePath, "repository/datastore");
            new File(dsDir).mkdirs();
            String dsid = SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getNameFromId(UUID.randomUUID().toString());
            new File(FilenameUtils.concat(dsDir, dsid)).createNewFile();
            dsids.add(dsid);
        }

        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        cds.init(homedir);
        for (String role : twoRoles) {
            DelegateDataStore ds = createDelegate(role);
            ds.getDataStore().getDataStore().init(FilenameUtils.concat(homedir, role));
            cds.addDelegate(ds);
        }

        for (DataRecord record : cds.getAllMetadataRecords(
                SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getType())) {
            assertTrue(dsids.contains(record.getIdentifier().toString()));
        }
    }

    @Test
    public void testUseExistingDSIDBeforeInitWorks() throws IOException {
        String homedir = folder.newFolder().getAbsolutePath();
        Set<String> dsids = Sets.newHashSet();
        for (String role : twoRoles) {
            String rolePath = FilenameUtils.concat(homedir, "compositeds/"+role);
            String dsDir = FilenameUtils.concat(rolePath, "repository/datastore");
            new File(dsDir).mkdirs();
            String dsid = SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getNameFromId(UUID.randomUUID().toString());
            new File(FilenameUtils.concat(dsDir, dsid)).createNewFile();
            dsids.add(dsid);
        }

        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);

        for (DataRecord record : cds.getAllMetadataRecords(
                SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getType())) {
            assertTrue(dsids.contains(record.getIdentifier().toString()));
        }
    }

    @Test
    public void testManageDSIDsBeforeAndAfterInit() throws IOException, RepositoryException {
        String homedir = folder.newFolder().getAbsolutePath();
        Set<String> preExistingDsids = Sets.newHashSet();
        for (String role : Lists.newArrayList("role1", "role2")) {
            String dsDir = FilenameUtils.concat(homedir,
                    "compositeds/" + role + "/repository/datastore");
            new File(dsDir).mkdirs();
            String dsid = SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getNameFromId(UUID.randomUUID().toString());
            new File(FilenameUtils.concat(dsDir, dsid)).createNewFile();
            preExistingDsids.add(dsid);
        }

        List<String> fourRoles = Lists.newArrayList("role1", "role2", "role3", "role4");
        CompositeDataStore cds = createEmptyCompositeDataStore(fourRoles);

        cds.addDelegate(createDelegate("role1"));
        cds.addDelegate(createDelegate("role3"));

        cds.init(homedir);

        for (String role : Lists.newArrayList("role2", "role4")) {
            DelegateDataStore ds = createDelegate(role);
            ds.getDataStore().getDataStore().init(FilenameUtils.concat(homedir,
                    "compositeds/" + role));
            cds.addDelegate(ds);
        }

        int preExistingDsidsFound = 0;
        int dsidsFound = 0;
        for (DataRecord record : cds.getAllMetadataRecords(
                SharedDataStoreUtils.SharedStoreRecordType.DATA_STORE_ID.getType())) {
            if (preExistingDsids.contains(record.getIdentifier().toString())) {
                preExistingDsidsFound++;
            }
            dsidsFound++;
        }

        assertEquals(2, preExistingDsidsFound);
        assertEquals(4, dsidsFound);
    }

    static class MissingRoleCompositeDataStoreDelegate extends DelegateDataStore {
        MissingRoleCompositeDataStoreDelegate(DataStoreProvider dsp, Map<String, Object> config) {
            super(dsp, config);
        }

        @Override
        public String getRole() {
            return null;
        }
    }

    static class EmptyRoleCompositeDataStoreDelegate extends DelegateDataStore {
        EmptyRoleCompositeDataStoreDelegate(DataStoreProvider dsp, Map<String, Object> config) {
            super(dsp, config);
        }

        @Override
        public String getRole() {
            return "";
        }
    }
}