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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;

import javax.jcr.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class CompositeDataStoreTestUtils {
    static List<String> twoRoles = Lists.newArrayList("role1", "role2");
    static List<String> threeRoles = Lists.newArrayList("role1", "role2", "role3");

    static DataStoreProvider createDataStoreProvider(final String role) {
        return createDataStoreProvider(new OakFileDataStore(), role);
    }

    static DataStoreProvider createDataStoreProvider(final DataStore ds, final String role) {
        return new DataStoreProvider() {
            @Override
            public DataStore getDataStore() {
                return ds;
            }

            @Override
            public String getRole() {
                return role;
            }
        };
    }

    static DataRecord addTestRecord(final DataStore ds) throws RepositoryException {
        return addTestRecord(ds, null, "testrecord");
    }

    static DataRecord addTestRecord(final DataStore ds, final DelegateHandler delegateHandler) throws RepositoryException {
        return addTestRecord(ds, delegateHandler, "testrecord");
    }

    static DataRecord addTestRecord(final DataStore ds, final DelegateHandler delegateHandler, final String contents) throws RepositoryException {
        DataRecord record = ds.addRecord(new ByteArrayInputStream(contents.getBytes()));
        return record;
    }

    static List<DelegateDataStore> createDelegates(List<String> roles) {
        List<DelegateDataStore> delegates = Lists.newArrayList();
        for (String role : roles) {
            delegates.add(createDelegate(role));
        }
        return delegates;
    }

    static DelegateDataStore createDelegate(String role) {
        return createDelegate(
                createDataStoreProvider(role),
                Maps.newHashMap()
        );
    }

    static DelegateDataStore createDelegate(String role, DataStore ds) {
        return createDelegate(
                createDataStoreProvider(ds, role),
                Maps.newHashMap()
        );
    }

    static DelegateDataStore createDelegate(DataStoreProvider dsp) {
        return createDelegate(dsp, Maps.newHashMap());
    }

    static DelegateDataStore createDelegate(DataStoreProvider dsp, Map<String, Object> cfg) {
        return DelegateDataStore.builder(dsp)
                .withConfig(cfg)
                .build();
    }

    static DelegateDataStore createReadOnlyDelegate(String role) {
        return createReadOnlyDelegate(role, new OakFileDataStore());
    }

    static DelegateDataStore createReadOnlyDelegate(String role, DataStore ds) {
        Map<String, Object> cfg = Maps.newHashMap();
        cfg.put("readOnly", true);
        return createDelegate(
                createDataStoreProvider(ds, role),
                cfg
        );
    }

    static CompositeDataStore createEmptyCompositeDataStore(List<String> roles) {
        Map<String, Object> config = Maps.newHashMap();

        config.put(CompositeDataStore.ROLES, Joiner.on(",").join(roles));

        Properties properties = new Properties();
        properties.putAll(config);
        CompositeDataStore cds = new CompositeDataStore(properties);
        return cds;
    }

    static CompositeDataStore createCompositeDataStore(List<String> roles, String homedir) {
        List<DelegateDataStore> delegates = Lists.newArrayList();

        for (String role : roles) {
            delegates.add(createDelegate(role));
        }

        CompositeDataStore cds = createEmptyCompositeDataStore(roles);
        for (DelegateDataStore ds : delegates) {
            cds.addDelegate(ds);
        }

        if (null != homedir) {
            try {
                cds.init(homedir);
            }
            catch (RepositoryException e) {
                fail(e.getMessage());
            }
        }

        return cds;
    }

    static DataStore getFileDataStore(File dsPath) throws RepositoryException {
        DataStore ds = new OakFileDataStore();
        ds.init(dsPath.getAbsolutePath());
        return ds;
    }

    static DataStore createSpyDelegate(File homedir, String role, CompositeDataStore cds)
            throws RepositoryException {
        DataStore ds = spy(getFileDataStore(homedir));
        cds.addDelegate(createDelegate(role, ds));
        return ds;
    }

    static DataStore createReadOnlySpyDelegate(File homedir, String role, CompositeDataStore cds)
            throws RepositoryException, IOException {
        DataStore ds = spy(getFileDataStore(homedir));
        cds.addDelegate(createReadOnlyDelegate(role, ds));
        return ds;
    }

    static DataStore createSharedDataStoreSpyDelegate(File homedir, String role, CompositeDataStore cds)
            throws RepositoryException {
        DataStore ds = spy(new CompositeDataStoreTestUtils.TestableFileDataStore());
        ds.init(homedir.getAbsolutePath());
        cds.addDelegate(createDelegate(role, ds));
        return ds;
    }

    static DataStore createReadOnlySharedDataStoreSpyDelegate(File homedir, String role, CompositeDataStore cds)
            throws RepositoryException {
        DataStore ds = spy(new CompositeDataStoreTestUtils.TestableFileDataStore());
        ds.init(homedir.getAbsolutePath());
        cds.addDelegate(createReadOnlyDelegate(role, ds));
        return ds;
    }

    static String extractRecordData(DataRecord record) throws DataStoreException, IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(record.getStream(), writer, "utf-8");
        return writer.toString();
    }

    static void verifyRecord(DataRecord record, String expectedContent) {
        verifyRecord(record, expectedContent, true);
    }

    static void verifyRecord(DataRecord record, String expectedContent, boolean fullMatch) {
        assertNotNull(record);
        try {
            String recordData = extractRecordData(record);
            if (fullMatch) {
                assertEquals(expectedContent, recordData);
            }
            else {
                // Assume startswith match
                assertTrue(recordData.startsWith(expectedContent));
            }
        }
        catch (DataStoreException | IOException e) {
            fail(e.getMessage());
        }
    }

    static int verifyRecordCount(CompositeDataStore cds) throws DataStoreException {
        return verifyRecords(cds, null);
    }

    static int verifyRecords(CompositeDataStore cds, Collection<DataRecord> records)
            throws DataStoreException {
        int ctr = 0;
        Iterator<DataRecord> iter = cds.getAllRecords();
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        while (iter.hasNext()) {
            DataRecord record = iter.next();
            assertNotNull(record);
            if (null != records) {
                assertTrue(records.contains(record));
            }
            ++ctr;
        }
        return ctr;
    }

    static int verifyRecordIdCount(CompositeDataStore cds) throws DataStoreException {
        return verifyRecordIds(cds, null);
    }

    static int verifyRecordIds(CompositeDataStore cds, Collection<DataIdentifier> ids)
            throws DataStoreException {
        int ctr = 0;
        Iterator<DataIdentifier> iter = cds.getAllIdentifiers();
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        while (iter.hasNext()) {
            DataIdentifier id = iter.next();
            assertNotNull(id);
            if (null != ids) {
                assertTrue(ids.contains(id));
            }
            ++ctr;
        }
        return ctr;
    }

    static ByteArrayInputStream randomDataRecordStream() {
        return randomDataRecordStream(64);
    }

    static ByteArrayInputStream randomDataRecordStream(int length) {
        if (length <= 0) {
            throw new IllegalArgumentException();
        }
        final String symbols =
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringWriter writer = new StringWriter();
        Random random = new Random();
        for (int i=0; i<length; i++) {
            writer.append(symbols.charAt(random.nextInt(symbols.length())));
        }
        return new ByteArrayInputStream(writer.toString().getBytes());
    }

    static class TestableFileDataStore extends OakFileDataStore {
        Map<String, DataRecord> metastore = Maps.newConcurrentMap();
        Map<DataIdentifier, DataRecord> recordsById = Maps.newConcurrentMap();

        private DataRecord recordFromString(String s) {
            DataRecord r = mock(DataRecord.class);
            if (null != s) {
                try {
                    DataIdentifier id = new DataIdentifier(s);
                    when(r.getStream()).thenReturn(new ByteArrayInputStream(s.getBytes()));
                    when(r.getIdentifier()).thenReturn(new DataIdentifier(s));
                    recordsById.put(id, r);
                } catch (DataStoreException e) {
                }
            }
            return r;
        }

//        @Override
//        public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
//            StringWriter writer = new StringWriter();
//            try {
//                IOUtils.copy(stream, writer, "utf-8");
//                metastore.put(name, recordFromString(writer.toString()));
//            }
//            catch (IOException e) {
//                throw new DataStoreException(e);
//            }
//        }
//
//        @Override
//        public void addMetadataRecord(File f, String name) throws DataStoreException {
//            try {
//                addMetadataRecord(new FileInputStream(f), name);
//            }
//            catch (FileNotFoundException e) {
//                throw new DataStoreException(e);
//            }
//        }
//
//        @Override
//        public DataRecord getMetadataRecord(String name) {
//            return metastore.get(name);
//        }
//
//        @Override
//        public List<DataRecord> getAllMetadataRecords(String prefix) {
//            List<DataRecord> records = Lists.newArrayList();
//            for (String key : metastore.keySet()) {
//                if (key.startsWith(prefix)) {
//                    records.add(getMetadataRecord(key));
//                }
//            }
//            return records;
//        }
//
//        @Override
//        public boolean deleteMetadataRecord(String name) {
//            return null != metastore.remove(name);
//        }
//
//        @Override
//        public void deleteAllMetadataRecords(String prefix) {
//            for (String key : metastore.keySet()) {
//                if (key.startsWith(prefix)) {
//                    deleteMetadataRecord(key);
//                }
//            }
//        }
//
//        @Override
//        public Iterator<DataRecord> getAllRecords() throws DataStoreException {
//            Iterator<DataIdentifier> iter = getAllIdentifiers();
//            List<DataRecord> records = Lists.newArrayList();
//            while (iter.hasNext()) {
//                DataRecord record = getRecord(iter.next());
//                if (null != record) {
//                    records.add(record);
//                }
//            }
//            return records.iterator();
//        }
//
//        @Override
//        public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
//            try {
//                return getRecord(id);
//            }
//            catch (DataStoreException e) {
//                return null;
//            }
//        }

        public String getReferenceFromIdentifier(DataIdentifier id) {
            return super.getReferenceFromIdentifier(id);
        }
//
//        @Override
//        public Type getType() {
//            return Type.SHARED;
//        }
    }
}
