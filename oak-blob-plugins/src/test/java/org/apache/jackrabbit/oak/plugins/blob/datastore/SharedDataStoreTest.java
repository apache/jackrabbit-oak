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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreTest.FixtureHelper.DATA_STORE;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreTest.FixtureHelper.DATA_STORE.CACHING_FDS;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreTest.FixtureHelper.DATA_STORE.FDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SharedDataStoreTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    private DATA_STORE type;

    @Parameterized.Parameters(name="{index}: ({0})")
    public static List<Object[]> fixtures() {
        return FixtureHelper.get();
    }

    static class FixtureHelper {
        enum DATA_STORE {
            CACHING_FDS, FDS
        }

        static List<Object[]> get() {
            return List.of(new Object[] {CACHING_FDS}, new Object[] {FDS});
        }
    }

    public SharedDataStoreTest(DATA_STORE type) {
        this.type = type;
    }

    @Before
    public void setup() throws Exception {
        if (type == CACHING_FDS) {
            CachingFileDataStore ds = new CachingFileDataStore();

            Properties props = new Properties();
            props.setProperty("fsBackendPath", folder.newFolder().getAbsolutePath());
            PropertiesUtil.populate(ds, Maps.fromProperties(props), false);
            ds.setProperties(props);
            ds.init(folder.newFolder().getAbsolutePath());
            dataStore = ds;
        } else {
            OakFileDataStore ds = new OakFileDataStore();
            ds.init(folder.newFolder().getAbsolutePath());
            dataStore = ds;
        }
    }

    protected SharedDataStore dataStore;

    @Test
    public void testGetAllIdentifiersRelative1() throws Exception {
        File f = new File("./target/oak-fds-test1");
        testGetAllIdentifiers(f.getAbsolutePath(), f.getPath());
    }

    @Test
    public void testGetAllIdentifiersRelative2() throws Exception {
        File f = new File("./target", "/fds/../oak-fds-test2");
        testGetAllIdentifiers(FilenameUtils.normalize(f.getAbsolutePath()), f.getPath());
    }

    @Test
    public void testGetAllIdentifiers() throws Exception {
        File f = new File("./target", "oak-fds-test3");
        testGetAllIdentifiers(f.getAbsolutePath(), f.getPath());
    }

    private void testGetAllIdentifiers(String path, String unnormalizedPath) throws Exception {
        File testDir = new File(path);
        FileUtils.touch(new File(testDir, "ab/cd/ef/abcdef"));
        FileUtils.touch(new File(testDir, "bc/de/fg/bcdefg"));
        FileUtils.touch(new File(testDir, "cd/ef/gh/cdefgh"));
        FileUtils.touch(new File(testDir, "c"));

        FileDataStore fds = new OakFileDataStore();
        fds.setPath(unnormalizedPath);
        fds.init(null);

        Iterator<DataIdentifier> dis = fds.getAllIdentifiers();
        Set<String> fileNames = CollectionUtils.toSet(Iterators.transform(dis, DataIdentifier::toString));

        Set<String> expectedNames = Set.of("abcdef","bcdefg","cdefgh");
        assertEquals(expectedNames, fileNames);
        FileUtils.cleanDirectory(testDir);
    }

    // AddMetadataRecord (Backend)

    @Test
    public void testBackendAddMetadataRecordsFromInputStream() throws Exception {
        SharedDataStore fds = dataStore;

        for (boolean fromInputStream : List.of(false, true)) {
            String prefix = String.format("%s.META.", getClass().getSimpleName());
            for (int count : List.of(1, 3)) {
                Map<String, String> records = Maps.newHashMap();
                for (int i = 0; i < count; i++) {
                    String recordName = String.format("%sname.%d", prefix, i);
                    String data = String.format("testData%d", i);
                    records.put(recordName, data);

                    if (fromInputStream) {
                        fds.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), recordName);
                    }
                    else {
                        File testFile = folder.newFile();
                        copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                        fds.addMetadataRecord(testFile, recordName);
                    }
                }

                assertEquals(count, fds.getAllMetadataRecords(prefix).size());

                for (Map.Entry<String, String> entry : records.entrySet()) {
                    DataRecord record = fds.getMetadataRecord(entry.getKey());
                    StringWriter writer = new StringWriter();
                    IOUtils.copy(record.getStream(), writer);
                    fds.deleteMetadataRecord(entry.getKey());
                    assertTrue(writer.toString().equals(entry.getValue()));
                }

                assertEquals(0, fds.getAllMetadataRecords(prefix).size());
            }
        }
    }

    @Test
    public void testBackendAddMetadataRecordFileNotFoundThrowsDataStoreException() throws IOException {
        SharedDataStore fds = dataStore;

        File testFile = folder.newFile();
        copyInputStreamToFile(randomStream(0, 10), testFile);
        testFile.delete();
        try {
            fds.addMetadataRecord(testFile, "name");
            fail();
        }
        catch (DataStoreException e) {
            assertTrue(e.getCause() instanceof FileNotFoundException);
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullInputStreamThrowsNullPointerException() throws DataStoreException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("input should not be null");

        SharedDataStore fds = dataStore;
        fds.addMetadataRecord((InputStream)null, "name");
    }

    @Test
    public void testBackendAddMetadataRecordNullFileThrowsNullPointerException() throws DataStoreException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("input should not be null");

        SharedDataStore fds = dataStore;
        fds.addMetadataRecord((File)null, "name");
    }

    @Test
    public void testBackendAddMetadataRecordNullEmptyNameThrowsIllegalArgumentException() throws DataStoreException, IOException {
        SharedDataStore fds = dataStore;

        final String data = "testData";
        for (boolean fromInputStream : List.of(false, true)) {
            for (String name : new ArrayList<>(Arrays.asList(null, ""))) {
                try {
                    if (fromInputStream) {
                        fds.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), name);
                    } else {
                        File testFile = folder.newFile();
                        copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                        fds.addMetadataRecord(testFile, name);
                    }
                    fail();
                } catch (IllegalArgumentException e) {
                    assertTrue("name should not be empty".equals(e.getMessage()));
                }
            }
        }
    }

    // GetMetadataRecord (Backend)

    @Test
    public void testBackendGetMetadataRecordInvalidName() throws DataStoreException {
        SharedDataStore fds = dataStore;

        fds.addMetadataRecord(randomStream(0, 10), "testRecord");
        assertNull(fds.getMetadataRecord("invalid"));
        for (String name : new ArrayList<>(Arrays.asList("", null))) {
            try {
                fds.getMetadataRecord(name);
                fail("Expect to throw");
            } catch(Exception e) {}
        }

        fds.deleteMetadataRecord("testRecord");
    }

    // GetAllMetadataRecords (Backend)

    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesAll() throws DataStoreException {
        SharedDataStore fds = dataStore;

        assertEquals(0, fds.getAllMetadataRecords("").size());

        String prefixAll = "prefix1";
        String prefixSome = "prefix1.prefix2";
        String prefixOne = "prefix1.prefix3";
        String prefixNone = "prefix4";

        fds.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
        fds.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
        fds.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
        fds.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));
        fds.addMetadataRecord(randomStream(5, 10), "prefix5.testRecord5");

        assertEquals(5, fds.getAllMetadataRecords("").size());
        assertEquals(4, fds.getAllMetadataRecords(prefixAll).size());
        assertEquals(2, fds.getAllMetadataRecords(prefixSome).size());
        assertEquals(1, fds.getAllMetadataRecords(prefixOne).size());
        assertEquals(0, fds.getAllMetadataRecords(prefixNone).size());

        fds.deleteAllMetadataRecords("");
        assertEquals(0, fds.getAllMetadataRecords("").size());
    }

    @Test
    public void testBackendGetAllMetadataRecordsNullPrefixThrowsNullPointerException() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("prefix should not be null");

        SharedDataStore fds = dataStore;
        fds.getAllMetadataRecords(null);
    }

    // DeleteMetadataRecord (Backend)

    @Test
    public void testBackendDeleteMetadataRecord() throws DataStoreException {
        SharedDataStore fds = dataStore;

        fds.addMetadataRecord(randomStream(0, 10), "name");
        for (String name : new ArrayList<>(Arrays.asList("", null))) {
            if (Strings.isNullOrEmpty(name)) {
                try {
                    fds.deleteMetadataRecord(name);
                }
                catch (IllegalArgumentException e) { }
            }
            else {
                fds.deleteMetadataRecord(name);
                fail();
            }
        }
        assertTrue(fds.deleteMetadataRecord("name"));
    }

    // DeleteAllMetadataRecords (Backend)

    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesAll() throws DataStoreException {
        SharedDataStore fds = dataStore;

        String prefixAll = "prefix1";
        String prefixSome = "prefix1.prefix2";
        String prefixOne = "prefix1.prefix3";
        String prefixNone = "prefix4";

        Map<String, Integer> prefixCounts = Maps.newHashMap();
        prefixCounts.put(prefixAll, 4);
        prefixCounts.put(prefixSome, 2);
        prefixCounts.put(prefixOne, 1);
        prefixCounts.put(prefixNone, 0);

        for (Map.Entry<String, Integer> entry : prefixCounts.entrySet()) {
            fds.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
            fds.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
            fds.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
            fds.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));

            int preCount = fds.getAllMetadataRecords("").size();

            fds.deleteAllMetadataRecords(entry.getKey());

            int deletedCount = preCount - fds.getAllMetadataRecords("").size();
            assertEquals(entry.getValue().intValue(), deletedCount);

            fds.deleteAllMetadataRecords("");
        }
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsNoRecordsNoChange() {
        SharedDataStore fds = dataStore;

        assertEquals(0, fds.getAllMetadataRecords("").size());

        fds.deleteAllMetadataRecords("");

        assertEquals(0, fds.getAllMetadataRecords("").size());
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsNullPrefixThrowsNullPointerException() {
        expectedEx.expect(IllegalArgumentException.class);

        SharedDataStore fds = dataStore;
        fds.deleteAllMetadataRecords(null);
    }

    // MetadataRecordExists (Backend)
    @Test
    public void testBackendMetadataRecordExists() throws DataStoreException {
        SharedDataStore fds = dataStore;

        fds.addMetadataRecord(randomStream(0, 10), "name");
        for (String name : new ArrayList<>(Arrays.asList("invalid", "", null))) {
            if (Strings.isNullOrEmpty(name)) {
                try {
                    fds.metadataRecordExists(name);
                }
                catch (IllegalArgumentException | NullPointerException e) { }
            }
            else {
                assertFalse(fds.metadataRecordExists(name));
            }
        }
        assertTrue(fds.metadataRecordExists("name"));
    }
}
