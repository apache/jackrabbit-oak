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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple tests for S3DataStore.
 */
@RunWith(Parameterized.class)
public class TestS3DataStore {
    protected static final Logger LOG = LoggerFactory.getLogger(TestS3DataStore.class);

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Properties props;

    @Parameterized.Parameter
    public String s3Class;

    private File dataStoreDir;

    private DataStore ds;
    private Date startTime;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return getFixtures();
    }

    private String bucket;

    @Before
    public void setUp() throws Exception {
        // Backdate by 1 minute, to allow for time drift when deleting
        // resources created by the test.
        startTime = DateUtils.addMinutes(new Date(), -1);

        dataStoreDir = folder.newFolder();
        props = new Properties();
    }

    @Test
    public void testAccessParamLeakOnError() throws Exception {
        expectedEx.expect(RepositoryException.class);
        expectedEx.expectMessage("Could not initialize S3 from {mode=S3, s3Region=us-standard, intValueKey=25}");

        props.put(S3Constants.ACCESS_KEY, "abcd");
        props.put(S3Constants.SECRET_KEY, "123456");
        props.put(S3Constants.S3_REGION, "us-standard");
        props.put("intValueKey", 25);
        ds = getS3DataStore(s3Class, props, dataStoreDir.getAbsolutePath());
    }

    @Test
    public void testSecret() throws Exception {
        assumeTrue(isS3Configured());

        S3DataStore s3 = getDataStore();
        // get ref as record
        DataRecord refRec = s3.getMetadataRecord("reference.key");
        assertNotNull("Reference data record null", refRec);
        byte[] refDirectFromBackend = IOUtils.toByteArray(refRec.getStream());
        LOG.warn("Ref direct from backend {}", refDirectFromBackend);
        
        // reference.key initialized in backend#init() - OAK-9807, so expected reference.key already created
        byte[] refKey = ((S3Backend) s3.getBackend()).getOrCreateReferenceKey();
        assertTrue("refKey in memory not equal to the metadata record",
            Arrays.equals(refKey, refDirectFromBackend));
        
        Random randomGen = new Random();
        byte[] data = new byte[4096];
        randomGen.nextBytes(data);
        DataRecord rec = s3.addRecord(new ByteArrayInputStream(data));
        assertEquals(data.length, rec.getLength());
        String ref = rec.getReference();
        assertNotNull(ref);

        String id = rec.getIdentifier().toString();
        
        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(new SecretKeySpec(refKey, "HmacSHA1"));
        byte[] hash = mac.doFinal(id.getBytes("UTF-8"));
        String calcRef = id + ':' + encodeHexString(hash);

        assertEquals("getReference() not equal", calcRef, ref);
    }

    @Test
    public void testAlternateBucketProp() throws Exception {
        assumeTrue(isS3Configured());

        Random randomGen = new Random();
        props = S3DataStoreUtils.getS3Config();
        //Replace bucket in props with container
        bucket = props.getProperty(S3Constants.S3_BUCKET);
        props.remove(S3Constants.S3_BUCKET);
        props.put(S3Constants.S3_CONTAINER, bucket);
        props.put("cacheSize", "0");

        ds = getS3DataStore(s3Class, props, dataStoreDir.getAbsolutePath());
        byte[] data = new byte[4096];
        randomGen.nextBytes(data);
        DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
        assertEquals(data.length, rec.getLength());
    }


    // AddMetadataRecord (Backend)

    @Test
    public void testBackendAddMetadataRecordsFromInputStream() throws Exception {
        assumeTrue(isS3Configured());
        S3DataStore s3ds = getDataStore();

        for (boolean fromInputStream : List.of(false, true)) {
            String prefix = String.format("%s.META.", getClass().getSimpleName());
            for (int count : List.of(1, 3)) {
                Map<String, String> records = new HashMap<>();
                for (int i = 0; i < count; i++) {
                    String recordName = String.format("%sname.%d", prefix, i);
                    String data = String.format("testData%d", i);
                    records.put(recordName, data);

                    if (fromInputStream) {
                        s3ds.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), recordName);
                    }
                    else {
                        File testFile = folder.newFile();
                        copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                        s3ds.addMetadataRecord(testFile, recordName);
                    }
                }

                assertEquals(count, s3ds.getAllMetadataRecords(prefix).size());

                for (Map.Entry<String, String> entry : records.entrySet()) {
                    DataRecord record = s3ds.getMetadataRecord(entry.getKey());
                    StringWriter writer = new StringWriter();
                    IOUtils.copy(record.getStream(), writer);
                    s3ds.deleteMetadataRecord(entry.getKey());
                    assertTrue(writer.toString().equals(entry.getValue()));
                }

                assertEquals(0, s3ds.getAllMetadataRecords(prefix).size());
            }
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullInputStreamThrowsNullPointerException() throws Exception {
        assumeTrue(isS3Configured());

        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("input should not be null");

        S3DataStore s3ds = getDataStore();
        s3ds.addMetadataRecord((InputStream)null, "name");
    }

    @Test
    public void testBackendAddMetadataRecordNullFileThrowsNullPointerException() throws Exception {
        assumeTrue(isS3Configured());

        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("input should not be null");

        S3DataStore s3ds = getDataStore();
        s3ds.addMetadataRecord((File)null, "name");
    }

    @Test
    public void testBackendAddMetadataRecordNullEmptyNameThrowsIllegalArgumentException() throws Exception {
        assumeTrue(isS3Configured());

        S3DataStore s3ds = getDataStore();

        final String data = "testData";
        for (boolean fromInputStream : List.of(false, true)) {
            for (String name : Arrays.asList(null, "")) {
                try {
                    if (fromInputStream) {
                        s3ds.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), name);
                    } else {
                        File testFile = folder.newFile();
                        copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                        s3ds.addMetadataRecord(testFile, name);
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
    public void testBackendGetMetadataRecordInvalidName() throws Exception {
        assumeTrue(isS3Configured());
        S3DataStore s3ds = getDataStore();

        s3ds.addMetadataRecord(randomStream(0, 10), "testRecord");
        for (String name : Arrays.asList("", null)) {
            try {
                s3ds.getMetadataRecord(name);
                fail("Expect to throw");
            } catch(Exception e) {}
        }
        s3ds.deleteMetadataRecord("testRecord");
    }

    // GetAllMetadataRecords (Backend)

    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesAll() throws Exception {
        assumeTrue(isS3Configured());
        S3DataStore s3ds = getDataStore();

        // reference.key initialized in backend#init() - OAK-9807, so expected 1 record
        assertEquals(1, s3ds.getAllMetadataRecords("").size());
        s3ds.deleteAllMetadataRecords("");

        String prefixAll = "prefix1";
        String prefixSome = "prefix1.prefix2";
        String prefixOne = "prefix1.prefix3";
        String prefixNone = "prefix4";

        s3ds.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
        s3ds.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
        s3ds.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
        s3ds.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));
        s3ds.addMetadataRecord(randomStream(5, 10), "prefix5.testRecord5");

        assertEquals(5, s3ds.getAllMetadataRecords("").size());
        assertEquals(4, s3ds.getAllMetadataRecords(prefixAll).size());
        assertEquals(2, s3ds.getAllMetadataRecords(prefixSome).size());
        assertEquals(1, s3ds.getAllMetadataRecords(prefixOne).size());
        assertEquals(0, s3ds.getAllMetadataRecords(prefixNone).size());

        s3ds.deleteAllMetadataRecords("");
        assertEquals(0, s3ds.getAllMetadataRecords("").size());
    }


    @Test
    public void testBackendGetAllMetadataRecordsNullPrefixThrowsNullPointerException() throws Exception {
        assumeTrue(isS3Configured());

        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("prefix should not be null");

        S3DataStore s3ds = getDataStore();
        s3ds.getAllMetadataRecords(null);
    }

    // MetadataRecordExists (Backend)
    @Test
    public void testBackendMetadataRecordExists() throws Exception {
        assumeTrue(isS3Configured());
        S3DataStore s3ds = getDataStore();

        s3ds.addMetadataRecord(randomStream(0, 10), "name");
        for (String name : Arrays.asList("invalid", "", null)) {
            if (Strings.isNullOrEmpty(name)) {
                try {
                    s3ds.metadataRecordExists(name);
                }
                catch (IllegalArgumentException e) { }
            }
            else {
                assertFalse(s3ds.metadataRecordExists(name));
            }
        }
        assertTrue(s3ds.metadataRecordExists("name"));
    }

    // DeleteMetadataRecord (Backend)

    @Test
    public void testBackendDeleteMetadataRecord() throws Exception {
        assumeTrue(isS3Configured());
        S3DataStore s3ds = getDataStore();

        s3ds.addMetadataRecord(randomStream(0, 10), "name");
        for (String name : Arrays.asList("", null)) {
            if (Strings.isNullOrEmpty(name)) {
                try {
                    s3ds.deleteMetadataRecord(name);
                }
                catch (IllegalArgumentException e) { }
            }
            else {
                s3ds.deleteMetadataRecord(name);
                fail();
            }
        }
        assertTrue(s3ds.deleteMetadataRecord("name"));
    }

    // DeleteAllMetadataRecords (Backend)

    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesAll() throws Exception {
        assumeTrue(isS3Configured());
        S3DataStore s3ds = getDataStore();

        String prefixAll = "prefix1";
        String prefixSome = "prefix1.prefix2";
        String prefixOne = "prefix1.prefix3";
        String prefixNone = "prefix4";

        Map<String, Integer> prefixCounts = new HashMap<>();
        prefixCounts.put(prefixAll, 4);
        prefixCounts.put(prefixSome, 2);
        prefixCounts.put(prefixOne, 1);
        prefixCounts.put(prefixNone, 0);

        for (Map.Entry<String, Integer> entry : prefixCounts.entrySet()) {
            s3ds.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
            s3ds.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
            s3ds.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
            s3ds.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));

            int preCount = s3ds.getAllMetadataRecords("").size();

            s3ds.deleteAllMetadataRecords(entry.getKey());

            int deletedCount = preCount - s3ds.getAllMetadataRecords("").size();
            assertEquals(entry.getValue().intValue(), deletedCount);

            s3ds.deleteAllMetadataRecords("");
        }
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsNoRecordsNoChange() throws Exception {
        assumeTrue(isS3Configured());
        S3DataStore s3ds = getDataStore();

        // reference.key initialized in backend#init() - OAK-9807, so expected 1 record
        assertEquals(1, s3ds.getAllMetadataRecords("").size());

        s3ds.deleteAllMetadataRecords("");

        assertEquals(0, s3ds.getAllMetadataRecords("").size());
    }

    private S3DataStore getDataStore() throws Exception {
        props = S3DataStoreUtils.getS3Config();
        bucket = props.getProperty(S3Constants.S3_BUCKET);
        bucket = bucket + "-" + System.currentTimeMillis();
        props.put(S3Constants.S3_BUCKET, bucket);
        props.put("cacheSize", "0");
        return (S3DataStore) getS3DataStore(s3Class, props, dataStoreDir.getAbsolutePath());
    }

    @After
    public void tearDown() {
        try {
            if (bucket != null) {
                S3DataStoreUtils.deleteBucket(bucket, startTime);
            }
        } catch (Exception ignore) {
        }
    }
}
