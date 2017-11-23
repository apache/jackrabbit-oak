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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.microsoft.azure.storage.StorageException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.security.DigestOutputStream;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.jcr.RepositoryException;

/**
 * Test {@link AzureDataStore} with AzureDataStore and local cache on.
 * It requires to pass azure config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link AzureDataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/azure.properties. Sample azure properties located at
 * src/test/resources/azure.properties
 */
public class AzureDataStoreTest {
    protected static final Logger LOG = LoggerFactory.getLogger(AzureDataStoreTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Properties props;
    private static byte[] testBuffer = "test".getBytes();
    private AzureDataStore ds;
    private AzureBlobStoreBackend backend;
    private String container;
    Random randomGen = new Random();

    @BeforeClass
    public static void assumptions() {
        assumeTrue(AzureDataStoreUtils.isAzureConfigured());
    }

    @Before
    public void setup() throws IOException, RepositoryException, URISyntaxException, InvalidKeyException, StorageException {

        props = AzureDataStoreUtils.getAzureConfig();
        container = String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                    + "-test";
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container);

        ds = new AzureDataStore();
        ds.setProperties(props);
        ds.setCacheSize(0);  // Turn caching off so we don't get weird test results due to caching
        ds.init(folder.newFolder().getAbsolutePath());
        backend = (AzureBlobStoreBackend) ds.getBackend();
    }

    @After
    public void teardown() throws InvalidKeyException, URISyntaxException, StorageException {
        ds = null;
        try {
            AzureDataStoreUtils.deleteContainer(container);
        } catch (Exception ignore) {}
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataRecord rhs)
            throws DataStoreException, IOException {
        validateRecord(record, contents, rhs.getIdentifier(), rhs.getLength(), rhs.getLastModified());
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataIdentifier identifier,
                                final long length,
                                final long lastModified)
            throws DataStoreException, IOException {
        validateRecord(record, contents, identifier, length, lastModified, true);
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataIdentifier identifier,
                                final long length,
                                final long lastModified,
                                final boolean lastModifiedEquals)
            throws DataStoreException, IOException {
        assertEquals(record.getLength(), length);
        if (lastModifiedEquals) {
            assertEquals(record.getLastModified(), lastModified);
        } else {
            assertTrue(record.getLastModified() > lastModified);
        }
        assertTrue(record.getIdentifier().toString().equals(identifier.toString()));
        StringWriter writer = new StringWriter();
        org.apache.commons.io.IOUtils.copy(record.getStream(), writer, "utf-8");
        assertTrue(writer.toString().equals(contents));
    }

    private static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private static String getIdForInputStream(final InputStream in)
            throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        OutputStream output = new DigestOutputStream(new NullOutputStream(), digest);
        try {
            IOUtils.copyLarge(in, output);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(in);
        }
        return encodeHexString(digest.digest());
    }


    @Test
    public void testCreateAndDeleteBlobHappyPath() throws DataStoreException, IOException {
        final DataRecord uploadedRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
        DataIdentifier identifier = uploadedRecord.getIdentifier();
        assertTrue(backend.exists(identifier));
        assertTrue(0 != uploadedRecord.getLastModified());
        assertEquals(testBuffer.length, uploadedRecord.getLength());

        final DataRecord retrievedRecord = ds.getRecord(identifier);
        validateRecord(retrievedRecord, new String(testBuffer), uploadedRecord);

        ds.deleteRecord(identifier);
        assertFalse(backend.exists(uploadedRecord.getIdentifier()));
    }


    @Test
    public void testCreateAndReUploadBlob() throws DataStoreException, IOException {
        final DataRecord createdRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
        DataIdentifier identifier1 = createdRecord.getIdentifier();
        assertTrue(backend.exists(identifier1));

        final DataRecord record1 = ds.getRecord(identifier1);
        validateRecord(record1, new String(testBuffer), createdRecord);

        try { Thread.sleep(1001); } catch (InterruptedException e) { }

        final DataRecord updatedRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
        DataIdentifier identifier2 = updatedRecord.getIdentifier();
        assertTrue(backend.exists(identifier2));

        assertTrue(identifier1.toString().equals(identifier2.toString()));
        validateRecord(record1, new String(testBuffer), createdRecord);

        ds.deleteRecord(identifier1);
        assertFalse(backend.exists(createdRecord.getIdentifier()));
    }

    @Test
    public void testListBlobs() throws DataStoreException, IOException {
        final Set<DataIdentifier> identifiers = Sets.newHashSet();
        final Set<String> testStrings = Sets.newHashSet("test1", "test2", "test3");

        for (String s : testStrings) {
            identifiers.add(ds.addRecord(new ByteArrayInputStream(s.getBytes())).getIdentifier());
        }

        Iterator<DataIdentifier> iter = ds.getAllIdentifiers();
        while (iter.hasNext()) {
            DataIdentifier identifier = iter.next();
            assertTrue(identifiers.contains(identifier));
            ds.deleteRecord(identifier);
        }
    }

    ////
    // Backend Tests
    ////

    private void validateRecordData(final SharedBackend backend,
                                    final DataIdentifier identifier,
                                    int expectedSize,
                                    final InputStream expected) throws IOException, DataStoreException {
        byte[] blobData = new byte[expectedSize];
        backend.read(identifier).read(blobData);
        byte[] expectedData = new byte[expectedSize];
        expected.read(expectedData);
        for (int i=0; i<expectedSize; i++) {
            assertEquals(expectedData[i], blobData[i]);
        }
    }

    // Write (Backend)

    @Test
    public void testBackendWriteDifferentSizedRecords() throws IOException, NoSuchAlgorithmException, DataStoreException {
        // Sizes are chosen as follows:
        // 0 - explicitly test zero-size file
        // 10 - very small file
        // 1000 - under 4K (a reasonably expected stream buffer size)
        // 4100 - over 4K but under 8K and 16K (other reasonably expected stream buffer sizes)
        // 16500 - over 8K and 16K but under 64K (another reasonably expected stream buffer size)
        // 66000 - over 64K but under 128K (probably the largest reasonably expected stream buffer size)
        // 132000 - over 128K
        for (int size : Lists.newArrayList(0, 10, 1000, 4100, 16500, 66000, 132000)) {
            File testFile = folder.newFile();
            copyInputStreamToFile(randomStream(size, size), testFile);
            DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile)));
            backend.write(identifier, testFile);
            assertTrue(backend.exists(identifier));

            validateRecordData(backend, identifier, size, new FileInputStream(testFile));

            backend.deleteRecord(identifier);
            assertFalse(backend.exists(identifier));
        }
    }

    @Test
    public void testBackendWriteRecordNullIdentifierThrowsNullPointerException() throws IOException, DataStoreException{
        DataIdentifier identifier = null;
        File testFile = folder.newFile();
        copyInputStreamToFile(randomStream(0, 10), testFile);
        try {
            backend.write(identifier, testFile);
            fail();
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }
    }

    @Test
    public void testBackendWriteRecordNullFileThrowsNullPointerException() throws DataStoreException {
        File testFile = null;
        DataIdentifier identifier = new DataIdentifier("fake");
        try {
            backend.write(identifier, testFile);
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("file".equals(e.getMessage()));
        }
    }

    @Test
    public void testBackendWriteRecordFileNotFoundThrowsException() throws IOException, NoSuchAlgorithmException {
        File testFile = folder.newFile();
        copyInputStreamToFile(randomStream(0, 10), testFile);
        DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile)));
        assertTrue(testFile.delete());
        try {
            backend.write(identifier, testFile);
            fail();
        } catch (DataStoreException e) {
            assertTrue(e.getCause() instanceof FileNotFoundException);
        }
    }

    // Read (Backend)

    @Test
    public void testBackendReadRecordNullIdentifier() throws DataStoreException {
        DataIdentifier identifier = null;
        try {
            backend.read(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assert("identifier".equals(e.getMessage()));
        }
    }

    @Test
    public void testBackendReadRecordInvalidIdentifier() {
        DataIdentifier identifier = new DataIdentifier("fake");
        try {
            backend.read(identifier);
            fail();
        }
        catch (DataStoreException e) { }
    }

    // Delete (Backend)

    @Test
    public void testBackendDeleteRecordNullIdentifier() throws DataStoreException {
        DataIdentifier identifier = null;
        try {
            backend.deleteRecord(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assert("identifier".equals(e.getMessage()));
        }
    }

    @Test
    public void testBackendDeleteRecordInvalidIdentifier() throws DataStoreException {
        DataIdentifier identifier = new DataIdentifier("fake");
        backend.deleteRecord(identifier); // We don't care if the identifier is invalid; this is a noop
    }

    // Exists (Backend)

    @Test
    public void testBackendNotCreatedRecordDoesNotExist() throws DataStoreException {
        assertFalse(backend.exists(new DataIdentifier(("fake"))));
    }

    @Test
    public void testBackendRecordExistsNullIdentifierThrowsNullPointerException() throws DataStoreException {
        try {
            DataIdentifier nullIdentifier = null;
            backend.exists(nullIdentifier);
            fail();
        }
        catch (NullPointerException e) { }
    }

    // GetAllIdentifiers (Backend)

    @Test
    public void testBackendGetAllIdentifiersNoRecordsReturnsNone() throws DataStoreException {
        Iterator<DataIdentifier> allIdentifiers = backend.getAllIdentifiers();
        assertFalse(allIdentifiers.hasNext());
    }

    @Test
    public void testBackendGetAllIdentifiers() throws DataStoreException, IOException, NoSuchAlgorithmException {
        for (int expectedRecCount : Lists.newArrayList(1, 2, 5)) {
            final List<DataIdentifier> ids = Lists.newArrayList();
            for (int i=0; i<expectedRecCount; i++) {
                File testfile = folder.newFile();
                copyInputStreamToFile(randomStream(i, 10), testfile);
                DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testfile)));
                backend.write(identifier, testfile);
                ids.add(identifier);
            }

            int actualRecCount = Iterators.size(backend.getAllIdentifiers());

            for (DataIdentifier identifier : ids) {
                backend.deleteRecord(identifier);
            }

            assertEquals(expectedRecCount, actualRecCount);
        }
    }

    // GetRecord (Backend)

    @Test
    public void testBackendGetRecord() throws IOException, DataStoreException {
        String recordData = "testData";
        DataRecord record = ds.addRecord(new ByteArrayInputStream(recordData.getBytes()));
        DataRecord retrievedRecord = backend.getRecord(record.getIdentifier());
        validateRecord(record, recordData, retrievedRecord);
    }

    @Test
    public void testBackendGetRecordNullIdentifierThrowsNullPointerException() throws DataStoreException {
        try {
            DataIdentifier identifier = null;
            backend.getRecord(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("identifier".equals(e.getMessage()));
        }
    }

    @Test
    public void testBackendGetRecordInvalidIdentifierThrowsDataStoreException() {
        try {
            backend.getRecord(new DataIdentifier("invalid"));
            fail();
        }
        catch (DataStoreException e) {

        }
    }

    // GetAllRecords (Backend)

    @Test
    public void testBackendGetAllRecordsReturnsAll() throws DataStoreException, IOException {
        for (int recCount : Lists.newArrayList(0, 1, 2, 5)) {
            Map<DataIdentifier, String> addedRecords = Maps.newHashMap();
            if (0 < recCount) {
                for (int i = 0; i < recCount; i++) {
                    String data = String.format("testData%d", i);
                    DataRecord record = ds.addRecord(new ByteArrayInputStream(data.getBytes()));
                    addedRecords.put(record.getIdentifier(), data);
                }
            }

            Iterator<DataRecord> iter = backend.getAllRecords();
            List<DataIdentifier> identifiers = Lists.newArrayList();
            int actualCount = 0;
            while (iter.hasNext()) {
                DataRecord record = iter.next();
                identifiers.add(record.getIdentifier());
                assertTrue(addedRecords.containsKey(record.getIdentifier()));
                StringWriter writer = new StringWriter();
                IOUtils.copy(record.getStream(), writer);
                assertTrue(writer.toString().equals(addedRecords.get(record.getIdentifier())));
                actualCount++;
            }

            for (DataIdentifier identifier : identifiers) {
                ds.deleteRecord(identifier);
            }

            assertEquals(recCount, actualCount);
        }
    }

    // AddMetadataRecord (Backend)

    @Test
    public void testBackendAddMetadataRecordsFromInputStream() throws DataStoreException, IOException, NoSuchAlgorithmException {
        for (boolean fromInputStream : Lists.newArrayList(false, true)) {
            String prefix = String.format("%s.META.", getClass().getSimpleName());
            for (int count : Lists.newArrayList(1, 3)) {
                Map<String, String> records = Maps.newHashMap();
                for (int i = 0; i < count; i++) {
                    String recordName = String.format("%sname.%d", prefix, i);
                    String data = String.format("testData%d", i);
                    records.put(recordName, data);

                    if (fromInputStream) {
                        backend.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), recordName);
                    }
                    else {
                        File testFile = folder.newFile();
                        copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                        backend.addMetadataRecord(testFile, recordName);
                    }
                }

                assertEquals(count, backend.getAllMetadataRecords(prefix).size());

                for (Map.Entry<String, String> entry : records.entrySet()) {
                    DataRecord record = backend.getMetadataRecord(entry.getKey());
                    StringWriter writer = new StringWriter();
                    IOUtils.copy(record.getStream(), writer);
                    backend.deleteMetadataRecord(entry.getKey());
                    assertTrue(writer.toString().equals(entry.getValue()));
                }

                assertEquals(0, backend.getAllMetadataRecords(prefix).size());
            }
        }
    }

    @Test
    public void testBackendAddMetadataRecordFileNotFoundThrowsDataStoreException() throws IOException {
        File testFile = folder.newFile();
        copyInputStreamToFile(randomStream(0, 10), testFile);
        testFile.delete();
        try {
            backend.addMetadataRecord(testFile, "name");
            fail();
        }
        catch (DataStoreException e) {
            assertTrue(e.getCause() instanceof FileNotFoundException);
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullInputStreamThrowsNullPointerException() throws DataStoreException {
        try {
            backend.addMetadataRecord((InputStream)null, "name");
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("input".equals(e.getMessage()));
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullFileThrowsNullPointerException() throws DataStoreException {
        try {
            backend.addMetadataRecord((File)null, "name");
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("input".equals(e.getMessage()));
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullEmptyNameThrowsIllegalArgumentException() throws DataStoreException, IOException {
        final String data = "testData";
        for (boolean fromInputStream : Lists.newArrayList(false, true)) {
            for (String name : Lists.newArrayList(null, "")) {
                try {
                    if (fromInputStream) {
                        backend.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), name);
                    } else {
                        File testFile = folder.newFile();
                        copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                        backend.addMetadataRecord(testFile, name);
                    }
                    fail();
                } catch (IllegalArgumentException e) {
                    assertTrue("name".equals(e.getMessage()));
                }
            }
        }
    }

    // GetMetadataRecord (Backend)

    @Test
    public void testBackendGetMetadataRecordInvalidName() throws DataStoreException {
        backend.addMetadataRecord(randomStream(0, 10), "testRecord");
        assertNull(backend.getMetadataRecord("invalid"));
        for (String name : Lists.newArrayList("", null)) {
            try {
                backend.getMetadataRecord(name);
                fail("Expect to throw");
            } catch(Exception e) {}
        }

        backend.deleteMetadataRecord("testRecord");
    }

    // GetAllMetadataRecords (Backend)

    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesAll() throws DataStoreException {
        assertEquals(0, backend.getAllMetadataRecords("").size());

        String prefixAll = "prefix1";
        String prefixSome = "prefix1.prefix2";
        String prefixOne = "prefix1.prefix3";
        String prefixNone = "prefix4";

        backend.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
        backend.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
        backend.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
        backend.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));
        backend.addMetadataRecord(randomStream(5, 10), "prefix5.testRecord5");

        assertEquals(5, backend.getAllMetadataRecords("").size());
        assertEquals(4, backend.getAllMetadataRecords(prefixAll).size());
        assertEquals(2, backend.getAllMetadataRecords(prefixSome).size());
        assertEquals(1, backend.getAllMetadataRecords(prefixOne).size());
        assertEquals(0, backend.getAllMetadataRecords(prefixNone).size());

        backend.deleteAllMetadataRecords("");
        assertEquals(0, backend.getAllMetadataRecords("").size());
    }

    @Test
    public void testBackendGetAllMetadataRecordsNullPrefixThrowsNullPointerException() {
        try {
            backend.getAllMetadataRecords(null);
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("prefix".equals(e.getMessage()));
        }
    }

    // DeleteMetadataRecord (Backend)

    @Test
    public void testBackendDeleteMetadataRecord() throws DataStoreException {
        backend.addMetadataRecord(randomStream(0, 10), "name");
        for (String name : Lists.newArrayList("invalid", "", null)) {
            if (Strings.isNullOrEmpty(name)) {
                try {
                    backend.deleteMetadataRecord(name);
                }
                catch (IllegalArgumentException e) { }
            }
            else {
                assertFalse(backend.deleteMetadataRecord(name));
            }
        }
        assertTrue(backend.deleteMetadataRecord("name"));
    }

    // DeleteAllMetadataRecords (Backend)

    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesAll() throws DataStoreException {
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
            backend.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
            backend.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
            backend.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
            backend.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));

            int preCount = backend.getAllMetadataRecords("").size();

            backend.deleteAllMetadataRecords(entry.getKey());

            int deletedCount = preCount - backend.getAllMetadataRecords("").size();
            assertEquals(entry.getValue().intValue(), deletedCount);

            backend.deleteAllMetadataRecords("");
        }
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsNoRecordsNoChange() {
        assertEquals(0, backend.getAllMetadataRecords("").size());

        backend.deleteAllMetadataRecords("");

        assertEquals(0, backend.getAllMetadataRecords("").size());
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsNullPrefixThrowsNullPointerException() {
        try {
            backend.deleteAllMetadataRecords(null);
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("prefix".equals(e.getMessage()));
        }
    }

    @Test
    public void testSecret() throws Exception {
        byte[] data = new byte[4096];
        randomGen.nextBytes(data);
        DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
        assertEquals(data.length, rec.getLength());
        String ref = rec.getReference();

        String id = rec.getIdentifier().toString();
        assertNotNull(ref);

        byte[] refKey = backend.getOrCreateReferenceKey();

        Mac mac = Mac.getInstance("HmacSHA1");
        mac.init(new SecretKeySpec(refKey, "HmacSHA1"));
        byte[] hash = mac.doFinal(id.getBytes("UTF-8"));
        String calcRef = id + ':' + encodeHexString(hash);

        assertEquals("getReference() not equal", calcRef, ref);

        DataRecord refRec = ds.getMetadataRecord("reference.key");
        assertNotNull("Reference data record null", refRec);

        byte[] refDirectFromBackend = IOUtils.toByteArray(refRec.getStream());
        LOG.warn("Ref direct from backend {}", refDirectFromBackend);
        assertTrue("refKey in memory not equal to the metadata record",
            Arrays.equals(refKey, refDirectFromBackend));
    }
}
