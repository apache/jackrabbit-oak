/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.blob;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStoreFriend;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceWrapper;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStoreTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests the RDBBlobStore implementation.
 */
@RunWith(Parameterized.class)
public class RDBBlobStoreTest extends AbstractBlobStoreTest {

    @Override
    protected boolean supportsStatsCollection() {
        return true;
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        Collection<Object[]> result = new ArrayList<Object[]>();
        RDBBlobStoreFixture candidates[] = new RDBBlobStoreFixture[] { RDBBlobStoreFixture.RDB_DB2, RDBBlobStoreFixture.RDB_H2,
                RDBBlobStoreFixture.RDB_DERBY, RDBBlobStoreFixture.RDB_MSSQL, RDBBlobStoreFixture.RDB_MYSQL,
                RDBBlobStoreFixture.RDB_ORACLE, RDBBlobStoreFixture.RDB_PG };

        for (RDBBlobStoreFixture bsf : candidates) {
            if (bsf.isAvailable()) {
                result.add(new Object[] { bsf });
            }
        }

        return result;
    }

    private RDBBlobStore blobStore;
    private String blobStoreName;
    private RDBDataSourceWrapper dsw;

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStoreTest.class);

    public RDBBlobStoreTest(RDBBlobStoreFixture bsf) {
        blobStore = bsf.createRDBBlobStore();
        blobStoreName = bsf.getName();
        dsw = bsf.getDataSource();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        blobStore.setBlockSize(128);
        blobStore.setBlockSizeMin(48);
        this.store = blobStore;
        empty(blobStore);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (blobStore != null) {
            empty(blobStore);
            blobStore.close();
        }
    }

    private static void empty(RDBBlobStore blobStore) throws Exception {
        Iterator<String> iter = blobStore.getAllChunkIds(0);
        List<String> ids = Lists.newArrayList();
        while (iter.hasNext()) {
            ids.add(iter.next());
        }
        blobStore.deleteChunks(ids, 0);
    }

    @Test
    public void testBigBlob() throws Exception {
        int min = 0;
        int max = 8 * 1024 * 1024;
        int test = 0;

        while (max - min > 256) {
            if (test == 0) {
                test = max; // try largest first
            } else {
                test = (max + min) / 2;
            }
            byte[] data = new byte[test];
            Random r = new Random(0);
            r.nextBytes(data);
            byte[] digest = getDigest(data);
            try {
                RDBBlobStoreFriend.storeBlock(blobStore, digest, 0, data);
                byte[] data2 = RDBBlobStoreFriend.readBlockFromBackend(blobStore, digest);
                if (!Arrays.equals(data, data2)) {
                    throw new Exception("data mismatch for length " + data.length);
                }
                min = test;
            } catch (Exception ex) {
                max = test;
            }
        }

        LOG.info("max blob length for " + blobStoreName + " was " + test);

        int expected = Math.max(blobStore.getBlockSize(), 2 * 1024 * 1024);
        assertTrue(blobStoreName + ": expected supported block size is " + expected + ", but measured: " + test, test >= expected);
    }

    @Test
    public void testDeleteManyBlobs() throws Exception {
        // see https://issues.apache.org/jira/browse/OAK-3807
        int count = 3000;
        List<String> toDelete = new ArrayList<String>();

        for (int i = 0; i < count; i++) {
            byte[] data = new byte[256];
            Random r = new Random(0);
            r.nextBytes(data);
            byte[] digest = getDigest(data);
            RDBBlobStoreFriend.storeBlock(blobStore, digest, 0, data);
            byte[] data2 = RDBBlobStoreFriend.readBlockFromBackend(blobStore, digest);
            if (!Arrays.equals(data, data2)) {
                throw new Exception("data mismatch for length " + data.length);
            }
            String id = StringUtils.convertBytesToHex(digest);
            toDelete.add(id);
        }

        RDBBlobStoreFriend.deleteChunks(blobStore, toDelete, System.currentTimeMillis() + 1000);
    }

    @Test
    public void testUpdateAndDelete() throws Exception {
        byte[] data = new byte[256];
        Random r = new Random(0);
        r.nextBytes(data);
        byte[] digest = getDigest(data);
        RDBBlobStoreFriend.storeBlock(blobStore, digest, 0, data);
        String id = StringUtils.convertBytesToHex(digest);
        long until = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < until) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        // Force update to update timestamp
        long beforeUpdateTs = System.currentTimeMillis() - 100;
        RDBBlobStoreFriend.storeBlock(blobStore, digest, 0, data);
        // Metadata row should not have been touched
        Assert.assertFalse("entry was cleaned although it shouldn't have",
                blobStore.deleteChunks(ImmutableList.of(id), beforeUpdateTs));
        // Actual data row should still be present
        Assert.assertNotNull(RDBBlobStoreFriend.readBlockFromBackend(blobStore, digest));
    }

    @Test
    public void testDeleteChunks() throws Exception {
        byte[] data1 = new byte[256];
        Random r = new Random(0);
        r.nextBytes(data1);
        byte[] digest1 = getDigest(data1);
        RDBBlobStoreFriend.storeBlock(blobStore, digest1, 0, data1);
        String id1 = StringUtils.convertBytesToHex(digest1);

        long now = System.currentTimeMillis();

        long until = System.currentTimeMillis() + 10;
        while (System.currentTimeMillis() < until) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
            }
        }

        byte[] data2 = new byte[256];
        r.nextBytes(data2);
        byte[] digest2 = getDigest(data2);
        RDBBlobStoreFriend.storeBlock(blobStore, digest2, 0, data2);

        Assert.assertEquals("meta entry was not removed", 1, blobStore.countDeleteChunks(ImmutableList.of(id1), now));
        Assert.assertFalse("data entry was not removed", RDBBlobStoreFriend.isDataEntryPresent(blobStore, digest1));
    }

    @Test
    public void testResilienceMissingMetaEntry() throws Exception {
        int test = 1024 * 1024;
        byte[] data = new byte[test];
        Random r = new Random(0);
        r.nextBytes(data);
        byte[] digest = getDigest(data);
        RDBBlobStoreFriend.storeBlock(blobStore, digest, 0, data);
        byte[] data2 = RDBBlobStoreFriend.readBlockFromBackend(blobStore, digest);
        if (!Arrays.equals(data, data2)) {
            throw new Exception("data mismatch");
        }

        RDBBlobStoreFriend.killMetaEntry(blobStore, digest);

        // retry
        RDBBlobStoreFriend.storeBlock(blobStore, digest, 0, data);
        byte[] data3 = RDBBlobStoreFriend.readBlockFromBackend(blobStore, digest);
        if (!Arrays.equals(data, data3)) {
            throw new Exception("data mismatch");
        }
    }

    @Test
    public void testExceptionHandling() throws Exception {
        // see OAK-7068
        try {
            int test = 1024 * 1024;
            byte[] data = new byte[test];
            Random r = new Random(0);
            r.nextBytes(data);
            byte[] digest = getDigest(data);
            dsw.setTemporaryUpdateException("testExceptionHandling");
            RDBBlobStoreFriend.storeBlock(blobStore, digest, 0, data);
            fail("expects IOException");
        } catch (IOException expected) {
        } finally {
            dsw.setTemporaryUpdateException(null);
        }
    }

    private byte[] getDigest(byte[] bytes) throws IOException {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
        messageDigest.update(bytes, 0, bytes.length);
        return messageDigest.digest();
    }
}
