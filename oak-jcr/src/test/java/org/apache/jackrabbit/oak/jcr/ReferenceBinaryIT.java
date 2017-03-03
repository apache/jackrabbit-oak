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

package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.commons.jackrabbit.SimpleReferenceBinary;
import org.apache.jackrabbit.core.data.RandomInputStream;
import org.apache.jackrabbit.oak.fixture.DocumentMongoFixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.fixture.SegmentTarFixture;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;

@RunWith(Parameterized.class)
public class ReferenceBinaryIT {

    //Taken from org.apache.jackrabbit.oak.plugins.segment.Segment
    //As SegmentStore inlines binary content with size less then MEDIUM_LIMIT
    static final int SMALL_LIMIT = 1 << 7;
    static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;

    private static final int STREAM_LENGTH = MEDIUM_LIMIT + 1000;

    private final NodeStoreFixture fixture;

    private NodeStore nodeStore;
    private Repository repository;

    public ReferenceBinaryIT(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Before
    public void setup() throws RepositoryException {
        nodeStore = fixture.createNodeStore();
        repository  = new Jcr(nodeStore).createRepository();
    }

    /**
     * Taken from org.apache.jackrabbit.core.value.ReferenceBinaryTest
     * @throws Exception
     */
    @Test
    public void testReferenceBinaryExchangeWithSharedRepository() throws Exception {
        Session firstSession = createAdminSession();

        // create a binary
        Binary b = firstSession.getValueFactory().createBinary(new RandomInputStream(1, STREAM_LENGTH));

        ReferenceBinary referenceBinary = null;
        if (b instanceof ReferenceBinary) {
            referenceBinary = (ReferenceBinary) b;
        }

        assertNotNull(referenceBinary);

        assertNotNull(referenceBinary.getReference());

        // in the current test the message is exchanged via repository which is shared as well
        // put the reference message value in a property on a node
        String newNode = "sample_" + System.nanoTime();
        firstSession.getRootNode().addNode(newNode).setProperty("reference", referenceBinary.getReference());

        // save the first session
        firstSession.save();

        // get a second session over the same repository / ds
        Session secondSession = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));

        // read the binary referenced by the referencing binary
        String reference = secondSession.getRootNode().getNode(newNode).getProperty("reference").getString();

        ReferenceBinary ref = new SimpleReferenceBinary(reference);

        assertEquals(b, secondSession.getValueFactory().createValue(ref).getBinary());

        safeLogout(firstSession);
        safeLogout(secondSession);

    }

    @After
    public void tearDown() {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        fixture.dispose(nodeStore);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() throws Exception {
        File file = getTestDir("tar");
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(file)
                .withBlobStore(createBlobStore())
                .withMaxFileSize(256)
                .withMemoryMapping(true)
                .build();
        
        SegmentNodeStore sns = SegmentNodeStoreBuilders.builder(fileStore).build();
        
        List<Object[]> fixtures = Lists.newArrayList();
        SegmentTarFixture segmentTarFixture = new SegmentTarFixture(sns);
        
        if (segmentTarFixture.isAvailable()) {
            fixtures.add(new Object[] {segmentTarFixture});
        }
        
        FileBlobStore fbs = new FileBlobStore(getTestDir("fbs1").getAbsolutePath());
        fbs.setReferenceKeyPlainText("foobar");
        FileStore fileStoreWithFBS = FileStoreBuilder.fileStoreBuilder(getTestDir("tar2"))
                .withBlobStore(fbs)
                .withMaxFileSize(256)
                .withMemoryMapping(true)
                .build();
        
        SegmentNodeStore snsWithFBS = SegmentNodeStoreBuilders.builder(fileStoreWithFBS).build();
        
        SegmentTarFixture segmentTarFixtureFBS = new SegmentTarFixture(snsWithFBS);
        if (segmentTarFixtureFBS.isAvailable()) {
            fixtures.add(new Object[] {segmentTarFixtureFBS});
        }

        DocumentMongoFixture documentFixture = new DocumentMongoFixture(MongoUtils.URL, createBlobStore());
        if (documentFixture.isAvailable()) {
            fixtures.add(new Object[]{documentFixture});
        }
        return fixtures;
    }

    private static BlobStore createBlobStore(){
        File file = getTestDir("datastore");
        OakFileDataStore fds = new OakFileDataStore();
        byte[] key = new byte[256];
        new Random().nextBytes(key);
        fds.setReferenceKeyEncoded(BaseEncoding.base64().encode(key));
        fds.setMinRecordLength(4092);
        fds.init(file.getAbsolutePath());
        return new DataStoreBlobStore(fds);
    }

    private static File getTestDir(String prefix) {
        return new File(new File("target"), prefix+ "." + System.nanoTime());
    }

    private Session createAdminSession() throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    private static void safeLogout(Session session) {
        try {
            session.logout();
        } catch (Exception ignore) {}
    }
}