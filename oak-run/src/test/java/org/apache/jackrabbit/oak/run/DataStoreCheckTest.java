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
package org.apache.jackrabbit.oak.run;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DataStoreCheckCommand}
 */
public class DataStoreCheckTest {
    private static final Logger log = LoggerFactory.getLogger(DataStoreCheckTest.class);

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private String storePath;

    private Set<String> blobsAdded;

    private String cfgFilePath;

    private String dsPath;

    @Before
    public void setup() throws Exception {
        OakFileDataStore delegate = new OakFileDataStore();
        dsPath = temporaryFolder.newFolder().getAbsolutePath();
        delegate.setPath(dsPath);
        delegate.init(null);
        DataStoreBlobStore blobStore = new DataStoreBlobStore(delegate);

        File storeFile = temporaryFolder.newFolder();
        storePath = storeFile.getAbsolutePath();
        FileStore.Builder builder = FileStore.builder(storeFile)
            .withBlobStore(blobStore).withMaxFileSize(256)
            .withCacheSize(64).withMemoryMapping(false);
        FileStore fileStore = builder.build();
        NodeStore store = SegmentNodeStore.builder(fileStore).build();

        /* Create nodes with blobs stored in DS*/
        NodeBuilder a = store.getRoot().builder();
        int numBlobs = 10;
        blobsAdded = Sets.newHashSet();
        for (int i = 0; i < numBlobs; i++) {
            SegmentBlob b = (SegmentBlob) store.createBlob(randomStream(i, 18342));
            Iterator<String> idIter = blobStore.resolveChunks(b.getBlobId());
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                blobsAdded.add(chunk);
            }
            a.child("c" + i).setProperty("x", b);
        }

        store.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        log.info("Created blobs : {}", blobsAdded);

        File cfgFile = temporaryFolder.newFile();
        BufferedWriter writer = Files.newWriter(cfgFile, Charsets.UTF_8);
        FileIOUtils.writeAsLine(writer, "path=\"" + StringEscapeUtils.escapeJava(dsPath) + "\"",false);
        writer.close();
        cfgFilePath = cfgFile.getAbsolutePath();

        fileStore.close();
        blobStore.close();
    }

    @After
    public void tearDown() {
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.out)));
    }

    @Test
    public void testCorrect() throws Exception {
        File dump = temporaryFolder.newFolder();
        testAllParams(dump);
    }

    @Test
    public void testConsistency() throws Exception {
        File dump = temporaryFolder.newFolder();

        // Delete a random blob from datastore
        OakFileDataStore delegate = new OakFileDataStore();
        delegate.setPath(dsPath);
        delegate.init(null);
        DataStoreBlobStore blobStore = new DataStoreBlobStore(delegate);

        Random rand = new Random();
        String deletedBlobId = Iterables.get(blobsAdded, rand.nextInt(blobsAdded.size()));
        blobsAdded.remove(deletedBlobId);
        long count = blobStore.countDeleteChunks(ImmutableList.of(deletedBlobId), 0);
        assertEquals(1, count);

        testAllParams(dump);

        assertFileEquals(dump, "[id]", blobsAdded);
        assertFileEquals(dump, "[ref]", Sets.union(blobsAdded, Sets.newHashSet(deletedBlobId)));
        assertFileEquals(dump, "[consistency]", Sets.newHashSet(deletedBlobId));
    }

    public void testAllParams(File dump) throws Exception {
        DataStoreCheckCommand checkCommand = new DataStoreCheckCommand();
        List<String> argsList = Lists
            .newArrayList("--id", "--ref", "--consistency", "--fds", cfgFilePath, "--store", storePath,
                "--dump", dump.getAbsolutePath());

        checkCommand.execute(argsList.toArray(new String[0]));
    }

    @Test
    public void testMissingOpParams() throws Exception {
        File dump = temporaryFolder.newFolder();
        List<String> argsList = Lists
            .newArrayList("--fds", cfgFilePath, "--store", storePath,
                "--dump", dump.getAbsolutePath());
        testIncorrectParams(argsList, "Missing "
            + "required option(s) ['id', 'ref', 'consistency']");
    }

    @Test
    public void testTarNoDS() throws Exception {
        File dump = temporaryFolder.newFolder();
        List<String> argsList = Lists
            .newArrayList("--id", "--ref", "--consistency", "--store", storePath,
                "--dump", dump.getAbsolutePath());
        testIncorrectParams(argsList, "Operation not defined for SegmentNodeStore without external datastore");

    }

    @Test
    public void testOpNoStore() throws Exception {
        File dump = temporaryFolder.newFolder();
        List<String> argsList = Lists
            .newArrayList("--consistency", "--fds", cfgFilePath,
                "--dump", dump.getAbsolutePath());
        testIncorrectParams(argsList, "Missing required option(s) ['store']");

        argsList = Lists
            .newArrayList("--ref", "--fds", cfgFilePath,
                "--dump", dump.getAbsolutePath());
        testIncorrectParams(argsList, "Missing required option(s) ['store']");
    }

    public static void testIncorrectParams(List<String> argList, String assertMsg) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer));

        DataStoreCheckCommand checkCommand = new DataStoreCheckCommand();

        checkCommand.execute(argList.toArray(new String[0]));
        String message = buffer.toString(Charsets.UTF_8.toString());
        Assert.assertTrue(message.contains(assertMsg));
        System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.out)));
    }

    private static void assertFileEquals(File dump, String prefix, Set<String> blobsAdded)
        throws IOException {
        File files[] =
            FileFilterUtils.filter(FileFilterUtils.prefixFileFilter(prefix), dump.listFiles());
        Assert.assertNotNull(files);
        Assert.assertTrue(files.length == 1);
        Assert.assertTrue(files[0].exists());
        assertEquals(blobsAdded,
            FileIOUtils.readStringsAsSet(new FileInputStream(files[0]), false));
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
}
