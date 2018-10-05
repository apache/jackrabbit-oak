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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.ByteArrayDataInput;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakStreamingIndexFileTest.BlobFactoryMode.BATCH_READ;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakStreamingIndexFileTest.BlobFactoryMode.BYTE_WISE_READ;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakStreamingIndexFileTest.FileCreateMode.COPY_BYTES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakStreamingIndexFileTest.FileCreateMode.WRITE_FILE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class OakStreamingIndexFileTest {

    private Random rnd = new Random();

    private NodeState root = EMPTY_NODE;

    private NodeBuilder builder = root.builder();

    private int fileSize = 1000 + rnd.nextInt(1000);

    private final FileCreateMode fileCreateMode;
    private final BlobFactoryMode blobFactoryMode;
    private final ModeDependantBlobFactory modeDependantBlobFactory;

    enum BlobFactoryMode {
        BATCH_READ,
        BYTE_WISE_READ
    }

    enum FileCreateMode {
        WRITE_FILE,
        COPY_BYTES
    }

    @Parameterized.Parameters(name="{0}, {1}")
    public static Collection<Object[]> fixtures() throws Exception {
        List<Object[]> modes = Lists.newArrayList();
        modes.add(new Object[]{COPY_BYTES, BYTE_WISE_READ});
        modes.add(new Object[]{COPY_BYTES, BATCH_READ});
        modes.add(new Object[]{WRITE_FILE, BYTE_WISE_READ});
        modes.add(new Object[]{WRITE_FILE, BATCH_READ});
        return modes;
    }

    public OakStreamingIndexFileTest(FileCreateMode fileCreateMode, BlobFactoryMode blobFactoryMode) {
        this.fileCreateMode = fileCreateMode;
        this.blobFactoryMode = blobFactoryMode;
        this.modeDependantBlobFactory = new ModeDependantBlobFactory(blobFactoryMode);
    }

    @Test
    public void readSanity() throws Exception {
        byte[] fileBytes = writeFile();

        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile readFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            byte[] readBytes = new byte[fileBytes.length];
            readFile.readBytes(readBytes, 0, fileBytes.length);

            assertEquals("Must read same amount of data", fileBytes.length, readBytes.length);
            assertTrue("Must get back same data", Arrays.equals(readBytes, fileBytes));

            try {
                readFile.readBytes(readBytes, 0, 1);
                fail("Must not be able to read past stored number of bytes");
            } catch (Exception ignored) {
                // ignore
            }
        }
    }

    @Test
    public void rangeReadWorks() throws Exception {
        int numFewBytes = 100;

        byte[] fileBytes = writeFile();
        byte[] aFewBytes = Arrays.copyOfRange(fileBytes, 1, numFewBytes + 1);

        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile readFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            readFile.seek(1);
            assertEquals("Seeking should move position", 1, readFile.position());

            byte[] readBytes = new byte[numFewBytes];
            readFile.readBytes(readBytes, 0, numFewBytes);
            assertTrue("Reading a few bytes should be accurate", Arrays.equals(readBytes, aFewBytes));
        }
    }

    @Test
    public void rangeReadWorksOnSeekingBack() throws Exception {
        int numFewBytes = 100;

        byte[] fileBytes = writeFile();
        byte[] aFewBytes = Arrays.copyOfRange(fileBytes, 1, numFewBytes + 1);

        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile readFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            byte[] readBytes = new byte[numFewBytes];

            readFile.seek(100);
            readFile.readBytes(readBytes, 0, 1);
            readFile.seek(1);
            assertEquals("Seeking should move position", 1, readFile.position());

            readFile.readBytes(readBytes, 0, numFewBytes);
            assertTrue("Reading a few bytes should be accurate", Arrays.equals(readBytes, aFewBytes));
        }
    }

    @Test
    public void cloneCreatesSimilarUnrelatedStreams() throws Exception {
        int numFewBytes = 100;

        byte[] fileBytes = writeFile();
        byte[] aFewBytes = Arrays.copyOfRange(fileBytes, 2, numFewBytes + 2);

        byte[] readBytes = new byte[numFewBytes];

        OakStreamingIndexFile readFileClone;

        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile readFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            readFile.seek(1);
            readFile.readBytes(readBytes, 0, 1);

            readFileClone = (OakStreamingIndexFile) readFile.clone();

            readFile.readBytes(readBytes, 0, numFewBytes);
        }

        assertNotNull("Clone reader should have been created", readFileClone);

        try {
            readFileClone.readBytes(readBytes, 0, numFewBytes);
            assertTrue("Clone reader should start from same position as source",
                    Arrays.equals(readBytes, aFewBytes));
        } finally {
            readFileClone.close();
        }
    }

    @Test
    public void streamingWritesDontWorkPiecewise() throws Exception {
        Assume.assumeTrue("Piece write makes sense for " + WRITE_FILE + " mode", fileCreateMode == WRITE_FILE);
        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile writeFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            byte[] fileBytes = randomBytes(fileSize);

            writeFile.writeBytes(fileBytes, 0, fileBytes.length);
            try {
                writeFile.writeBytes(fileBytes, 0, 1);
                fail("Multiple write bytes must not be allowed with streaming writes");
            } catch (Exception ignored) {
                //ignore
            }
            writeFile.flush();
        }
    }

    @Test
    public void seekScenarios() throws Exception {
        byte[] fileBytes = writeFile();

        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile readFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            byte[] aFewBytes = new byte[10];
            byte[] expectedFewBytes;

            expectedFewBytes = Arrays.copyOfRange(fileBytes, 10, 10 + aFewBytes.length);
            readFile.seek(10);
            readFile.readBytes(aFewBytes, 0, aFewBytes.length);
            assertTrue("Range read after seek should read accurately",
                    Arrays.equals(expectedFewBytes, aFewBytes));


            expectedFewBytes = Arrays.copyOfRange(fileBytes, 25, 25 + aFewBytes.length);
            readFile.seek(25);
            readFile.readBytes(aFewBytes, 0, aFewBytes.length);
            assertTrue("Range read after seek should read accurately",
                    Arrays.equals(expectedFewBytes, aFewBytes));


            expectedFewBytes = Arrays.copyOfRange(fileBytes, 2, 2 + aFewBytes.length);
            readFile.seek(2);
            readFile.readBytes(aFewBytes, 0, aFewBytes.length);
            assertTrue("Range read after backward seek should read accurately",
                    Arrays.equals(expectedFewBytes, aFewBytes));

        }
    }

    @Test
    public void logWarnWhenSeekingBackAfterRead() throws Exception {
        byte[] fileBytes = writeFile();

        LogCustomizer logRecorder = LogCustomizer
                .forLogger(OakStreamingIndexFile.class.getName()).enable(Level.WARN)
                .contains("Seeking back on streaming index file").create();

        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile readFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            logRecorder.starting();
            byte[] readBytes = new byte[fileBytes.length];

            readFile.readBytes(readBytes, 0, 10);
            assertEquals("Don't log for simple reads", 0, logRecorder.getLogs().size());

            readFile.seek(12);
            assertEquals("Don't log for forward seeks", 0, logRecorder.getLogs().size());

            readFile.seek(2);
            assertEquals("Log warning for backward seeks", 1, logRecorder.getLogs().size());
        }

        logRecorder.finished();
    }

    static class ModeDependantBlobFactory {

        private final BlobFactoryMode blobFactoryMode;

        ModeDependantBlobFactory(BlobFactoryMode blobFactoryMode) {
            this.blobFactoryMode = blobFactoryMode;
        }

        BlobFactory getNodeBuilderBlobFactory(final NodeBuilder builder) {
            final BlobFactory delegate = BlobFactory.getNodeBuilderBlobFactory(builder);
            return in -> {
                if (blobFactoryMode == BYTE_WISE_READ) {
                    return delegate.createBlob(new InputStream() {
                        @Override
                        public int read() throws IOException {
                            return in.read();
                        }
                    });
                } else {
                    return delegate.createBlob(in);
                }
            };
        }
    }

    private byte[] writeFile() throws Exception {
        byte[] fileBytes = randomBytes(fileSize);

        NodeBuilder fooBuilder = builder.child("foo");
        try (OakStreamingIndexFile writeFile = new OakStreamingIndexFile("foo", fooBuilder, "dirDetails",
                modeDependantBlobFactory.getNodeBuilderBlobFactory(fooBuilder))
        ) {
            if (fileCreateMode == COPY_BYTES) {
                writeFile.copyBytes(new ByteArrayDataInput(fileBytes), fileBytes.length);
            } else {
                writeFile.writeBytes(fileBytes, 0, fileBytes.length);
            }
            writeFile.flush();
        }

        return fileBytes;
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }
}
