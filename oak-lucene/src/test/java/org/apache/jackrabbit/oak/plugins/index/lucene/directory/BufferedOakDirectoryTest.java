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
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Sets;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.BufferedOakDirectory.DELETE_THRESHOLD_UNTIL_REOPEN;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.BufferedOakDirectory.ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.BufferedOakDirectory.reReadCommandLineParam;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BufferedOakDirectoryTest {

    private Random rnd = new Random();

    private NodeState root = EmptyNodeState.EMPTY_NODE;

    private NodeBuilder builder = root.builder();

    @Test
    public void createOutput() throws Exception {
        Directory buffered = createDir(builder, true);
        byte[] data = writeFile(buffered, "file");

        // must not be visible yet in base
        Directory base = createDir(builder, false);
        assertFalse(base.fileExists("file"));
        base.close();

        buffered.close();

        // now it must exist
        base = createDir(builder, false);
        assertFile(base, "file", data);
        base.close();
    }

    @Test
    public void listAll() throws Exception {
        Directory buffered = createDir(builder, true);
        writeFile(buffered, "file");

        // must only show up after buffered is closed
        Directory base = createDir(builder, false);
        assertEquals(0, base.listAll().length);
        base.close();
        buffered.close();
        base = createDir(builder, false);
        assertEquals(Sets.newHashSet("file"), Sets.newHashSet(base.listAll()));
        base.close();

        buffered = createDir(builder, true);
        buffered.deleteFile("file");
        assertEquals(0, buffered.listAll().length);

        // must only disappear after buffered is closed
        base = createDir(builder, false);
        assertEquals(Sets.newHashSet("file"), Sets.newHashSet(base.listAll()));
        base.close();
        buffered.close();
        base = createDir(builder, false);
        assertEquals(0, base.listAll().length);
        base.close();
    }

    @Test
    public void fileLength() throws Exception {
        Directory base = createDir(builder, false);
        writeFile(base, "file");
        base.close();

        Directory buffered = createDir(builder, true);
        buffered.deleteFile("file");
        try {
            buffered.fileLength("file");
            fail("must throw FileNotFoundException");
        } catch (FileNotFoundException expected) {
            // expected
        }
        try {
            buffered.fileLength("unknown");
            fail("must throw FileNotFoundException");
        } catch (FileNotFoundException expected) {
            // expected
        }
        buffered.close();
    }

    @Test
    public void reopen() throws Exception {
        Random rand = new Random(42);
        Set<String> names = Sets.newHashSet();
        Directory dir = createDir(builder, true);
        for (int i = 0; i < 10 * DELETE_THRESHOLD_UNTIL_REOPEN; i++) {
            String name = "file-" + i;
            writeFile(dir, name);
            if (rand.nextInt(20) != 0) {
                dir.deleteFile(name);
            } else {
                // keep 5%
                names.add(name);
            }
        }
        assertEquals(names, Sets.newHashSet(dir.listAll()));
        dir.close();

        // open unbuffered and check list as well
        dir = createDir(builder, false);
        assertEquals(names, Sets.newHashSet(dir.listAll()));
        dir.close();
    }

    @Test
    public void respectSettingConfigForSingleBlobWrite() {
        boolean oldVal = BufferedOakDirectory.isEnableWritingSingleBlobIndexFile();

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        assertTrue("Flag not setting as set by configuration",
                BufferedOakDirectory.isEnableWritingSingleBlobIndexFile());

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        assertFalse("Flag not setting as set by configuration",
                BufferedOakDirectory.isEnableWritingSingleBlobIndexFile());

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(oldVal);
    }

    @Test
    public void selectWriteStrategyBasedOnFlagAndMode() throws Exception {
        boolean oldVal = BufferedOakDirectory.isEnableWritingSingleBlobIndexFile();

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        try (Directory multiBlobDir = createDir(builder, true)) {
            IndexOutput multiBlobIndexOutput = multiBlobDir.createOutput("foo", IOContext.DEFAULT);

            multiBlobIndexOutput.writeBytes(randomBytes(100), 0, 100);
            multiBlobIndexOutput.flush();
        }

        PropertyState jcrData = builder.getChildNode(":data").getChildNode("foo").getProperty("jcr:data");
        assertTrue("multiple blobs not written", jcrData.isArray());

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        try (Directory multiBlobDir = createDir(builder, true)) {
            IndexOutput multiBlobIndexOutput = multiBlobDir.createOutput("foo", IOContext.DEFAULT);

            multiBlobIndexOutput.writeBytes(randomBytes(100), 0, 100);
            multiBlobIndexOutput.flush();
        }

        jcrData = builder.getChildNode(":data").getChildNode("foo").getProperty("jcr:data");
        assertFalse("multiple blobs written", jcrData.isArray());

        try (Directory multiBlobDir = createDir(builder, false)) {
            IndexOutput multiBlobIndexOutput = multiBlobDir.createOutput("foo", IOContext.DEFAULT);

            multiBlobIndexOutput.writeBytes(randomBytes(100), 0, 100);
            multiBlobIndexOutput.flush();
        }

        jcrData = builder.getChildNode(":data").getChildNode("foo").getProperty("jcr:data");
        assertTrue("multiple blobs not written despite disabled buffered directory", jcrData.isArray());

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(oldVal);
    }

    @Test
    public void readNonStreamingWhenMultipleBlobsExist() throws Exception {
        boolean oldVal = BufferedOakDirectory.isEnableWritingSingleBlobIndexFile();

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        try (Directory multiBlobDir = createDir(builder, true)) {
            IndexOutput multiBlobIndexOutput = multiBlobDir.createOutput("foo", IOContext.DEFAULT);

            multiBlobIndexOutput.writeBytes(randomBytes(100), 0, 100);
            multiBlobIndexOutput.flush();
        }

        // Enable feature... reader shouldn't care about the flag.
        // Repo state needs to be used for that
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        try (Directory multiBlobDir = createDir(builder, true)) {
            OakIndexInput multiBlobIndexInput = (OakIndexInput)multiBlobDir.openInput("foo", IOContext.DEFAULT);

            assertTrue("OakBufferedIndexFile must be used",
                    multiBlobIndexInput.file instanceof OakBufferedIndexFile);
        }

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(oldVal);
    }

    @Test
    public void readStreamingWithSingleBlob() throws Exception {
        boolean oldVal = BufferedOakDirectory.isEnableWritingSingleBlobIndexFile();

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        try (Directory multiBlobDir = createDir(builder, true)) {
            IndexOutput multiBlobIndexOutput = multiBlobDir.createOutput("foo", IOContext.DEFAULT);

            multiBlobIndexOutput.writeBytes(randomBytes(100), 0, 100);
            multiBlobIndexOutput.flush();
        }

        // Enable feature... reader shouldn't care about the flag.
        // Repo state needs to be used for that
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        try (Directory multiBlobDir = createDir(builder, true)) {
            OakIndexInput multiBlobIndexInput = (OakIndexInput)multiBlobDir.openInput("foo", IOContext.DEFAULT);

            assertTrue("OakStreamingIndexFile must be used",
                    multiBlobIndexInput.file instanceof OakStreamingIndexFile);
        }

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(oldVal);
    }

    @Test
    public void writeNonStreamingIfDisabledByFlag() throws Exception {
        boolean oldVal = BufferedOakDirectory.isEnableWritingSingleBlobIndexFile();

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        try (Directory multiBlobDir = createDir(builder, true)) {
            OakIndexOutput multiBlobIndexOutput = (OakIndexOutput)multiBlobDir.createOutput("foo1", IOContext.DEFAULT);

            assertTrue("OakBufferedIndexFile must be used",
                    multiBlobIndexOutput.file instanceof OakBufferedIndexFile);
        }

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        try (Directory multiBlobDir = createDir(builder, true)) {
            OakIndexOutput multiBlobIndexOutput = (OakIndexOutput)multiBlobDir.createOutput("foo2", IOContext.DEFAULT);

            assertTrue("OakStreamingIndexFile must be used",
                    multiBlobIndexOutput.file instanceof OakStreamingIndexFile);
        }

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(oldVal);
    }

    @Test
    public void defaultValue() {
        BufferedOakDirectory bufferedOakDirectory = (BufferedOakDirectory)createDir(builder, true);
        assertTrue("Flag not setting as set by command line flag",
                bufferedOakDirectory.isEnableWritingSingleBlobIndexFile());
    }

    @Test
    public void commandLineParamSetsValue() {
        String oldVal = System.getProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);

        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "true");
        reReadCommandLineParam();
        assertTrue("Flag not setting as set by command line flag",
                BufferedOakDirectory.isEnableWritingSingleBlobIndexFile());

        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "false");
        reReadCommandLineParam();
        assertFalse("Flag not setting as set by command line flag",
                BufferedOakDirectory.isEnableWritingSingleBlobIndexFile());

        if (oldVal == null) {
            System.clearProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);
        } else {
            System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, oldVal);
        }
    }

    @Test
    public void commandLineOverridesSetter() {
        String oldVal = System.getProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);

        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "true");
        reReadCommandLineParam();
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        assertTrue("Flag not setting as set by command line flag",
                BufferedOakDirectory.isEnableWritingSingleBlobIndexFile());

        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "false");
        reReadCommandLineParam();
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        assertFalse("Flag not setting as set by command line flag",
                BufferedOakDirectory.isEnableWritingSingleBlobIndexFile());

        if (oldVal == null) {
            System.clearProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);
        } else {
            System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, oldVal);
        }
    }

    @Test
    public void settingConfigDifferentFromCLIWarns() {
        String oldVal = System.getProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);

        final LogCustomizer custom = LogCustomizer
                .forLogger(BufferedOakDirectory.class.getName())
                .contains("Ignoring configuration ")
                .enable(Level.WARN).create();

        custom.starting();
        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "true");
        reReadCommandLineParam();
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        assertEquals("Warn on conflicting config on CLI and set method", 1, custom.getLogs().size());
        custom.finished();

        custom.starting();
        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "false");
        reReadCommandLineParam();
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        assertEquals("Warn on conflicting config on CLI and set method", 1, custom.getLogs().size());
        custom.finished();

        if (oldVal == null) {
            System.clearProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);
        } else {
            System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, oldVal);
        }
    }

    @Test
    public void dontWarnUnnecesarily() {
        String oldVal = System.getProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);

        final LogCustomizer custom = LogCustomizer
                .forLogger(BufferedOakDirectory.class.getName())
                .contains("Ignoring configuration ")
                .enable(Level.WARN).create();

        custom.starting();

        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        assertEquals("Don't warn unnecessarily", 0, custom.getLogs().size());

        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "true");
        reReadCommandLineParam();
        assertEquals("Don't warn unnecessarily", 0, custom.getLogs().size());
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(true);
        assertEquals("Don't warn unnecessarily", 0, custom.getLogs().size());
        System.clearProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);

        System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, "false");
        reReadCommandLineParam();
        assertEquals("Don't warn unnecessarily", 0, custom.getLogs().size());
        BufferedOakDirectory.setEnableWritingSingleBlobIndexFile(false);
        assertEquals("Don't warn unnecessarily", 0, custom.getLogs().size());
        System.clearProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);

        custom.finished();

        if (oldVal == null) {
            System.clearProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM);
        } else {
            System.setProperty(ENABLE_WRITING_SINGLE_BLOB_INDEX_FILE_PARAM, oldVal);
        }
    }

    private void assertFile(Directory dir, String file, byte[] expected)
            throws IOException {
        assertTrue(dir.fileExists(file));
        assertEquals(expected.length, dir.fileLength(file));
        IndexInput in = dir.openInput(file, IOContext.DEFAULT);
        byte[] data = new byte[expected.length];
        in.readBytes(data, 0, data.length);
        in.close();
        assertTrue(Arrays.equals(expected, data));
    }

    private Directory createDir(NodeBuilder builder, boolean buffered) {
        IndexDefinition def = new IndexDefinition(root, builder.getNodeState(), "/foo");
        if (buffered) {
            return new BufferedOakDirectory(builder, INDEX_DATA_CHILD_NAME, def, null);
        } else {
            return new OakDirectory(builder, def,false);
        }
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }

    private byte[] writeFile(Directory dir, String name) throws IOException {
        byte[] data = randomBytes(rnd.nextInt((int) (16 * FileUtils.ONE_KB)));
        IndexOutput out = dir.createOutput(name, IOContext.DEFAULT);
        out.writeBytes(data, data.length);
        out.close();
        return data;
    }
}
