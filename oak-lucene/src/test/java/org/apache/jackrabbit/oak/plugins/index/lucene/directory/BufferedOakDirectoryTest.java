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

import com.google.common.collect.Sets;

import org.apache.commons.io.FileUtils;
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
