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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexSanityCheckerTest {
    private Random rnd = new Random();

    private Directory local = new RAMDirectory();
    private Directory remote = new RAMDirectory();

    @Test
    public void validDirs() throws Exception{
        byte[] t1 = writeFile(local, "t1", 100);
        writeFile(remote, "t1", t1);

        assertTrue(new IndexSanityChecker("/foo", local, remote).check());

        assertTrue(local.fileExists("t1"));
        assertTrue(remote.fileExists("t1"));
    }

    @Test
    public void sizeMismatch() throws Exception{
        byte[] t1L = writeFile(local, "t1", 100);
        // write t1 remote with at least one byte more
        writeFile(remote, "t1", randomBytes(t1L.length + rnd.nextInt(10) + 1));
        // write t2 remote only
        byte[] t2R = writeFile(remote, "t2", 120);
        // write t3 remote and local with same size and data
        byte[] t3R = writeFile(remote, "t3", 140);
        writeFile(local, "t3", t3R);

        assertFalse(new IndexSanityChecker("/foo", local, remote).check());

        assertTrue(remote.fileExists("t3"));

        //In case of size mismatch all local files would be removed
        assertFalse(local.fileExists("t1"));
        assertFalse(local.fileExists("t3"));
    }

    @Test
    public void extraLocalFiles() throws Exception{
        byte[] t1L = writeFile(local, "t1", 100);
        byte[] t3R = writeFile(remote, "t3", 140);
        writeFile(local, "t3", t3R);

        new IndexSanityChecker("/foo", local, remote).check();

        //t1 exist in local but not in remote
        //it must be removed
        assertFalse(local.fileExists("t1"));

        //t3 should remain present
        assertTrue(remote.fileExists("t3"));
    }

    private byte[] writeFile(Directory dir, String name, int size) throws IOException {
        byte[] data = randomBytes(rnd.nextInt(size) + 1);
        writeFile(dir, name, data);
        return data;
    }

    private void writeFile(Directory dir, String name, byte[] data) throws IOException {
        IndexOutput o = dir.createOutput(name, IOContext.DEFAULT);
        o.writeBytes(data, data.length);
        o.close();
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }
}