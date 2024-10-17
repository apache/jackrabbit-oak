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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.incrementalstore.MergeIncrementalTreeStore;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.TreeSession;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.FilePacker;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class MergeIncrementalTreeStoreTest {
    private static final String BUILD_TARGET_FOLDER = "target";
    private static final Compression algorithm = Compression.GZIP;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    @Test
    public void mergeWithoutTopUp() throws IOException {
        File baseDir = folder.newFolder("base");
        File baseFile = folder.newFile("base.gz");
        File baseMetadata = folder.newFile("base.metadata.gz");

        TreeStore base = new TreeStore("base", baseDir, null, 1);
        base.init();
        base.putNode("/tmp", "{prop1=\"foo\"}");
        base.putNode("/tmp/a", "{prop2=\"foo\"}");
        base.putNode("/tmp/a/b", "{prop3=\"foo\"}");
        base.putNode("/tmp/b", "{prop1=\"foo\"}");
        base.putNode("/tmp/b/c", "{prop2=\"foo\"}");
        base.putNode("/tmp/c", "{prop3=\"foo\"}");
        base.close();

        FilePacker.pack(baseDir, TreeSession.getFileNameRegex(), baseFile, true);

        File increment = folder.newFile("inc.gz");
        File incrementMetadata = folder.newFile("inc.metadata.gz");
        File mergedFile = folder.newFile("merged.gz");
        File mergedDir = folder.newFolder("merged");
        File mergedMetadata = folder.newFile("merged.metadata.gz");

        try (BufferedWriter baseBW = IndexStoreUtils.createWriter(baseMetadata, algorithm)) {
            baseBW.write("{\"checkpoint\":\"" + "r0" + "\",\"storeType\":\"TreeStore\"," +
                    "\"strategy\":\"" + "BaseFFSCreationStrategy" + "\"}");
            baseBW.newLine();
        }

        try (BufferedWriter baseInc = IndexStoreUtils.createWriter(increment, algorithm)) {
            baseInc.write("/tmp/a|{prop2=\"fooModified\"}|r1|M");
            baseInc.newLine();
            baseInc.write("/tmp/b|{prop1=\"foo\"}|r1|D");
            baseInc.newLine();
            baseInc.write("/tmp/b/c/d|{prop2=\"fooNew\"}|r1|A");
            baseInc.newLine();
            baseInc.write("/tmp/c|{prop3=\"fooModified\"}|r1|M");
            baseInc.newLine();
            baseInc.write("/tmp/d|{prop3=\"bar\"}|r1|A");
            baseInc.newLine();
            baseInc.write("/tmp/e|{prop3=\"bar\"}|r1|A");
        }
        try (BufferedWriter baseInc = IndexStoreUtils.createWriter(incrementMetadata, algorithm)) {
            baseInc.write("{\"beforeCheckpoint\":\"" + "r0" + "\",\"afterCheckpoint\":\"" + "r1" + "\"," +
                    "\"storeType\":\"" + "IncrementalFFSType" + "\"," +
                    "\"strategy\":\"" + "pipelineStrategy" + "\"," +
                    "\"preferredPaths\":[]}");
            baseInc.newLine();
        }

        List<String> expectedMergedList = new LinkedList<>();

        expectedMergedList.add("/tmp|{prop1=\"foo\"}");
        expectedMergedList.add("/tmp/a|{prop2=\"fooModified\"}");
        expectedMergedList.add("/tmp/a/b|{prop3=\"foo\"}");
        expectedMergedList.add("/tmp/b/c|{prop2=\"foo\"}");
        expectedMergedList.add("/tmp/b/c/d|{prop2=\"fooNew\"}");
        expectedMergedList.add("/tmp/c|{prop3=\"fooModified\"}");
        expectedMergedList.add("/tmp/d|{prop3=\"bar\"}");
        expectedMergedList.add("/tmp/e|{prop3=\"bar\"}");

        MergeIncrementalTreeStore merge = new MergeIncrementalTreeStore(
                baseFile, increment, mergedFile, algorithm);
        merge.doMerge();
        List<String> expectedMergedMetadataList = new LinkedList<>();
        expectedMergedMetadataList.add("{\"checkpoint\":\"" + "r1" + "\",\"storeType\":\"TreeStore\"," +
                "\"strategy\":\"" + merge.getStrategyName() + "\",\"preferredPaths\":[]}");

        FilePacker.unpack(mergedFile, mergedDir, true);
        TreeStore merged = new TreeStore("merged", mergedDir, null, 1);
        HashSet<String> paths = new HashSet<>();
        int i = 0;
        for (Entry<String, String> e : merged.getSession().entrySet()) {
            String key = e.getKey();
            if (e.getValue().isEmpty()) {
                String[] parts = TreeStore.toParentAndChildNodeName(key);
                String childEntry = TreeStore.toChildNodeEntry(parts[0], parts[1]);
                assertTrue(paths.add(childEntry));
                continue;
            }
            String expected = expectedMergedList.get(i++);
            String actual = key + "|" + e.getValue();
            Assert.assertEquals(expected, actual);
        }
        assertEquals(expectedMergedList.size(), i);
        assertEquals(expectedMergedList.size(), paths.size());
        merged.close();

        try (BufferedReader br = IndexStoreUtils.createReader(mergedMetadata, algorithm)) {
            for (String line : expectedMergedMetadataList) {
                String actual = br.readLine();
                // System.out.println(actual);
                Assert.assertEquals(line, actual);
            }
            Assert.assertNull(br.readLine());
        }
    }

    @Test
    public void mergeWithTopUp() throws IOException {
        File baseDir = folder.newFolder("base");
        File baseFile = folder.newFile("base.gz");
        File baseMetadata = folder.newFile("base.metadata.gz");

        TreeStore base = new TreeStore("base", baseDir, null, 1);
        base.init();
        base.putNode("/tmp", "{prop1=\"foo\"}");
        base.putNode("/tmp/a", "{prop2=\"foo\"}");
        base.putNode("/tmp/a/b", "{prop3=\"foo\"}");
        base.putNode("/tmp/b", "{prop1=\"foo\"}");
        base.putNode("/tmp/b/c", "{prop2=\"foo\"}");
        base.putNode("/tmp/c", "{prop3=\"foo\"}");
        base.putNode("/tmp/z", "{prop=\"theEnd\"}");
        base.close();

        FilePacker.pack(baseDir, TreeSession.getFileNameRegex(), baseFile, true);

        File increment = folder.newFile("inc.gz");
        File incrementMetadata = folder.newFile("inc.metadata.gz");
        File mergedFile = folder.newFile("merged.gz");
        File mergedDir = folder.newFolder("merged");
        File mergedMetadata = folder.newFile("merged.metadata.gz");

        try (BufferedWriter baseBW = IndexStoreUtils.createWriter(baseMetadata, algorithm)) {
            baseBW.write("{\"checkpoint\":\"" + "r0" + "\",\"storeType\":\"TreeStore\"," +
                    "\"strategy\":\"" + "BaseFFSCreationStrategy" + "\"}");
            baseBW.newLine();
        }

        try (BufferedWriter baseInc = IndexStoreUtils.createWriter(increment, algorithm)) {
            baseInc.write("/tmp/a|{prop2=\"fooModified\"}|r1|M");
            baseInc.newLine();
            baseInc.write("/tmp/b|{prop1=\"foo\"}|r1|D");
            baseInc.newLine();
            baseInc.write("/tmp/b/c/d|{prop2=\"fooNew\"}|r1|A");
            baseInc.newLine();
            baseInc.write("/tmp/c|{prop3=\"fooModified\"}|r1|M");
            baseInc.newLine();
            baseInc.write("/tmp/d|{prop3=\"bar\"}|r1|A");
            baseInc.newLine();
            baseInc.write("/tmp/e|{prop3=\"bar\"}|r1|A");
        }
        try (BufferedWriter baseInc = IndexStoreUtils.createWriter(incrementMetadata, algorithm)) {
            baseInc.write("{\"beforeCheckpoint\":\"" + "r0" + "\",\"afterCheckpoint\":\"" + "r1" + "\"," +
                    "\"storeType\":\"" + "IncrementalFFSType" + "\"," +
                    "\"strategy\":\"" + "pipelineStrategy" + "\"," +
                    "\"preferredPaths\":[]}");
            baseInc.newLine();
        }

        File topupFile = new File(folder.getRoot(), MergeIncrementalTreeStore.TOPUP_FILE);
        try (BufferedWriter topUp = IndexStoreUtils.createWriter(topupFile, algorithm)) {
            topUp.write("/tmp/a|{prop2=\"fooModified2\"}|r1|U");
            topUp.newLine();
            topUp.write("/tmp/f|{prop2=\"fooModified2\"}|r1|U");
            topUp.newLine();
            topUp.write("/tmp/g|{prop3=\"bar\"}|r1|R");
            topUp.newLine();
        }

        List<String> expectedMergedList = new LinkedList<>();

        expectedMergedList.add("/tmp|{prop1=\"foo\"}");
        expectedMergedList.add("/tmp/a|{prop2=\"fooModified2\"}");
        expectedMergedList.add("/tmp/a/b|{prop3=\"foo\"}");
        expectedMergedList.add("/tmp/b/c|{prop2=\"foo\"}");
        expectedMergedList.add("/tmp/b/c/d|{prop2=\"fooNew\"}");
        expectedMergedList.add("/tmp/c|{prop3=\"fooModified\"}");
        expectedMergedList.add("/tmp/d|{prop3=\"bar\"}");
        expectedMergedList.add("/tmp/e|{prop3=\"bar\"}");
        expectedMergedList.add("/tmp/f|{prop2=\"fooModified2\"}");
        expectedMergedList.add("/tmp/z|{prop=\"theEnd\"}");

        MergeIncrementalTreeStore merge = new MergeIncrementalTreeStore(
                baseFile, increment, mergedFile, algorithm);
        merge.doMerge();
        List<String> expectedMergedMetadataList = new LinkedList<>();
        expectedMergedMetadataList.add("{\"checkpoint\":\"" + "r1" + "\",\"storeType\":\"TreeStore\"," +
                "\"strategy\":\"" + merge.getStrategyName() + "\",\"preferredPaths\":[]}");

        FilePacker.unpack(mergedFile, mergedDir, true);
        TreeStore merged = new TreeStore("merged", mergedDir, null, 1);
        HashSet<String> paths = new HashSet<>();
        int i = 0;
        for (Entry<String, String> e : merged.getSession().entrySet()) {
            String key = e.getKey();
            if (e.getValue().isEmpty()) {
                String[] parts = TreeStore.toParentAndChildNodeName(key);
                String childEntry = TreeStore.toChildNodeEntry(parts[0], parts[1]);
                assertTrue(paths.add(childEntry));
                continue;
            }
            String expected = expectedMergedList.get(i++);
            String actual = key + "|" + e.getValue();
            Assert.assertEquals(expected, actual);
        }
        assertEquals(expectedMergedList.size(), i);
        assertEquals(expectedMergedList.size(), paths.size());
        merged.close();

        try (BufferedReader br = IndexStoreUtils.createReader(mergedMetadata, algorithm)) {
            for (String line : expectedMergedMetadataList) {
                String actual = br.readLine();
                // System.out.println(actual);
                Assert.assertEquals(line, actual);
            }
            Assert.assertNull(br.readLine());
        }
    }

}
