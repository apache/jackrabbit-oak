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
import org.apache.jackrabbit.oak.index.indexer.document.incrementalstore.MergeIncrementalFlatFileStore;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MergeIncrementalFFSTest {
    private static final String BUILD_TARGET_FOLDER = "target";
    private static final Compression algorithm = Compression.GZIP;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File(BUILD_TARGET_FOLDER));

    @Test
    public void test1() throws IOException {

        File baseFFS = folder.newFile("base.gz");
        File baseFFSMetadata = folder.newFile("base.metadata.gz");
        File incFFS = folder.newFile("inc.gz");
        File incFFSMetadata = folder.newFile("inc.metadata.gz");
        File mergedFFS = folder.newFile("merged.gz");

        try (BufferedWriter baseBW = IndexStoreUtils.createWriter(baseFFS, algorithm)) {
            baseBW.write("/tmp|{prop1=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/a|{prop2=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/a/b|{prop3=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/b|{prop1=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/b/c|{prop2=\"foo\"}");
            baseBW.newLine();
            baseBW.write("/tmp/c|{prop3=\"foo\"}");
        }
        try (BufferedWriter baseBW = IndexStoreUtils.createWriter(baseFFSMetadata, algorithm)) {
            baseBW.write("{\"checkpoint\":\"" + "r0" + "\",\"storeType\":\"FlatFileStore\"," +
                    "\"strategy\":\"" + "BaseFFSCreationStrategy" + "\",\"preferredPaths\":[]}");
            baseBW.newLine();
        }

        try (BufferedWriter baseInc = IndexStoreUtils.createWriter(incFFS, algorithm)) {
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
        try (BufferedWriter baseInc = IndexStoreUtils.createWriter(incFFSMetadata, algorithm)) {
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


        MergeIncrementalFlatFileStore merge = new MergeIncrementalFlatFileStore(Collections.emptySet(), baseFFS, incFFS, mergedFFS, algorithm);
        merge.doMerge();
        List<String> expectedMergedMetadataList = new LinkedList<>();
        expectedMergedMetadataList.add("{\"checkpoint\":\"" + "r1" + "\",\"storeType\":\"FlatFileStore\"," +
                "\"strategy\":\"" + merge.getStrategyName() + "\",\"preferredPaths\":[]}");

        try (BufferedReader br = IndexStoreUtils.createReader(mergedFFS, algorithm)) {
            for (String line : expectedMergedList) {
                String actual = br.readLine();
                System.out.println(actual);
                Assert.assertEquals(line, actual);

            }
            Assert.assertNull(br.readLine());
        }

        try (BufferedReader br = IndexStoreUtils.createReader(IndexStoreUtils.getMetadataFile(mergedFFS, algorithm), algorithm)) {
            for (String line : expectedMergedMetadataList) {
                String actual = br.readLine();
                System.out.println(actual);
                Assert.assertEquals(line, actual);
            }
            Assert.assertNull(br.readLine());
        }
    }

}
