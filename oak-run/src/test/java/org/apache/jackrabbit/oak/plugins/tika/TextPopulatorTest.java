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
package org.apache.jackrabbit.oak.plugins.tika;

import org.apache.jackrabbit.guava.common.collect.FluentIterable;
import org.apache.jackrabbit.guava.common.io.ByteSource;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TextWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakAnalyzer;
import org.apache.jackrabbit.oak.plugins.tika.TextPopulator.PopulatorStats;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TextPopulatorTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File indexDir = null;
    private File csv = null;
    private FakeTextWriter textWriter = new FakeTextWriter();
    private PopulatorStats stats = new PopulatorStats();
    private TextPopulator textPopulator = new TextPopulator(textWriter);

    @Before
    public void setup() throws Exception {
        indexDir = temporaryFolder.newFolder("index-dump");
        csv = temporaryFolder.newFile("blobs.csv");

        textPopulator.setStats(stats);

        setupIndexData();
    }

    private void setupIndexData() throws Exception {
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put("/sentence", "some sentence.");
        dataMap.put("/para", "some sentence.\nAnd more sentence after a new line");
        dataMap.put("/error", TextPopulator.ERROR_TEXT);
        dataMap.put("/null", null);
        dataMap.put("/empty", "");
        dataMap.put("/untrimmed-empty", " ");
        dataMap.put("/untrimmed", " untrimmed ");

        FSDirectory directory = FSDirectory.open(indexDir);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, new OakAnalyzer(Version.LUCENE_47));
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (Map.Entry<String, String> data : dataMap.entrySet()) {
                writer.addDocument(createLuceneDocument(data.getKey(), data.getValue()));
            }

            // add document with multiple :fulltext
            writer.addDocument(createLuceneDocument("/multi", "value1", "value2"));
        }
    }

    private void setupCSV(String ... paths) throws IOException {
        BinaryResourceProvider brp = new FakeBinaryResourceProvider(paths);
        CSVFileGenerator generator = new CSVFileGenerator(csv);
        generator.generate(brp.getBinaries("/"));
    }

    private List<Field> createLuceneDocument(@NotNull String path, String ... values) {
        List<Field> fields = new ArrayList<>();
        for (String value : values) {
            if (value != null) {
                fields.add(FieldFactory.newFulltextField(value, true));
            }
        }
        fields.add(FieldFactory.newPathField(path));
        return fields;
    }

    @Test
    public void simpleTest() throws Exception {
        setupCSV("/sentence", "/para");

        textPopulator.populate(csv, indexDir);
        assertEquals("Incorrect binaries processed", 2, stats.processed);

        textPopulator.populate(csv, indexDir);
        assertEquals("Repeated call for already processed stuff shouldn't process anything more",
                2, stats.ignored);

        assertConsistentStatsAndWriter();
        assertStatsInvariants();
    }

    @Test
    public void untrimmedText() throws Exception {
        setupCSV("/untrimmed");

        textPopulator.populate(csv, indexDir);
        assertEquals("Store generation didn't trim data", "untrimmed",
                textWriter.data.get(FakeBinaryResourceProvider.getBlobId("/untrimmed")));

        assertConsistentStatsAndWriter();
        assertStatsInvariants();
    }

    @Test
    public void indexedError() throws Exception {
        setupCSV("/error");

        textPopulator.populate(csv, indexDir);
        assertEquals("Indexed data reporting errored extraction not marked as error",
                1, stats.errored);

        textPopulator.populate(csv, indexDir);
        assertEquals("Repeated run for indexed error shouldn't get processed again", 1, stats.ignored);

        assertConsistentStatsAndWriter();
        assertStatsInvariants();
    }

    @Test
    public void indexedEmpty() throws Exception {
        setupCSV("/empty");

        textPopulator.populate(csv, indexDir);
        assertEquals("Indexed data for empty extraction not marked as empty",
                1, stats.empty);

        textPopulator.populate(csv, indexDir);
        assertEquals("Repeated run for empty extraction shouldn't get processed again", 1, stats.ignored);

        assertConsistentStatsAndWriter();
        assertStatsInvariants();
    }

    @Test
    public void indexedUntrimmedEmpty() throws Exception {
        setupCSV("/untrimmed-empty");

        textPopulator.populate(csv, indexDir);
        assertEquals("Indexed data for untrimmed empty extraction not marked as empty",
                1, stats.empty);

        textPopulator.populate(csv, indexDir);
        assertEquals("Repeated run for untrimmed empty extraction shouldn't get processed again",
                1, stats.ignored);

        assertConsistentStatsAndWriter();
        assertStatsInvariants();
    }

    @Test
    public void multiFTField() throws Exception {
        setupCSV("/multi");

        textPopulator.populate(csv, indexDir);
        assertEquals("Multi FT field in a doc not marked as error",
                1, stats.errored);

        textPopulator.populate(csv, indexDir);
        assertEquals("Repeated run for multi FT error should get processed again", 0, stats.ignored);

        assertStatsInvariants();
    }

    @Test
    public void indexHasDocumentButNotData() throws Exception {
        setupCSV("/null");

        textPopulator.populate(csv, indexDir);
        assertEquals("No FT field in a doc not marked as error",
                1, stats.errored);

        textPopulator.populate(csv, indexDir);
        assertEquals("Repeated run for no FT error should get processed again", 0, stats.ignored);

        assertStatsInvariants();
    }

    @Test
    public void indexDoesNotHaveDocument() throws Exception {
        setupCSV("/somethingRandom");

        textPopulator.populate(csv, indexDir);
        assertEquals("No indexed doc not marked as error",
                1, stats.errored);

        textPopulator.populate(csv, indexDir);
        assertEquals("Repeated run for no indexed doc error should get processed again", 0, stats.ignored);

        assertStatsInvariants();
    }

    private void assertConsistentStatsAndWriter() {
        assertEquals("Num blobs processed by text writer didn't process same not same as reported in stats",
                textWriter.processed.size(), stats.processed);

    }

    private void assertStatsInvariants() {
        assertTrue("Read (" + stats.read + ") !=" +
                        " Processed (" + stats.processed + ") + Ignored (" + stats.ignored + ")",
                stats.read == stats.processed + stats.ignored);

        assertTrue("Processed (" + stats.processed + ") !=" +
                        " Empty (" + stats.empty + ") + Errored (" + stats.errored + ") + Parsed (" + stats.parsed + ")",
                stats.processed == stats.empty + stats.errored + stats.parsed);
    }

    private static class FakeTextWriter implements TextWriter {
        final Set<String> processed = new HashSet<>();
        final Map<String, String> data = new HashMap<>();

        @Override
        public void write(@NotNull String blobId, @NotNull String text) {
            processed.add(blobId);
            data.put(blobId, text);
        }

        @Override
        public void markEmpty(String blobId) {
            processed.add(blobId);
        }

        @Override
        public void markError(String blobId) {
            processed.add(blobId);
        }

        @Override
        public boolean isProcessed(String blobId) {
            return processed.contains(blobId);
        }
    }

    private static class FakeBinaryResourceProvider implements BinaryResourceProvider {
        private List<BinaryResource> binaries = new ArrayList<>();

        FakeBinaryResourceProvider(String ... paths) {
            for (String path : paths) {
                binaries.add(new BinaryResource(new StringByteSource(""), null, null, path, getBlobId(path)));
            }
        }

        static String getBlobId(String path) {
            return path + ":" + path;
        }

        @Override
        public FluentIterable<BinaryResource> getBinaries(String path) {
            return new FluentIterable<BinaryResource>() {
                @NotNull
                @Override
                public Iterator<BinaryResource> iterator() {
                    return binaries.iterator();
                }
            };
        }
    }

    private static class StringByteSource extends ByteSource {
        private final String data;

        StringByteSource(String data) {
            this.data = data;
        }

        @Override
        public InputStream openStream() {
            return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        }
    }
}
