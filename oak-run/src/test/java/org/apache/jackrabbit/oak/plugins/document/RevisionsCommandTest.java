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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;

import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.run.RevisionsCommand;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_ID;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class RevisionsCommandTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void before() {
        ns = createDocumentNodeStore();
    }

    @Test
    public void info() throws Exception {
        ns.getVersionGarbageCollector().gc(1, TimeUnit.HOURS);
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("info"));
        assertTrue(output.contains("Last Successful Run"));
    }

    @Test
    public void reset() throws Exception {
        ns.getVersionGarbageCollector().gc(1, TimeUnit.HOURS);

        Document doc = ns.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(doc);

        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("reset"));
        assertTrue(output.contains("resetting recommendations and statistics"));

        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        ns = builderProvider.newBuilder()
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
        doc = ns.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID);
        assertNull(doc);
    }

    @Test
    public void resetDetailedGC() throws Exception {
        // need to set detailedGCEnabled to true, so let's bounce the default one
        ns.dispose();
        // and create it fresh with detailedGCEnabled==true
        ns = createDocumentNodeStore(true);
        ns.getVersionGarbageCollector().gc(1, TimeUnit.HOURS);

        Document doc = ns.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(doc);
        assertNotNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP));
        assertNotNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP));

        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("reset", "--detailedGCOnly"));
        assertTrue(output.contains("resetting recommendations and statistics"));

        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        ns = builderProvider.newBuilder()
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
        doc = ns.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(doc);
        assertNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP));
        assertNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP));
    }

    @Test
    public void resetDryRunFields() throws Exception {
        // need to set detailedGCEnabled to true, so let's bounce the default one
        ns.dispose();
        // and create it fresh with detailedGCEnabled==true
        ns = createDocumentNodeStore(true);
        ns.getVersionGarbageCollector().gc(1, TimeUnit.HOURS);

        Document doc = ns.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(doc);
        assertNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_TIMESTAMP_PROP));
        assertNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_DOCUMENT_ID_PROP));

        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--entireRepo"));
        assertTrue(output.contains("DryRun is enabled : true"));

        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        ns = builderProvider.newBuilder()
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
        doc = ns.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID);
        assertNotNull(doc);
        assertNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_TIMESTAMP_PROP));
        assertNull(doc.get(SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_DOCUMENT_ID_PROP));
    }

    @Test
    public void collect() throws Exception {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("collect"));
        assertTrue(output.contains("starting gc collect"));
    }

    @Test
    public void detailedGC() {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--entireRepo"));
        assertTrue(output.contains("DryRun is enabled : true"));
        assertTrue(output.contains("starting gc collect"));
    }

    @Test
    public void detailedGCWithoutDryRun() {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--dryRun", "false", "--entireRepo"));
        assertTrue(output.contains("DryRun is enabled : false"));
        assertTrue(output.contains("starting gc collect"));
    }

    @Test
    public void detailedGCWithDryRun() {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--dryRun", "true", "--entireRepo"));
        assertTrue(output.contains("DryRun is enabled : true"));
        assertTrue(output.contains("starting gc collect"));
    }

    @Test
    public void embeddedVerification() {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--entireRepo"));
        assertTrue(output.contains("EmbeddedVerification is enabled : true"));
        assertTrue(output.contains("starting gc collect"));
    }

    @Test
    public void detailedGCWithoutEmbeddedVerification() {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--verify", "false", "--entireRepo"));
        assertTrue(output.contains("EmbeddedVerification is enabled : false"));
        assertTrue(output.contains("starting gc collect"));
    }

    @Test
    public void detailedGCWithEmbeddedVerification() {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--verify", "true", "--entireRepo"));
        assertTrue(output.contains("EmbeddedVerification is enabled : true"));
        assertTrue(output.contains("starting gc collect"));
    }

    @Test
    public void detailedGCWithEmbeddedWithoutEntireRepoOrPath() {
        ns.dispose();

        String errOutput = captureSystemErr(new RevisionsCmd("detailedGC", "--verify", "true"));
        assertTrue(errOutput.contains("--path or --entireRepo option is required for detailedGC command"));
    }

    @Test
    public void detailedGCWithEmbeddedWithPath() {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("detailedGC", "--verify", "true", "--path", "/"));
        assertTrue(output.contains("EmbeddedVerification is enabled : true"));
        assertTrue(output.contains("running detailedGC on the document: /"));
    }

    @Test
    public void detailedGCWithEmbeddedWithNonExistingPath() {
        ns.dispose();

        String errOutput = captureSystemErr(new RevisionsCmd("detailedGC", "--verify", "true", "--path", "/non-existing-path"));
        assertTrue(errOutput.contains("Document not found: /non-existing-path"));
    }

    @Test
    public void sweep() throws Exception {
        int clusterId = ns.getClusterId();
        String output = captureSystemErr(new Sweep(clusterId));
        assertThat(output, containsString("cannot sweep revisions for active clusterId"));

        output = captureSystemErr(new Sweep(0));
        assertThat(output, containsString("clusterId option is required"));

        output = captureSystemErr(new Sweep(99));
        assertThat(output, containsString("store does not have changes with clusterId"));

        ns.dispose();
        output = captureSystemOut(new Sweep(clusterId));
        assertThat(output, containsString("Revision sweep not needed for clusterId"));

        // remove the sweep revision to force a sweep run
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        DocumentNodeStoreBuilder<?> builder = builderProvider.newBuilder()
                .setMongoDB(c.getMongoClient(), c.getDBName());
        DocumentStore store = builder.getDocumentStore();
        UpdateOp op = new UpdateOp(getIdFromPath("/"), false);
        op.removeMapEntry("_sweepRev", new Revision(0, 0, clusterId));
        assertNotNull(store.findAndUpdate(Collection.NODES, op));

        output = captureSystemOut(new Sweep(clusterId));
        assertThat(output, containsString("Updated sweep revision to"));
    }

    private DocumentNodeStore createDocumentNodeStore() {
        return createDocumentNodeStore(false);
    }

    private DocumentNodeStore createDocumentNodeStore(boolean detailedGCEnabled) {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoUtils.dropCollections(c.getDatabase());
        return builderProvider.newBuilder().setDetailedGCEnabled(detailedGCEnabled)
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
    }

    private String captureSystemOut(Runnable r) {
        PrintStream old = System.out;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            System.setOut(ps);
            r.run();
            System.out.flush();
            return baos.toString();
        } finally {
            System.setOut(old);
        }
    }

    private String captureSystemErr(Runnable r) {
        PrintStream old = System.err;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            System.setErr(ps);
            r.run();
            System.err.flush();
            return baos.toString();
        } finally {
            System.setErr(old);
        }
    }

    private static class RevisionsCmd implements Runnable {

        private final ImmutableList<String> args;

        public RevisionsCmd(String... args) {
            this.args = ImmutableList.<String>builder().add(MongoUtils.URL)
                    .add(args).build().asList();
        }

        @Override
        public void run() {
            try {
                new RevisionsCommand().execute(args.toArray(new String[args.size()]));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class Sweep extends RevisionsCmd {

        Sweep(int clusterId) {
            super("sweep", "--clusterId", String.valueOf(clusterId));
        }
    }
}
