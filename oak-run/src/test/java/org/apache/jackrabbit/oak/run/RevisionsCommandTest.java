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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.apache.jackrabbit.oak.api.Type.STRING;
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
    private Clock clock;
    public static final String MIN_ID_VALUE = "0000000";
    private VersionGarbageCollector vgc;

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
    public void infoFullGC() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        vgc = ns.getVersionGarbageCollector();
 //       Timestamp ts = new Timestamp();

        // enable the detailed gc flag
        writeField(vgc, "detailedGCEnabled", true, true);

        //1. Create nodes with properties
        NodeBuilder b1 = ns.getRoot().builder();

        // Add property to node & save
        b1.child("x").setProperty("test", "t", STRING);
        b1.child("z").setProperty("test", "t", STRING);
        ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Remove property
        NodeBuilder b2 = ns.getRoot().builder();
        b2.getChildNode("x").removeProperty("test");
        ns.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.runBackgroundOperations();

        //2. move clock forward with 2 hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));

        //3. Create a checkpoint now with expiry of 1 hour
        long expiryTime = 1, delta = MINUTES.toMillis(10);
        NodeBuilder b3 = ns.getRoot().builder();
        b3.getChildNode("z").removeProperty("test");
        ns.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.runBackgroundOperations();

        Revision.fromString(ns.checkpoint(HOURS.toMillis(expiryTime)));

        //4. move clock forward by 10 mins
        clock.waitUntil(clock.getTime() + delta);

        // 5. Remove a node
        NodeBuilder b4 = ns.getRoot().builder();
        b4.getChildNode("z").remove();
        ns.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ns.runBackgroundOperations();

        // 6. Now run gc after checkpoint and see removed properties gets collected
        clock.waitUntil(clock.getTime() + delta*2);
//        ts.setTimestamp(delta);
//        ts.setTimeUnit(MILLISECONDS);
      //  RevisionsOptions options = new RevisionsOptions(olderThan);
        String output = captureSystemOut(new RevisionsCmd("infofullgc"));
        assertTrue(output.contains("Last Successful Run"));
        assertTrue(output.contains("Retrieving Detailed GC info:"));
    }

    @Test
    public void reset() throws Exception {
        ns.getVersionGarbageCollector().gc(1, TimeUnit.HOURS);

        Document doc = ns.getDocumentStore().find(Collection.SETTINGS, "versionGC");
        assertNotNull(doc);

        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("reset"));
        assertTrue(output.contains("resetting recommendations and statistics"));

        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        ns = builderProvider.newBuilder()
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
        doc = ns.getDocumentStore().find(Collection.SETTINGS, "versionGC");
        assertNull(doc);
    }

    @Test
    public void collect() throws Exception {
        ns.dispose();

        String output = captureSystemOut(new RevisionsCmd("collect"));
        assertTrue(output.contains("starting gc collect"));
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
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoUtils.dropCollections(c.getDatabase());
        return builderProvider.newBuilder()
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
