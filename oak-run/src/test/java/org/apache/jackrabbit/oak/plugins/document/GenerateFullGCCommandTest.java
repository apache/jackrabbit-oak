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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.run.GenerateFullGCCommand;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.CommandTestUtils.captureSystemOut;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class GenerateFullGCCommandTest {

    final String OPTION_GARBAGE_NODES_COUNT = "--garbageNodesCount";
    final String OPTION_GARBAGE_NODES_PARENT_COUNT = "--garbageNodesParentCount";
    final String OPTION_GARBAGE_TYPE = "--garbageType";
    final String OPTION_MAX_REVISION_AGE_MILLIS = "--maxRevisionAgeMillis";
    final String OPTION_MAX_REVISION_AGE_DELAY_SECONDS = "--maxRevisionAgeDelaySeconds";
    final String OPTION_NUMBER_OF_RUNS = "--numberOfRuns";
    final String OPTION_GENERATE_INTERVAL_SECONDS = "--generateIntervalSeconds";

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

    private DocumentNodeStore createDocumentNodeStore() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoUtils.dropCollections(c.getDatabase());
        return builderProvider.newBuilder().setFullGCEnabled(false)
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
    }

    @Test
    public void generateGarbageEmptyPropsUnderOneParent() {
        ns.dispose();

        String output = captureSystemOut(new GenerateFullGCCmd(OPTION_GARBAGE_NODES_COUNT, "10", OPTION_GARBAGE_NODES_PARENT_COUNT, "1",
                OPTION_GARBAGE_TYPE, "1"));
//        assertTrue(output.contains("DryRun is enabled : true"));
//        assertTrue(output.contains("ResetFullGC is enabled : false"));
//        assertTrue(output.contains("Compaction is enabled : false"));
//        assertTrue(output.contains("starting gc collect"));
//        assertTrue(output.contains("FullGcMode is : 3"));

        String str = "";
    }

    private static class GenerateFullGCCmd implements Runnable {

        private final ImmutableList<String> args;

        public GenerateFullGCCmd(String... args) {
            this.args = ImmutableList.<String>builder().add(MongoUtils.URL)
                    .add(args).build();
        }

        @Override
        public void run() {
            try {
                new GenerateFullGCCommand().execute(args.toArray(new String[0]));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
