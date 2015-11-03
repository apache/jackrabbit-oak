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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.jackrabbit.oak.plugins.segment.file.FileStore.newFileStore;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Tests verifying if the repository gets corrupted or not: {@code OAK-2481 IllegalStateException in TarMk with large number of properties}</p>
 *
 * <p>These tests are disabled by default due to their long running time. On the
 * command line specify {@code -DLargeNumberOfPropertiesTestIT=true} to enable
 * them.</p>
 *
 *<p>If you only want to run this test:<br>
 * {@code mvn verify -Dsurefire.skip.ut=true -PintegrationTesting -Dit.test=LargeNumberOfPropertiesTestIT -DLargeNumberOfPropertiesTestIT=true}
 * </p>
 */
public class LargeNumberOfPropertiesTestIT {

    private static final Logger LOG = LoggerFactory
            .getLogger(LargeNumberOfPropertiesTestIT.class);
    private static final boolean ENABLED = Boolean
            .getBoolean(LargeNumberOfPropertiesTestIT.class.getSimpleName());

    private File directory;

    @Before
    public void setUp() throws IOException {
        assumeTrue(ENABLED);
        directory = File.createTempFile(getClass().getSimpleName(), "dir",
                new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void cleanDir() {
        try {
            if (directory != null) {
                deleteDirectory(directory);
            }
        } catch (IOException e) {
            LOG.error("Error cleaning directory", e);
        }
    }

    @Test
    public void corruption() throws Exception {
        FileStore fileStore = newFileStore(directory).withMaxFileSize(5)
                .withNoCache().withMemoryMapping(true).create();
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        NodeBuilder root = nodeStore.getRoot().builder();

        try {
            NodeBuilder c = root.child("c" + System.currentTimeMillis());
            // i=26 hits the hard limit for the number of properties a node can
            // have (262144)
            for (int i = 0; i < 25; i++) {
                LOG.debug("run {}/24", i);
                for (int j = 0; j < 10000; j++) {
                    c.setProperty("int-" + i + "-" + j, i);
                }
            }
            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } finally {
            fileStore.close();
        }
    }

}
