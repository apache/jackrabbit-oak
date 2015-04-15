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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeNumberOfPropertiesTestIT {

    private static final Logger LOG = LoggerFactory
            .getLogger(SegmentReferenceLimitTestIT.class);

    private File directory;

    @Before
    public void setUp() throws IOException {
        directory = File.createTempFile("LargeNumberOfPropertiesTestIT", "dir",
                new File("target"));
        directory.delete();
        directory.mkdir();
    }

    @After
    public void cleanDir() {
        try {
            FileUtils.deleteDirectory(directory);
        } catch (IOException e) {
            LOG.error("Error cleaning directory", e);
        }
    }

    /**
     * OAK-2481 IllegalStateException in TarMk with large number of properties
     *
     * TODO Test is currently ignored because of how memory intensive it is
     *
     * @see <a
     *      href="https://issues.apache.org/jira/browse/OAK-2481">OAK-2481</a>
     */
    @Test
    @Ignore
    public void corruption() throws Exception {
        FileStore fileStore = new FileStore(directory, 5, 0, false);
        SegmentNodeStore nodeStore = new SegmentNodeStore(fileStore);

        NodeBuilder root = nodeStore.getRoot().builder();

        try {
            NodeBuilder c = root.child("c" + System.currentTimeMillis());
            // i=26 hits the hard limit for the number of properties a node can
            // have (262144)
            for (int i = 0; i < 25; i++) {
                System.out.println(i);
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
