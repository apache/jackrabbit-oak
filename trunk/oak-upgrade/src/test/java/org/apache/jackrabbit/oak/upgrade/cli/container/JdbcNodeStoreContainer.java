/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade.cli.container;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.JdbcFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import static org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer.deleteRecursive;

public class JdbcNodeStoreContainer implements NodeStoreContainer {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcNodeStoreContainer.class);

    private final File h2Dir;

    private final String jdbcUri;

    private final JdbcFactory jdbcFactory;

    private final BlobStoreContainer blob;

    private Closer closer;

    public JdbcNodeStoreContainer() throws IOException {
        this(new DummyBlobStoreContainer());
    }

    public JdbcNodeStoreContainer(BlobStoreContainer blob) throws IOException {
        this.blob = blob;
        this.h2Dir = Files.createTempDirectory(Paths.get("target"), "repo-h2").toFile();
        this.jdbcUri = String.format("jdbc:h2:%s", h2Dir.getAbsolutePath() + "/JdbcNodeStoreContainer");
        this.jdbcFactory = new JdbcFactory(jdbcUri, 2, "sa", "sa", false);
    }

    @Override
    public NodeStore open() throws IOException {
        this.closer = Closer.create();
        return jdbcFactory.create(blob.open(), closer);
    }

    @Override
    public void close() {
        try {
            if (closer != null) {
                closer.close();
                closer = null;
            }
        } catch (IOException e) {
            LOG.error("Can't close document node store", e);
        }
    }

    @Override
    public void clean() throws IOException {
        deleteRecursive(h2Dir);
        blob.clean();
    }

    @Override
    public String getDescription() {
        return jdbcUri;
    }
}
