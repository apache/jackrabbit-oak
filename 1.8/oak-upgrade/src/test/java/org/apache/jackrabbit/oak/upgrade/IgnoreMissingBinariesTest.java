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
package org.apache.jackrabbit.oak.upgrade;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.OakUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.container.FileDataStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentNodeStoreContainer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IgnoreMissingBinariesTest extends AbstractOak2OakTest {

    private static final Logger log = LoggerFactory.getLogger(IgnoreMissingBinariesTest.class);

    private final FileDataStoreContainer blob;

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    @Before
    public void prepare() throws Exception {
        NodeStore source = getSourceContainer().open();
        try {
            initContent(source);
        } finally {
            getSourceContainer().close();
        }

        assertTrue(new File(blob.getDirectory(), "0c/07/02/0c0702b43bfcc7c0bb1329a10bbc6d5c5ef15856afd714c1331495b95f65b292").delete());

        String[] args = getArgs();
        log.info("oak2oak {}", Joiner.on(' ').join(args));
        OakUpgrade.main(args);

        createSession();
    }

    public IgnoreMissingBinariesTest() throws IOException {
        blob = new FileDataStoreContainer();
        source = new SegmentNodeStoreContainer(blob);
        destination = new SegmentNodeStoreContainer(blob);
    }

    @Override
    protected NodeStoreContainer getSourceContainer() {
        return source;
    }

    @Override
    protected NodeStoreContainer getDestinationContainer() {
        return destination;
    }

    @Override
    protected String[] getArgs() {
        return new String[]{"--ignore-missing-binaries", "--src-datastore", blob.getDescription(), source.getDescription(), destination.getDescription()};
    }

    @Test
    public void validateMigration() throws RepositoryException, IOException {
        verifyContent(session);
        verifyBlob(session);
        assertEquals(0, session.getNode("/libs/sling/xss/config.xml/jcr:content").getProperty("jcr:data").getLength());
    }
}
