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
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.AbstractOak2OakTest;
import org.apache.jackrabbit.oak.upgrade.cli.OakUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.container.BlobStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.FileDataStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.MongoNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.IOException;

import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.upgrade.cli.container.MongoNodeStoreContainer.isMongoAvailable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

public class IncludeIndexTest extends AbstractOak2OakTest {

    private static final Logger log = LoggerFactory.getLogger(IncludeIndexTest.class);

    private final BlobStoreContainer blob;

    private final NodeStoreContainer source;

    private final NodeStoreContainer destination;

    private NodeStore nodeStore;

    @Before
    public void prepare() throws Exception {
        NodeStore source = getSourceContainer().open();
        try {
            initContent(source);
        } finally {
            getSourceContainer().close();
        }

        String[] args = getArgs();
        log.info("oak2oak {}", Joiner.on(' ').join(args));
        OakUpgrade.main(args);

        nodeStore = destination.open();
    }

    @After
    public void clean() throws IOException {
        IOUtils.closeQuietly(getDestinationContainer());
        getDestinationContainer().clean();
        getSourceContainer().clean();
    }

    public IncludeIndexTest() throws IOException {
        assumeTrue(isMongoAvailable());
        blob = new FileDataStoreContainer();
        source = new MongoNodeStoreContainer(blob);
        destination = new MongoNodeStoreContainer(blob);
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
        return new String[] { "--src-datastore", blob.getDescription(), "--copy-versions=false", "--skip-init", "--include-index", "--include-paths=/apps,/libs",  source.getDescription(), destination.getDescription() };
    }

    @Test
    public void validateMigration() throws RepositoryException, IOException {
        assertNodeExists("/oak:index/nodetype/:index/nt%3Afile/libs/sling/xss/config.xml");
        assertNodeExists("/oak:index/uuid/:index/" + getUuid("/apps/repl/components/repl/repl.html/jcr:content"));

        assertNodeMissing("/oak:index/nodetype/:index/nt%3Afile/sling.css");
        assertNodeMissing("/oak:index/uuid/:index/" + getUuid("/index.html/jcr:content"));
    }

    private String getUuid(String path) {
        return getNode(path).getString("jcr:uuid");
    }

    private void assertNodeMissing(String path) {
        assertFalse(getNode(path).exists());
    }

    private void assertNodeExists(String path) {
        assertTrue(getNode(path).exists());
    }

    private NodeState getNode(String path) {
        NodeState ns = nodeStore.getRoot();
        for (String element : PathUtils.elements(path)) {
            ns = ns.getChildNode(element);
        }
        return ns;
    }
}