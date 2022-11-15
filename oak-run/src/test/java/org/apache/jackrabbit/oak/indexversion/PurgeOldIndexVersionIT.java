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
package org.apache.jackrabbit.oak.indexversion;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.composite.blueGreen.IndexUtils;
import org.apache.jackrabbit.oak.composite.blueGreen.Persistence;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class PurgeOldIndexVersionIT {

    private final Persistence.Config config = new Persistence.Config();

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder(new File("target"));

    private File globalDir;
    private File libsDir;
    private File datastoreDir;
    private File indexDir;
    private LogCustomizer purgeOldIndexVersionLogger;

    @Before
    public void setup() {
        purgeOldIndexVersionLogger = LogCustomizer.forLogger(PurgeOldIndexVersion.class.getName())
                .enable(Level.INFO).contains("Found some index need to be purged over base").create();
    }

    @After
    public void teardown() {
        purgeOldIndexVersionLogger.finished();
    }

    @Test
    public void test() throws Exception {
        createFolders();
        config.blobStore = Persistence.getFileBlobStore(datastoreDir);
        config.indexDir = indexDir;
        initGlobal();
        initLibs();
        compositeLibs();

        purgeOldIndexVersionLogger.starting();
        PurgeOldIndexVersion purgeOldIndexVersion = new PurgeOldIndexVersion();
        Persistence p = Persistence.openComposite(globalDir, libsDir, config);
        NodeStore n = p.getCompositeNodestore();
        purgeOldIndexVersion.execute(n, true, 1, Arrays.asList("/oak:index"));

        String purgeLog = "Found some index need to be purged over base'/oak:index/test': '[" +
                "/oak:index/test-2 base=/oak:index/test versioned product=2 custom=0 operation:DELETE_HIDDEN_AND_DISABLE," +
                " /oak:index/test-1 base=/oak:index/test versioned product=1 custom=0 operation:DELETE_HIDDEN_AND_DISABLE]'";
        Assert.assertEquals(1, purgeOldIndexVersionLogger.getLogs().size());
        Assert.assertEquals(purgeLog, purgeOldIndexVersionLogger.getLogs().get(0));
        Assert.assertTrue(IndexUtils.isIndexDisabledAndHiddenNodesDeleted(n, "/oak:index/test-1"));
        Assert.assertTrue(IndexUtils.isIndexDisabledAndHiddenNodesDeleted(n, "/oak:index/test-2"));
        Assert.assertTrue(IndexUtils.isIndexEnabledAndHiddenNodesPresent(n, "/oak:index/test-2-custom-1"));
    }

    @Test
    public void testDonotPurgeBaseIndex() throws Exception {
        createFolders();
        config.blobStore = Persistence.getFileBlobStore(datastoreDir);
        config.indexDir = indexDir;
        initGlobal();
        initLibs();
        compositeLibs();

        purgeOldIndexVersionLogger.starting();
        PurgeOldIndexVersion purgeOldIndexVersion = new PurgeOldIndexVersion();
        Persistence p = Persistence.openComposite(globalDir, libsDir, config);
        NodeStore n = p.getCompositeNodestore();
        purgeOldIndexVersion.execute(n, true, 1, Arrays.asList("/oak:index"), false);

        String purgeLog = "Found some index need to be purged over base'/oak:index/test': '[" +
                "/oak:index/test-1 base=/oak:index/test versioned product=1 custom=0 operation:DELETE_HIDDEN_AND_DISABLE]'";
        Assert.assertEquals(1, purgeOldIndexVersionLogger.getLogs().size());
        Assert.assertEquals(purgeLog, purgeOldIndexVersionLogger.getLogs().get(0));
        Assert.assertTrue(IndexUtils.isIndexDisabledAndHiddenNodesDeleted(n, "/oak:index/test-1"));
        Assert.assertTrue(IndexUtils.isIndexEnabledAndHiddenNodesPresent(n, "/oak:index/test-2"));
        Assert.assertTrue(IndexUtils.isIndexEnabledAndHiddenNodesPresent(n, "/oak:index/test-2-custom-1"));
    }

    private void initGlobal() throws Exception {
        Persistence p = Persistence.open(globalDir, config);
        p.session.getRootNode().addNode("content").addNode("test").setProperty("foo", "a");
        p.session.save();
        p.close();
    }

    private void initLibs() throws Exception {
        Persistence p = Persistence.open(libsDir, config);
        p.session.getRootNode().addNode("libs").addNode("test2").setProperty("foo", "a");
        p.session.save();
        IndexUtils.createIndex(p, "test-1", "foo", 10);
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo]",
                "test-1",
                "[/libs/test2]");
        IndexUtils.createIndex(p, "test-2", "foo", 20);
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo]",
                "test-2",
                "[/libs/test2]");
        IndexUtils.createIndex(p, "test-2-custom-1", "foo", 30);
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo]",
                "test-2-custom-1",
                "[/libs/test2]");
        p.close();
    }

    private void compositeLibs() throws Exception {
        Persistence p = Persistence.openComposite(globalDir, libsDir, config);
        IndexUtils.checkLibsIsReadOnly(p);

        IndexUtils.createIndex(p, "test-1", "foo", 10);
        IndexUtils.createIndex(p, "test-2", "foo", 20);
        IndexUtils.createIndex(p, "test-2-custom-1", "foo", 30);

        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-2-custom-1",
                "[/content/test, /libs/test2]");
        p.close();
    }

    private void createFolders() throws IOException {
        globalDir = tempDir.newFolder("global");
        libsDir = tempDir.newFolder("libs");
        datastoreDir = tempDir.newFolder("datastore");
        indexDir = tempDir.newFolder("index");
    }

}
