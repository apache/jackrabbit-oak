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
package org.apache.jackrabbit.oak.composite.blueGreen;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import javax.jcr.Node;

import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests customized indexes.
 */
public class CustomizedIndexTest {

    private final Persistence.Config config = new Persistence.Config();

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder(new File("target"));

    private File globalDir;
    private File libs1Dir;
    private File libs2Dir;
    private File libs3Dir;
    private File datastoreDir;
    private File indexDir;

    @Test
    public void test() throws Exception {
        createFolders();
        config.blobStore = Persistence.getFileBlobStore(datastoreDir);
        config.indexDir = indexDir;
        initLibs1();
        initGlobal();
        compositeLibs1();
        initLibs2();
        compositeLibs2();
        compositeWithMergedIndex();
        initLibs3();
        compositeWithMergedIndexTwice();
    }

    private void initLibs1() throws Exception {
        Persistence p = Persistence.open(libs1Dir, config);
        p.session.getRootNode().addNode("libs").addNode("test").setProperty("foo", "a");
        p.session.save();
        IndexUtils.createIndex(p, "test-1", "foo", 10);
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo]",
                "test-1",
                "[/libs/test]");
        p.close();
    }

    private void initGlobal() throws Exception {
        Persistence p = Persistence.open(globalDir, config);
        p.session.getRootNode().addNode("content").addNode("test").setProperty("foo", "a");
        p.session.save();
        p.close();
    }

    private void compositeLibs1() throws Exception {
        Persistence p = Persistence.openComposite(globalDir, libs1Dir, config);
        IndexUtils.checkLibsIsReadOnly(p);
        IndexUtils.createIndex(p, "test-1", "foo", 10);
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-1",
                "[/content/test, /libs/test]");
        assertEquals("[/oak:index/lucene, /oak:index/test-1]",
                getActiveLuceneIndexes(p));
        p.close();
    }

    private void compositeLibs2() throws Exception {
        Persistence p = Persistence.openComposite(globalDir, libs2Dir, config);
        IndexUtils.checkLibsIsReadOnly(p);

        IndexUtils.createIndex(p, "test-2", "foo", 20);

        // the new index must be used in the new version (with libs2)
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-2",
                "[/content/test, /libs/test2]");
        assertEquals("[/oak:index/lucene, /oak:index/test-2]",
                getActiveLuceneIndexes(p));
        p.close();

        // the new index must not be used in the old version (with libs1)
        p = Persistence.openComposite(globalDir, libs1Dir, config);
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-1",
                "[/content/test, /libs/test]");
        assertEquals("[/oak:index/lucene, /oak:index/test-1]",
                getActiveLuceneIndexes(p));
        p.close();
    }

    private void compositeWithMergedIndex() throws Exception {
        // a new _merged_ index must be used by the new version (with libs2)
        // but it won't contain the libs part (as that's not indexed)
        Persistence p = Persistence.openComposite(globalDir, libs2Dir, config);
        Node n = IndexUtils.createIndex(p, "test-2-custom-1", "foo", 10);
        n.setProperty("merges", new String[]{"/oak:index/test-2"});
        n.getSession().save();
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-2-custom-1",
                "[/content/test]");
        // no let it point to a non-existing node: the index should be used
        n.setProperty("merges", new String[]{"/oak:index/test-does-not-exist"});
        n.getSession().save();
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-2-custom-1",
                "[/content/test]");
        assertEquals("[/oak:index/lucene, /oak:index/test-2-custom-1]",
                getActiveLuceneIndexes(p));
        p.close();

        // the merged index is not used in the old version (with libs1)
        // because the index test-2 is not active
        p = Persistence.openComposite(globalDir, libs1Dir, config);
        n = p.session.getNode("/oak:index/test-2-custom-1");
        n.setProperty("merges", new String[]{"/oak:index/test-2"});
        n.getSession().save();
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-1",
                "[/content/test, /libs/test]");
        // no let it point to a non-existing node: the index should be used
        n.setProperty("merges", new String[]{"/oak:index/test-does-not-exist"});
        n.getSession().save();
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-2-custom-1",
                "[/content/test]");
        assertEquals("[/oak:index/lucene, /oak:index/test-2-custom-1]",
                getActiveLuceneIndexes(p));
        p.close();
    }

    private void initLibs2() throws Exception {
        Persistence p = Persistence.open(libs2Dir, config);
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
        p.close();
    }

    private void initLibs3() throws Exception {
        Persistence p = Persistence.open(libs3Dir, config);
        p.session.getRootNode().addNode("libs").addNode("test3").setProperty("foo", "a");
        p.session.save();
        IndexUtils.createIndex(p, "test-3", "foo", 10);
        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo]",
                "test-3",
                "[/libs/test3]");
        p.close();
    }

    private void compositeWithMergedIndexTwice() throws Exception {
        // if it is merged again (without further customization),
        // then the newly merged index also needs to be used.

        Persistence p = Persistence.openComposite(globalDir, libs3Dir, config);
        IndexUtils.checkLibsIsReadOnly(p);

        Node n3 = IndexUtils.createIndex(p, "test-3", "foo", 10);
        n3.getSession().save();
        Node n31 = IndexUtils.createIndex(p, "test-3-custom-1", "foo", 10);
        n31.setProperty("merges", new String[]{"/oak:index/test-2-custom-1", "/oak:index/test-3"});
        n31.getSession().save();
        Node n21 = p.session.getNode("/oak:index/test-2-custom-1");
        n21.setProperty("merges", new String[]{"/oak:index/does-not-exist", "/oak:index/test-2"});
        n21.getSession().save();

        IndexUtils.assertQueryUsesIndexAndReturns(p,
                "/jcr:root//*[@foo] order by @jcr:path",
                "test-3-custom-1",
                "[/content/test]");
        assertEquals("[/oak:index/lucene, /oak:index/test-3-custom-1]",
                getActiveLuceneIndexes(p));
    }

    private void createFolders() throws IOException {
        globalDir = tempDir.newFolder("global");
        libs1Dir = tempDir.newFolder("libs1");
        libs2Dir = tempDir.newFolder("libs2");
        libs3Dir = tempDir.newFolder("libs3");
        datastoreDir = tempDir.newFolder("datastore");
        indexDir = tempDir.newFolder("index");
    }

    private static String getActiveLuceneIndexes(Persistence p) {
        return getActiveLuceneIndexes(p.getCompositeNodestore(), p.getMountInfoProvider());
    }

    private static String getActiveLuceneIndexes(NodeStore ns, MountInfoProvider m) {
        ArrayList<String> list = new ArrayList<>();
        IndexInfoServiceImpl indexService = new IndexInfoServiceImpl(ns,
                new IndexPathServiceImpl(ns, m));
        for (IndexInfo info : indexService.getAllIndexInfo()) {
            if (!LuceneIndexConstants.TYPE_LUCENE.equals(info.getType())) {
                continue;
            }
            if (!info.isActive()) {
                continue;
            }
            list.add(info.getIndexPath());
        }
        Collections.sort(list);
        return list.toString();
    }

}
