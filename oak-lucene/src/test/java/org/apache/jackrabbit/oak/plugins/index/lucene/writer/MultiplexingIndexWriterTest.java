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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.plugins.blob.datastore.CachingFileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DefaultDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.newDoc;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MultiplexingIndexWriterTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();
    private LuceneIndexDefinition defn = new LuceneIndexDefinition(root, builder.getNodeState(), "/foo");
    private MountInfoProvider mip = Mounts.newBuilder()
            .mount("foo", "/libs", "/apps")
            .readOnlyMount("ro", "/ro-tree")
            .build();

    private Mount fooMount;
    private Mount roMount;
    private Mount defaultMount;
    private LuceneIndexWriterConfig writerConfig = new LuceneIndexWriterConfig();

    @Before
    public void setUp(){
        initializeMounts();
    }

    @Test
    public void defaultWriterWithNoMounts() throws Exception{
        LuceneIndexWriterFactory factory = newDirectoryFactory(Mounts.defaultMountInfoProvider());
        LuceneIndexWriter writer = factory.newInstance(defn, builder, null, true);
        assertThat(writer, instanceOf(DefaultIndexWriter.class));
    }

    @Test
    public void closeWithoutChange() throws Exception{
        LuceneIndexWriterFactory factory = newDirectoryFactory();
        LuceneIndexWriter writer = factory.newInstance(defn, builder, null, true);
        assertTrue(writer.close(0));
        assertEquals(2, Iterables.size(getIndexDirNodes()));
        assertFalse(builder.hasChildNode(indexDirName(roMount)));
        assertEquals(0, numDocs(fooMount));
        assertEquals(0, numDocs(defaultMount));

        // delete all dir nodes first
        getIndexDirNodes().forEach(dirName -> builder.getChildNode(dirName).remove());

        // empty index dir doesn't get created during normal indexing
        writer = factory.newInstance(defn, builder, null, false);
        assertFalse(writer.close(0));
        assertEquals(0, Iterables.size(getIndexDirNodes()));
    }

    @Test
    public void writesInDefaultMount() throws Exception{
        LuceneIndexWriterFactory factory = newDirectoryFactory();
        LuceneIndexWriter writer = factory.newInstance(defn, builder, null, true);


        //1. Add entry in foo mount
        writer.updateDocument("/libs/config", newDoc("/libs/config"));
        writer.close(0);

        // only mount foo must have indexed document
        assertEquals(1, numDocs(fooMount));
        assertEquals(0, numDocs(defaultMount));

        //2. Add entry in default mount
        writer = factory.newInstance(defn, builder, null, true);
        writer.updateDocument("/content", newDoc("/content"));
        writer.close(0);


        // both mounts must have 1 document each
        assertEquals(1, numDocs(fooMount));
        assertEquals(1, numDocs(defaultMount));

        //3. Add another entry in foo mount without reindexing
        writer = factory.newInstance(defn, builder, null, false);
        writer.updateDocument("/libs/config1", newDoc("/libs/config1"));
        writer.close(0);

        // only mount foo must have indexed document
        assertEquals(2, numDocs(fooMount));
        assertEquals(1, numDocs(defaultMount));

        //4. Add another entry in default mount without reindexing
        writer = factory.newInstance(defn, builder, null, false);
        writer.updateDocument("/content1", newDoc("/content1"));
        writer.close(0);


        // both mounts must have 1 document each
        assertEquals(2, numDocs(fooMount));
        assertEquals(2, numDocs(defaultMount));
    }

    @Test
    public void writesInDefaultMountBlobStore() throws Exception {
        CachingFileDataStore ds = DataStoreUtils
                .createCachingFDS(folder.newFolder().getAbsolutePath(),
                        folder.newFolder().getAbsolutePath());

        DirectoryFactory directoryFactory = new DefaultDirectoryFactory(null, new DataStoreBlobStore(ds));
        LuceneIndexWriterFactory factory = new DefaultIndexWriterFactory(mip, directoryFactory, writerConfig);
        LuceneIndexWriter writer = factory.newInstance(defn, builder, null, true);

        //1. Add entry in foo mount
        writer.updateDocument("/libs/config", newDoc("/libs/config"));
        writer.close(0);

        // only mount foo must have indexed document
        assertEquals(1, numDocs(fooMount));
        assertEquals(0, numDocs(defaultMount));

        //2. Add entry in default mount
        writer = factory.newInstance(defn, builder, null, true);
        writer.updateDocument("/content", newDoc("/content"));
        writer.close(0);


        // both mounts must have 1 document each
        assertEquals(1, numDocs(fooMount));
        assertEquals(1, numDocs(defaultMount));

        //3. Add another entry in foo mount without reindexing
        writer = factory.newInstance(defn, builder, null, false);
        writer.updateDocument("/libs/config1", newDoc("/libs/config1"));
        writer.close(0);

        // only mount foo must have indexed document
        assertEquals(2, numDocs(fooMount));
        assertEquals(1, numDocs(defaultMount));

        //4. Add another entry in default mount without reindexing
        writer = factory.newInstance(defn, builder, null, false);
        writer.updateDocument("/content1", newDoc("/content1"));
        writer.close(0);


        // both mounts must have 1 document each
        assertEquals(2, numDocs(fooMount));
        assertEquals(2, numDocs(defaultMount));
    }

    @Test
    public void deletes() throws Exception{
        LuceneIndexWriterFactory factory = newDirectoryFactory();
        LuceneIndexWriter writer = factory.newInstance(defn, builder, null, true);

        writer.updateDocument("/libs/config", newDoc("/libs/config"));
        writer.updateDocument("/libs/install", newDoc("/libs/install"));
        writer.updateDocument("/content", newDoc("/content"));
        writer.updateDocument("/content/en", newDoc("/content/en"));
        writer.close(0);

        assertEquals(2, numDocs(fooMount));
        assertEquals(2, numDocs(defaultMount));

        writer = factory.newInstance(defn, builder, null, true);
        writer.deleteDocuments("/libs/config");
        writer.close(0);

        assertEquals(1, numDocs(fooMount));
        assertEquals(2, numDocs(defaultMount));

        writer = factory.newInstance(defn, builder, null, true);
        writer.deleteDocuments("/content");
        writer.close(0);

        assertEquals(1, numDocs(fooMount));
        assertEquals(0, numDocs(defaultMount));
    }

    @Test
    public void deleteIncludingMount() throws Exception{
        mip = Mounts.newBuilder()
                .mount("foo", "/content/remote").build();
        initializeMounts();
        LuceneIndexWriterFactory factory = newDirectoryFactory();
        LuceneIndexWriter writer = factory.newInstance(defn, builder, null, true);

        writer.updateDocument("/content/remote/a", newDoc("/content/remote/a"));
        writer.updateDocument("/etc", newDoc("/etc"));
        writer.updateDocument("/content", newDoc("/content"));
        writer.close(0);

        assertEquals(1, numDocs(fooMount));
        assertEquals(2, numDocs(defaultMount));

        writer = factory.newInstance(defn, builder, null, true);
        writer.deleteDocuments("/content");
        writer.close(0);

        assertEquals(0, numDocs(fooMount));
        assertEquals(1, numDocs(defaultMount));

    }

    private void initializeMounts() {
        fooMount = mip.getMountByName("foo");
        roMount = mip.getMountByName("ro");
        defaultMount = mip.getDefaultMount();
    }

    private int numDocs(Mount m) throws IOException {
        String indexDirName = indexDirName(m);
        Directory d = new OakDirectory(builder, indexDirName, defn, true);
        IndexReader r = DirectoryReader.open(d);
        return r.numDocs();
    }

    private List<String> getIndexDirNodes(){
        List<String> names = new ArrayList<>();
        for (String name : builder.getChildNodeNames()){
            if (MultiplexersLucene.isIndexDirName(name)){
                names.add(name);
            }
        }
        return names;
    }

    private void deleteIndexDirNodes(){
        for (String name : builder.getChildNodeNames()) {
            if (MultiplexersLucene.isIndexDirName(name)){
                builder.getChildNode(name).remove();
            }
        }
    }

    private String indexDirName(Mount m){
        return MultiplexersLucene.getIndexDirName(m);
    }

    private LuceneIndexWriterFactory newDirectoryFactory(){
        return newDirectoryFactory(mip);
    }

    private LuceneIndexWriterFactory newDirectoryFactory(MountInfoProvider mountInfoProvider){
        DirectoryFactory directoryFactory = new DefaultDirectoryFactory(null, null);
        return new DefaultIndexWriterFactory(mountInfoProvider, directoryFactory, writerConfig);
    }

}
