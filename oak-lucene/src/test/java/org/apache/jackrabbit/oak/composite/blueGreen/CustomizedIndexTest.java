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

import java.io.File;
import java.io.IOException;

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
        p.close();
    }
    
    private void compositeLibs2() throws Exception {
        Persistence p = Persistence.openComposite(globalDir, libs2Dir, config);
        IndexUtils.checkLibsIsReadOnly(p);
        IndexUtils.createIndex(p, "test-2", "foo", 20);
        IndexUtils.assertQueryUsesIndexAndReturns(p, 
                "/jcr:root//*[@foo] order by @jcr:path", 
                "test-2", 
                "[/content/test, /libs/test2]");
        p.close();
        // the new index must not be used in the old version (wiht libs1)
        p = Persistence.openComposite(globalDir, libs1Dir, config);
        IndexUtils.assertQueryUsesIndexAndReturns(p, 
                "/jcr:root//*[@foo] order by @jcr:path", 
                "test-1", 
                "[/content/test, /libs/test]");
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
    
    private void createFolders() throws IOException {
        globalDir = tempDir.newFolder("global");
        libs1Dir = tempDir.newFolder("libs1");
        libs2Dir = tempDir.newFolder("libs2");
        datastoreDir = tempDir.newFolder("datastore");
        indexDir = tempDir.newFolder("index");
    }
    
}
