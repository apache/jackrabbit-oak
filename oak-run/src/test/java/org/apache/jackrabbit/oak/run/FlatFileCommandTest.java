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
package org.apache.jackrabbit.oak.run;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;

import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class FlatFileCommandTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;

    private File tmpFlatfileOut;

    @BeforeClass
    public static void assumeMongoDB() {
        assumeTrue(MongoUtils.isAvailable());
    }

    private DocumentNodeStore createDocumentNodeStore() {
        MongoConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        MongoUtils.dropCollections(c.getDatabase());
        return builderProvider.newBuilder().setBlobStore(new MemoryBlobStore())
                .setMongoDB(c.getMongoClient(), c.getDBName()).getNodeStore();
    }

    @Before
    public void before() {
        ns = createDocumentNodeStore();
        tmpFlatfileOut = new File("tmp.flatfile.out");
        if (tmpFlatfileOut.exists()) {
            tmpFlatfileOut.delete();
        }
        assertFalse(tmpFlatfileOut.exists());
    }

    @After
    public void after() {
        if (tmpFlatfileOut != null && tmpFlatfileOut.exists()) {
            assertTrue("File should not now be deleted but is not : " + tmpFlatfileOut,
                    tmpFlatfileOut.delete());
        }
        tmpFlatfileOut = null;
        if (ns != null) {
            ns.dispose();
            ns = null;
        }
    }

    @Test
    public void flatfile() throws Exception {
        FlatFileCommand cmd = new FlatFileCommand();
        assertFalse("File should not exist but does : " + tmpFlatfileOut,
                tmpFlatfileOut.exists());
        cmd.execute(MongoUtils.URL, "--out",
                tmpFlatfileOut.getAbsolutePath(), "--fake-ds-path=.");
        assertTrue("File should exist but does not : " + tmpFlatfileOut,
                tmpFlatfileOut.exists());
        assertTrue(tmpFlatfileOut.exists());
    }

}