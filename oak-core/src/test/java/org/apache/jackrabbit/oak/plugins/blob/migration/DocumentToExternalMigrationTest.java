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

package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assume;
import org.junit.BeforeClass;

public class DocumentToExternalMigrationTest extends AbstractMigratorTest {

    private DocumentNodeStore nodeStore;;

    @Override
    protected NodeStore createNodeStore(BlobStore blobStore, File repository) throws IOException {
        MongoConnection connection = MongoUtils.getConnection();
        Assume.assumeNotNull(connection);
        DocumentMK.Builder builder = new DocumentMK.Builder();
        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }
        builder.setMongoDB(connection.getDB());
        return nodeStore = builder.getNodeStore();
    }

    @Override
    protected void closeNodeStore() {
        if (nodeStore != null) {
            nodeStore.dispose();
            nodeStore = null;
        }
    }
    
    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeNotNull(MongoUtils.getConnection());
    }

    @Override
    protected BlobStore createOldBlobStore(File repository) {
        MongoConnection connection = MongoUtils.getConnection();
        return new MongoBlobStore(connection.getDB());
    }

    @Override
    protected BlobStore createNewBlobStore(File repository) {
        return new FileBlobStore(repository.getPath() + "/new");
    }

}
