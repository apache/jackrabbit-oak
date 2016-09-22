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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

public class JdbcFactory implements NodeStoreFactory {

    private static final Logger log = LoggerFactory.getLogger(JdbcFactory.class);

    private final String jdbcUri;

    private final int cacheSize;

    private final String user;

    private final String password;

    public JdbcFactory(String jdbcUri, int cacheSize, String user, String password) {
        this.jdbcUri = jdbcUri;
        this.cacheSize = cacheSize;
        if (user == null || password == null) {
            throw new IllegalArgumentException("RBD requires username and password parameters.");
        }
        this.user = user;
        this.password = password;
    }

    @Override
    public NodeStore create(BlobStore blobStore, Closer closer) {
        DocumentMK.Builder builder = MongoFactory.getBuilder(cacheSize);
        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }
        builder.setRDBConnection(getDataSource(closer));
        log.info("Initialized DocumentNodeStore on RDB with Cache size : {} MB, Fast migration : {}", cacheSize,
                builder.isDisableBranches());
        DocumentNodeStore documentNodeStore = builder.getNodeStore();
        closer.register(MongoFactory.asCloseable(documentNodeStore));
        return documentNodeStore;
    }

    private DataSource getDataSource(Closer closer) {
        DataSource ds = RDBDataSourceFactory.forJdbcUrl(jdbcUri, user, password);
        if (ds instanceof Closeable) {
            closer.register((Closeable)ds);
        }
        return ds;
    }

    @Override
    public boolean hasExternalBlobReferences() throws IOException {
        Closer closer = Closer.create();
        try {
            DataSource ds = getDataSource(closer);
            RDBBlobStore blobStore = new RDBBlobStore(ds);
            return !blobStore.getAllChunkIds(0).hasNext();
        } catch(Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    @Override
    public String toString() {
        return String.format("DocumentNodeStore[%s]", jdbcUri);
    }
}
