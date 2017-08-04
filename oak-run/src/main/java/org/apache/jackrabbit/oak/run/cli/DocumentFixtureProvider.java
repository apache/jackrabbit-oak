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

package org.apache.jackrabbit.oak.run.cli;

import java.net.UnknownHostException;

import javax.sql.DataSource;

import com.google.common.io.Closer;
import com.mongodb.MongoClientURI;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getService;

class DocumentFixtureProvider {
    static NodeStore configureDocumentMk(Options options,
                                         BlobStore blobStore,
                                         Whiteboard wb,
                                         Closer closer,
                                         boolean readOnly) throws UnknownHostException {
        DocumentMK.Builder builder = new DocumentMK.Builder();
        StatisticsProvider statisticsProvider = checkNotNull(getService(wb, StatisticsProvider.class));

        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }

        DocumentNodeStoreOptions docStoreOpts = options.getOptionBean(DocumentNodeStoreOptions.class);

        builder.setClusterId(docStoreOpts.getClusterId());
        builder.setStatisticsProvider(statisticsProvider);
        if (readOnly) {
            builder.setReadOnlyMode();
        }

        int cacheSize = docStoreOpts.getCacheSize();
        if (cacheSize != 0) {
            builder.memoryCacheSize(cacheSize * FileUtils.ONE_MB);
        }

        if (docStoreOpts.disableBranchesSpec()) {
            builder.disableBranches();
        }

        CommonOptions commonOpts = options.getOptionBean(CommonOptions.class);

        if (docStoreOpts.isCacheDistributionDefined()){
            builder.memoryCacheDistribution(
                    docStoreOpts.getNodeCachePercentage(),
                    docStoreOpts.getPrevDocCachePercentage(),
                    docStoreOpts.getChildrenCachePercentage(),
                    docStoreOpts.getDiffCachePercentage()
            );
        }

        DocumentNodeStore dns;
        if (commonOpts.isMongo()) {
            MongoClientURI uri = new MongoClientURI(commonOpts.getStoreArg());
            if (uri.getDatabase() == null) {
                System.err.println("Database missing in MongoDB URI: "
                        + uri.getURI());
                System.exit(1);
            }
            MongoConnection mongo = new MongoConnection(uri.getURI());
            wb.register(MongoConnection.class, mongo, emptyMap());
            closer.register(mongo::close);
            builder.setMongoDB(mongo.getDB());
            dns = builder.getNodeStore();
            wb.register(MongoDocumentStore.class, (MongoDocumentStore) builder.getDocumentStore(), emptyMap());
        } else if (commonOpts.isRDB()) {
            RDBStoreOptions rdbOpts = options.getOptionBean(RDBStoreOptions.class);
            DataSource ds = RDBDataSourceFactory.forJdbcUrl(commonOpts.getStoreArg(),
                    rdbOpts.getUser(), rdbOpts.getPassword());
            wb.register(DataSource.class, ds, emptyMap());
            builder.setRDBConnection(ds);
            dns = builder.getNodeStore();
            wb.register(RDBDocumentStore.class, (RDBDocumentStore) builder.getDocumentStore(), emptyMap());
        } else {
            throw new IllegalStateException("Unknown DocumentStore");
        }

        closer.register(dns::dispose);

        return dns;
    }
}
