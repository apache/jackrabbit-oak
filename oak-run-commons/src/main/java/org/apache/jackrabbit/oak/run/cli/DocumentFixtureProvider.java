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

import java.io.IOException;

import javax.sql.DataSource;

import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.guava.common.io.Closer;
import com.mongodb.MongoClientURI;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.getService;

class DocumentFixtureProvider {
    static DocumentNodeStore configureDocumentMk(Options options,
                                         BlobStore blobStore,
                                         Whiteboard wb,
                                         Closer closer,
                                         boolean readOnly) throws IOException {
        CommonOptions commonOpts = options.getOptionBean(CommonOptions.class);

        DocumentNodeStoreBuilder<?> builder;
        if (commonOpts.isMongo()) {
            builder = newMongoDocumentNodeStoreBuilder();
        } else if (commonOpts.isRDB()) {
            builder = newRDBDocumentNodeStoreBuilder();
        } else {
            throw new IllegalStateException("Unknown DocumentStore");
        }

        StatisticsProvider statisticsProvider = requireNonNull(getService(wb, StatisticsProvider.class));

        DocumentBuilderCustomizer customizer = getService(wb, DocumentBuilderCustomizer.class);
        if (customizer != null) {
            customizer.customize(builder);
        }

        if (blobStore != null) {
            builder.setBlobStore(blobStore);
        }

        DocumentNodeStoreOptions docStoreOpts = options.getOptionBean(DocumentNodeStoreOptions.class);

        builder.setClusterId(docStoreOpts.getClusterId());
        builder.setStatisticsProvider(statisticsProvider);
        if (readOnly) {
            builder.setReadOnlyMode();
        }
        builder.setClusterInvisible(true);

        int cacheSize = docStoreOpts.getCacheSize();
        if (cacheSize != 0) {
            builder.memoryCacheSize(cacheSize * FileUtils.ONE_MB);
        }

        if (docStoreOpts.disableBranchesSpec()) {
            builder.disableBranches();
        }

        if (docStoreOpts.isCacheDistributionDefined()){
            builder.memoryCacheDistribution(
                    docStoreOpts.getNodeCachePercentage(),
                    docStoreOpts.getPrevDocCachePercentage(),
                    docStoreOpts.getChildrenCachePercentage(),
                    docStoreOpts.getDiffCachePercentage(),
                    docStoreOpts.getPrevNoPropCachePercentage()
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
            wb.register(MongoClientURI.class, uri, emptyMap());
            wb.register(MongoConnection.class, mongo, emptyMap());
            wb.register(MongoDatabase.class, mongo.getDatabase(), emptyMap());
            closer.register(mongo::close);
            ((MongoDocumentNodeStoreBuilder) builder).setMongoDB(mongo.getMongoClient(), mongo.getDBName());
            dns = builder.build();
            wb.register(MongoDocumentStore.class, (MongoDocumentStore) builder.getDocumentStore(), emptyMap());
        } else if (commonOpts.isRDB()) {
            RDBStoreOptions rdbOpts = options.getOptionBean(RDBStoreOptions.class);
            DataSource ds = RDBDataSourceFactory.forJdbcUrl(commonOpts.getStoreArg(),
                    rdbOpts.getUser(), rdbOpts.getPassword());
            wb.register(DataSource.class, ds, emptyMap());
            ((RDBDocumentNodeStoreBuilder) builder).setRDBConnection(ds);
            dns = builder.build();
            wb.register(RDBDocumentStore.class, (RDBDocumentStore) builder.getDocumentStore(), emptyMap());
        } else {
            throw new IllegalStateException("Unknown DocumentStore");
        }

        closer.register(dns::dispose);

        return dns;
    }
}
