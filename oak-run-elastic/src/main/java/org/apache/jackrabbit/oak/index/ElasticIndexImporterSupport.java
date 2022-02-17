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
package org.apache.jackrabbit.oak.index;

import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexImporter;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.Closeable;
import java.io.IOException;

public class ElasticIndexImporterSupport extends IndexImporterSupportBase implements Closeable {
    private final String indexPrefix;
    private final String scheme;
    private final String host;
    private final int port;
    private final String apiKeyId;
    private final String apiSecretId;
    protected final Closer closer = Closer.create();

    public ElasticIndexImporterSupport(IndexHelper indexHelper ,String indexPrefix, String scheme,
                                       String host, int port,
                                       String apiKeyId, String apiSecretId) {
        super(indexHelper);
        this.indexPrefix = indexPrefix;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.apiKeyId = apiKeyId;
        this.apiSecretId = apiSecretId;
    }

    @Override
    protected IndexEditorProvider createIndexEditorProvider() {
        MountInfoProvider mip = indexHelper.getMountInfoProvider();
        return new CompositeIndexEditorProvider(
                createElasticEditorProvider(),
                new PropertyIndexEditorProvider().with(mip),
                new ReferenceEditorProvider().with(mip)
        );
    }

    @Override
    protected void addImportProviders(IndexImporter importer) {
        importer.addImporterProvider(new ElasticIndexImporter());
    }

    private IndexEditorProvider createElasticEditorProvider() {
        final ElasticConnection.Builder.BuildStep buildStep = ElasticConnection.newBuilder()
                .withIndexPrefix(indexPrefix)
                .withConnectionParameters(
                        scheme,
                        host,
                        port
                );
        final ElasticConnection connection;
        if (apiKeyId != null && apiSecretId != null) {
            connection = buildStep.withApiKeys(apiKeyId, apiSecretId).build();
        } else {
            connection = buildStep.build();
        }
        closer.register(connection);
        ElasticIndexTracker indexTracker = new ElasticIndexTracker(connection,
                new ElasticMetricHandler(StatisticsProvider.NOOP));
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(indexTracker, connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        return editorProvider;
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }
}
