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
package org.apache.jackrabbit.oak.index.async;

import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.ElasticIndexOptions;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.util.List;

public class AsyncIndexerElastic extends AsyncIndexerBase {

    private final ElasticIndexOptions indexOpts;
    public AsyncIndexerElastic(IndexHelper indexHelper, Closer closer, List<String> names,
                               long delay, ElasticIndexOptions indexOpts) {
        super(indexHelper, closer, names, delay);
        this.indexOpts = indexOpts;
    }

    @Override
    public IndexEditorProvider getIndexEditorProvider() {
        final ElasticConnection.Builder.BuildStep buildStep = ElasticConnection.newBuilder()
                .withIndexPrefix(indexOpts.getIndexPrefix())
                .withConnectionParameters(
                        indexOpts.getElasticScheme(),
                        indexOpts.getElasticHost(),
                        indexOpts.getElasticPort()
                );
        final ElasticConnection connection;
        if (indexOpts.getApiKeyId() != null && indexOpts.getApiKeySecret() != null) {
            connection = buildStep.withApiKeys(indexOpts.getApiKeyId(), indexOpts.getApiKeySecret()).build();
        } else {
            connection = buildStep.build();
        }
        closer.register(connection);

        ElasticIndexTracker indexTracker = new ElasticIndexTracker(connection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        return new ElasticIndexEditorProvider(indexTracker, connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
    }
}
