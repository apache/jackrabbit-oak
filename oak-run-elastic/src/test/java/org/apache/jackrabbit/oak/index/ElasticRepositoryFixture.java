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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.File;

public class ElasticRepositoryFixture extends IndexRepositoryFixture {

    private final ElasticConnection connection;

    public ElasticRepositoryFixture(File dir, ElasticConnection connection) {
        super(dir);
        this.connection = connection;
    }

    @Override
    protected void configureIndexProvider(Oak oak) {
        ElasticIndexTracker tracker = new ElasticIndexTracker(connection, new ElasticMetricHandler(StatisticsProvider.NOOP));

        ElasticIndexEditorProvider ep = new ElasticIndexEditorProvider(tracker, connection,new ExtractedTextCache(0,0));
        ElasticIndexProvider provider = new ElasticIndexProvider(tracker);
        oak.with(provider)
                .with(ep);
    }
}
