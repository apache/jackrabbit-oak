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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.plugins.index.search.IndexMBean;

import javax.management.openmbean.TabularData;
import java.io.IOException;

public class ElasticIndexMBean implements IndexMBean {

    private final ElasticConnection elasticConnection;
    private final ElasticIndexTracker indexTracker;

    public ElasticIndexMBean(ElasticConnection elasticConnection, ElasticIndexTracker indexTracker) {
        this.elasticConnection = elasticConnection;
        this.indexTracker = indexTracker;
    }

    @Override
    public TabularData getIndexStats() throws IOException {
        return null;
    }

    @Override
    public String getSize(String indexPath) throws IOException {
        return null;
    }

    @Override
    public String getDocCount(String indexPath) throws IOException {
        return null;
    }
}
