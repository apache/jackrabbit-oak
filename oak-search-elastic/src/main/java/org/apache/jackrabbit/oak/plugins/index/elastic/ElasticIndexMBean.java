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

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexMBean;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import java.util.Set;

/**
 * {@link IndexMBean} implementation to expose Elastic index details and utility functions.
 */
class ElasticIndexMBean implements IndexMBean {

    static final String TYPE = "ElasticIndex";

    private final ElasticIndexTracker indexTracker;

    ElasticIndexMBean(ElasticIndexTracker indexTracker) {
        this.indexTracker = indexTracker;
    }

    @Override
    public TabularData getIndexStats() {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(ElasticIndexMBean.class.getName(),
                    "Elastic Index Stats", ElasticMBeanConfig.TYPE, new String[]{"path"});
            tds = new TabularDataSupport(tt);
            Set<String> indexes = indexTracker.getIndexNodePaths();
            for (String path : indexes) {
                ElasticIndexNode indexNode = null;
                try {
                    indexNode = indexTracker.acquireIndexNode(path);
                    if (indexNode != null) {
                        long primaryStoreSize = indexNode.getIndexStatistics().primaryStoreSize();
                        long storeSize = indexNode.getIndexStatistics().storeSize();
                        Object[] values = new Object[]{
                                path,
                                IOUtils.humanReadableByteCount(primaryStoreSize),
                                primaryStoreSize,
                                IOUtils.humanReadableByteCount(storeSize),
                                storeSize,
                                indexNode.getIndexStatistics().numDocs(),
                                indexNode.getIndexStatistics().luceneNumDocs(),
                                indexNode.getIndexStatistics().luceneNumDeletedDocs()
                        };
                        tds.put(new CompositeDataSupport(ElasticMBeanConfig.TYPE, ElasticMBeanConfig.FIELD_NAMES, values));
                    }
                } finally {
                    if (indexNode != null) {
                        indexNode.release();
                    }
                }
            }
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    @Override
    public String getSize(String indexPath) {
        ElasticIndexNode indexNode = indexTracker.acquireIndexNode(indexPath);
        if (indexNode != null) {
            try {
                return String.valueOf(indexNode.getIndexStatistics().primaryStoreSize());
            } finally {
                indexNode.release();
            }
        }
        return null;
    }

    @Override
    public String getDocCount(String indexPath) {
        ElasticIndexNode indexNode = indexTracker.acquireIndexNode(indexPath);
        if (indexNode != null) {
            try {
                return String.valueOf(indexNode.getIndexStatistics().numDocs());
            } finally {
                indexNode.release();
            }
        }
        return null;
    }

    private static class ElasticMBeanConfig {

        static final String[] FIELD_NAMES = new String[]{
                "path",
                "indexSizeStr",
                "indexSize",
                "indexSizeWithReplicasStr",
                "indexSizeWithReplicas",
                "numDocs",
                "luceneNumDoc",
                "luceneNumDeletedDocs"
        };

        static final String[] FIELD_DESCRIPTIONS = new String[]{
                "Path",
                "Index size in human readable format",
                "Index size in bytes",
                "Index size, including replicas, in human readable format",
                "Index size, including replicas, in bytes",
                "Number of documents in this index",
                "Number of low-level lucene documents in this index, including nested ones",
                "Number of deleted low-level lucene documents in this index, including nested ones"
        };

        @SuppressWarnings("rawtypes")
        static final OpenType[] FIELD_TYPES = new OpenType[]{
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.LONG,
                SimpleType.STRING,
                SimpleType.LONG,
                SimpleType.INTEGER,
                SimpleType.INTEGER,
                SimpleType.INTEGER
        };

        static final CompositeType TYPE;

        static {
            try {
                TYPE = new CompositeType(
                        ElasticIndexMBean.class.getName(),
                        "Composite data type for Elastic Index statistics",
                        FIELD_NAMES,
                        FIELD_DESCRIPTIONS,
                        FIELD_TYPES);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
