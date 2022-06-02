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
package org.apache.jackrabbit.oak.benchmark.util;

import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexHelper;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public class ElasticGlobalInitializer implements RepositoryInitializer {

    private final String name;

    private final Set<String> propertyTypes;

    private final Set<String> excludes;

    private final String filePath;

    private String async = null;

    private Boolean storageEnabled;


    public ElasticGlobalInitializer(String name) {
        this(name, IndexHelper.JR_PROPERTY_INCLUDES, null, null, null);
    }

    public ElasticGlobalInitializer(String name, Boolean storageEnabled) {
        this(name, IndexHelper.JR_PROPERTY_INCLUDES, null, null, storageEnabled);
    }

    public ElasticGlobalInitializer(String name, Set<String> propertyTypes) {
        this(name, propertyTypes, null, null, null);
    }

    public ElasticGlobalInitializer(String name, Set<String> propertyTypes,
                                    Set<String> excludes) {
        this(name, propertyTypes, excludes, null, null);
    }

    public ElasticGlobalInitializer(String name, Set<String> propertyTypes,
                                    String filePath) {
        this(name, propertyTypes, null, filePath, null);
    }

    public ElasticGlobalInitializer(String name, Set<String> propertyTypes,
                                    Set<String> excludes, String filePath, Boolean storageEnabled) {
        this.name = name;
        this.propertyTypes = propertyTypes;
        this.excludes = excludes;
        this.filePath = filePath;
        this.storageEnabled = storageEnabled;
    }

    /**
     * set the {@code async} property to "async".
     *
     * @return
     */
    public ElasticGlobalInitializer async() {
        return async("async");
    }

    /**
     * will set the {@code async} property to the provided value
     *
     * @param async
     * @return
     */
    public ElasticGlobalInitializer async(@NotNull final String async) {
        this.async = checkNotNull(async);
        return this;
    }

    @Override
    public void initialize(@NotNull NodeBuilder builder) {
        if (builder.hasChildNode(INDEX_DEFINITIONS_NAME)
                && builder.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(this.name)) {
            // do nothing
        } else {
            IndexHelper.newFTIndexDefinition(IndexUtils.getOrCreateOakIndex(builder),
                    this.name, ElasticIndexDefinition.TYPE_ELASTICSEARCH,
                    propertyTypes, excludes, async, storageEnabled);
        }
    }
}
