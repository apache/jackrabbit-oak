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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneFileIndexDefinition;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class LuceneInitializerHelper implements RepositoryInitializer {

    private final String name;

    private final Set<String> propertyTypes;

    private final Set<String> excludes;

    private final String filePath;

    private String async = null;

    private Boolean storageEnabled;

    public LuceneInitializerHelper(String name) {
        this(name, LuceneIndexHelper.JR_PROPERTY_INCLUDES, null, null, null);
    }

    public LuceneInitializerHelper(String name, Boolean storageEnabled) {
        this(name, LuceneIndexHelper.JR_PROPERTY_INCLUDES, null, null, storageEnabled);
    }

    public LuceneInitializerHelper(String name, Set<String> propertyTypes) {
        this(name, propertyTypes, null, null, null);
    }

    public LuceneInitializerHelper(String name, Set<String> propertyTypes,
            Set<String> excludes) {
        this(name, propertyTypes, excludes, null, null);
    }

    public LuceneInitializerHelper(String name, Set<String> propertyTypes,
            String filePath) {
        this(name, propertyTypes, null, filePath, null);
    }

    public LuceneInitializerHelper(String name, Set<String> propertyTypes,
            Set<String> excludes, String filePath, Boolean storageEnabled) {
        this.name = name;
        this.propertyTypes = propertyTypes;
        this.excludes = excludes;
        this.filePath = filePath;
        this.storageEnabled = storageEnabled;
    }

    /**
     * set the {@code async} property to "async".
     * @return
     */
    public LuceneInitializerHelper async() {
        return async("async");
    }

    /**
     * will set the {@code async} property to the provided value
     * 
     * @param async
     * @return
     */
    public LuceneInitializerHelper async(@Nonnull final String async) {
        this.async = checkNotNull(async);
        return this;
    }
    
    @Override
    public void initialize(@Nonnull NodeBuilder builder) {
        if (builder.hasChildNode(INDEX_DEFINITIONS_NAME)
                && builder.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name)) {
            // do nothing
        } else if (filePath == null) {
            newLuceneIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                    name, propertyTypes, excludes, async, storageEnabled);
        } else {
            newLuceneFileIndexDefinition(
                    builder.child(INDEX_DEFINITIONS_NAME),
                    name, propertyTypes, excludes, filePath, async);
        }
    }

}
