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

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_FILE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PERSISTENCE_PATH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.collect.ImmutableSet;

public class LuceneIndexHelper {

    public static final Set<String> JR_PROPERTY_INCLUDES = ImmutableSet.of(
            PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_BINARY);

    private LuceneIndexHelper() {
    }

    public static NodeBuilder newLuceneIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes) {
        return newLuceneIndexDefinition(index, name, propertyTypes, null, null);
    }

    public static NodeBuilder newLuceneIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @Nullable String async) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            index.setProperty(createProperty(INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        if (excludes != null && !excludes.isEmpty()) {
            index.setProperty(createProperty(EXCLUDE_PROPERTY_NAMES, excludes,
                    STRINGS));
        }
        return index;
    }

    public static NodeBuilder newLuceneFileIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes, @Nonnull String path) {
        return newLuceneFileIndexDefinition(index, name, propertyTypes, null,
                path, null);
    }

    public static NodeBuilder newLuceneFileIndexDefinition(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes,
            @Nullable Set<String> excludes, @Nonnull String path,
            @Nullable String async) {
        if (index.hasChildNode(name)) {
            return index.child(name);
        }
        index = index.child(name);
        index.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, TYPE_LUCENE)
                .setProperty(PERSISTENCE_NAME, PERSISTENCE_FILE)
                .setProperty(PERSISTENCE_PATH, path)
                .setProperty(REINDEX_PROPERTY_NAME, true);
        if (async != null) {
            index.setProperty(ASYNC_PROPERTY_NAME, async);
        }
        if (propertyTypes != null && !propertyTypes.isEmpty()) {
            index.setProperty(createProperty(INCLUDE_PROPERTY_TYPES,
                    propertyTypes, STRINGS));
        }
        if (excludes != null && !excludes.isEmpty()) {
            index.setProperty(createProperty(EXCLUDE_PROPERTY_NAMES, excludes,
                    STRINGS));
        }
        return index;
    }
}
