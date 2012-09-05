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
package org.apache.jackrabbit.oak.plugins.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.spi.query.Index;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyIndexFactory implements IndexFactory {

    public static final String TYPE_PROPERTY = "property";
    public static final String TYPE_PREFIX = "prefix";

    public static final String PROPERTY_NAME_PROPERTY = "property";
    public static final String PROPERTY_NAME_PREFIX = "prefix";

    private static final Logger LOG = LoggerFactory
            .getLogger(PropertyIndexFactory.class);

    private final ConcurrentHashMap<IndexDefinition, Index> indexes = new ConcurrentHashMap<IndexDefinition, Index>();

    private Indexer indexer;

    @Override
    public void init(MicroKernel mk) {
        this.indexer = new Indexer(mk);
    }

    @Override
    public String[] getTypes() {
        return new String[] { TYPE_PREFIX, TYPE_PROPERTY };
    }

    private Index createIndex(IndexDefinition indexDefinition) {
        if (TYPE_PREFIX.equals(indexDefinition.getType())) {
            String prefix = indexDefinition.getProperties().get(
                    PROPERTY_NAME_PREFIX);
            if (prefix != null) {
                return new PrefixIndex(indexer, prefix, indexDefinition);
            }
        }
        if (TYPE_PROPERTY.equals(indexDefinition.getType())) {
            String name = indexDefinition.getProperties().get(
                    PROPERTY_NAME_PROPERTY);
            if (name != null) {
                return new PropertyIndex(indexer, name,
                        indexDefinition.isUnique(), indexDefinition);
            }
        }
        return null;
    }

    @Override
    public Index getIndex(IndexDefinition indexDefinition) {
        Index index = indexes.get(indexDefinition);
        if (index == null) {
            index = createIndex(indexDefinition);
            indexes.put(indexDefinition, index);
        }
        return index;
    }

    @Override
    public void close() throws IOException {
        Iterator<IndexDefinition> iterator = indexes.keySet().iterator();
        while (iterator.hasNext()) {
            IndexDefinition id = iterator.next();
            try {
                indexes.get(id).close();
            } catch (IOException e) {
                LOG.error("Error closing index {}.", id.getName(), e);
            }
            iterator.remove();
        }
    }

    @Override
    public String toString() {
        return "PropertyIndexFactory [getTypes()="
                + Arrays.toString(getTypes()) + "]";
    }

}
