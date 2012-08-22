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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.spi.query.Index;
import org.apache.jackrabbit.oak.spi.query.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.IndexFactory;

public class PropertyIndexFactory implements IndexFactory {

    public static final String TYPE_PROPERTY = "property";
    public static final String TYPE_PREFIX = "prefix";

    private Indexer indexer;

    @Override
    public void init(MicroKernel mk) {
        this.indexer = new Indexer(mk);
    }

    @Override
    public String[] getTypes() {
        return new String[] { TYPE_PREFIX, TYPE_PROPERTY };
    }

    @Override
    public Index createIndex(IndexDefinition indexDefinition) {
        if (TYPE_PREFIX.equals(indexDefinition.getType())) {
            String prefix = indexDefinition.getProperties().get("prefix");
            if (prefix != null) {
                return new PrefixIndex(indexer, prefix, indexDefinition);
            }
            return null;
        }
        if (TYPE_PROPERTY.equals(indexDefinition.getType())) {
            String name = indexDefinition.getProperties().get("pname");
            if (name != null) {
                return new PropertyIndex(indexer, name,
                        indexDefinition.isUnique(), indexDefinition);
            }
        }
        return null;
    }

}
