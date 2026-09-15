/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mongot;

import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;

import static com.mongodb.client.model.Filters.exists;

public final class MongotIndexStatistics implements IndexStatistics {

    private final MongoConnection connection;
    private final MongotIndexDefinition definition;

    MongotIndexStatistics(MongoConnection connection, MongotIndexDefinition definition) {
        this.connection = connection;
        this.definition = definition;
    }

    @Override
    public int numDocs() {
        return (int) Math.min(Integer.MAX_VALUE,
                connection.getCollection(definition).countDocuments());
    }

    @Override
    public int getDocCountFor(String key) {
        String field = FieldNames.NULL_PROPS.equals(key)
                ? MongoFieldNames.NULL_PROPERTIES
                : MongoFieldNames.TYPED + "." + MongoFieldNames.encodeProperty(key);
        return (int) Math.min(Integer.MAX_VALUE,
                connection.getCollection(definition).countDocuments(exists(field)));
    }
}
