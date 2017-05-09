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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.Set;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Provides static utility methods for MongoDB.
 */
class MongoUtils {

    /**
     * Forces creation of an index on a field, if one does not already exist.
     *
     * @param collection the collection.
     * @param field the name of the field.
     * @param ascending {@code true} for an ascending, {@code false} for a
     *                              descending index.
     * @param unique whether values are unique.
     * @param sparse whether the index should be sparse.
     * @throws MongoException if the operation fails.
     */
    static void createIndex(DBCollection collection,
                            String field,
                            boolean ascending,
                            boolean unique,
                            boolean sparse)
            throws MongoException {
        createIndex(collection, new String[]{field},
                new boolean[]{ascending}, unique, sparse);
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already
     * exist.
     *
     * @param collection the collection.
     * @param fields the name of the fields.
     * @param ascending {@code true} for an ascending, {@code false} for a
     *                              descending index.
     * @param unique whether values are unique.
     * @param sparse whether the index should be sparse.
     * @throws IllegalArgumentException if {@code fields} and {@code ascending}
     *          arrays have different lengths.
     * @throws MongoException if the operation fails.
     */
    static void createIndex(DBCollection collection,
                            String[] fields,
                            boolean[] ascending,
                            boolean unique,
                            boolean sparse)
            throws MongoException {
        checkArgument(fields.length == ascending.length);
        DBObject index = new BasicDBObject();
        for (int i = 0; i < fields.length; i++) {
            index.put(fields[i], ascending[i] ? 1 : -1);
        }
        DBObject options = new BasicDBObject();
        options.put("unique", unique);
        options.put("sparse", sparse);
        collection.createIndex(index, options);
    }

    /**
     * Forces creation of a partial index on a set of fields, if one does not
     * already exist.
     *
     * @param collection the collection.
     * @param fields the name of the fields.
     * @param ascending {@code true} for an ascending, {@code false} for a
     *                              descending index.
     * @param filter the filter expression for the partial index.
     * @throws MongoException if the operation fails.
     */
    static void createPartialIndex(DBCollection collection,
                                   String[] fields,
                                   boolean[] ascending,
                                   String filter) throws MongoException {
        checkArgument(fields.length == ascending.length);
        DBObject index = new BasicDBObject();
        for (int i = 0; i < fields.length; i++) {
            index.put(fields[i], ascending[i] ? 1 : -1);
        }
        DBObject options = new BasicDBObject();
        options.put("partialFilterExpression", BasicDBObject.parse(filter));
        collection.createIndex(index, options);
    }

    /**
     * Returns {@code true} if there is an index on the given fields,
     * {@code false} otherwise. If multiple fields are passed, this method
     * check if there a compound index on those field. This method does not
     * check the sequence of fields for a compound index. That is, this method
     * will return {@code true} as soon as it finds an index that covers the
     * given fields, no matter their sequence in the compound index.
     *
     * @param collection the collection.
     * @param fields the fields of an index.
     * @return {@code true} if the index exists, {@code false} otherwise.
     * @throws MongoException if the operation fails.
     */
    static boolean hasIndex(DBCollection collection, String... fields)
            throws MongoException {
        Set<String> uniqueFields = Sets.newHashSet(fields);
        for (DBObject info : collection.getIndexInfo()) {
            DBObject key = (DBObject) info.get("key");
            Set<String> indexFields = Sets.newHashSet(key.keySet());
            if (uniqueFields.equals(indexFields)) {
                return true;
            }
        }
        return false;
    }
}
