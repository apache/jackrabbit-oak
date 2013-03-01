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
package org.apache.jackrabbit.mongomk.prototype;

import java.util.List;
import java.util.Map;

/**
 * The interface for the backend storage for documents.
 */
public interface DocumentStore {

    /**
     * The list of collections.
     */
    enum Collection { NODES }

    /**
     * Get a document. The returned map is a clone (the caller
     * can modify it without affecting the stored version).
     *
     * @param collection the collection
     * @param path the path
     * @return the map, or null if not found
     */
    Map<String, Object> find(Collection collection, String key);

    List<Map<String, Object>> query(Collection collection, String fromKey, String toKey, int limit);
    
    /**
     * Remove a document.
     *
     * @param collection the collection
     * @param path the path
     */
    void remove(Collection collection, String key);

    /**
     * Try to create a list of documents.
     * 
     * @param collection the collection
     * @param updateOps the list of documents to add
     * @return true if this worked (if none of the documents already existed)
     */
    boolean create(Collection collection, List<UpdateOp> updateOps);
    
    /**
     * Create or update a document. For MongoDb, this is using "findAndModify" with
     * the "upsert" flag (insert or update).
     *
     * @param collection the collection
     * @param update the update operation
     * @return the new document
     */    
    Map<String, Object> createOrUpdate(Collection collection, UpdateOp update); 
    
    void dispose();

}
