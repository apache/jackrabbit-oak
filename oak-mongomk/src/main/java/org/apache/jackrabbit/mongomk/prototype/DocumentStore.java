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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernelException;

/**
 * The interface for the backend storage for documents.
 */
public interface DocumentStore {

    /**
     * The list of collections.
     */
    enum Collection { 
        
        /**
         * The 'nodes' collection. It contains all the node data, with one document
         * per node, and the path as the primary key. Each document possibly
         * contains multiple revisions.
         * <p>
         * Key: the path, value: the node data (possibly multiple revisions)
         * <p>
         * Old revisions are removed after some time, either by the process that
         * removed or updated the node, lazily when reading, or in a background
         * process.
         */
        NODES("nodes"), 
        
        /**
         * The 'clusterNodes' collection contains the list of currently running
         * cluster nodes. The key is the clusterNodeId (0, 1, 2,...).
         */
        CLUSTER_NODES("clusterNodes");
            
        final String name;
        
        Collection(String name) {
            this.name = name;
        }
        
        public String toString() {
            return name;
        }
        
    }

    /**
     * Get a document.
     * <p>
     * The returned map is a clone (the caller can modify it without affecting
     * the stored version).
     * 
     * @param collection the collection
     * @param key the key
     * @return the map, or null if not found
     */
    @CheckForNull
    Map<String, Object> find(Collection collection, String key);
    
    /**
     * Get a document, ignoring the cache if the cached entry is older than the
     * specified time.
     * <p>
     * The returned map is a clone (the caller can modify it without affecting
     * the stored version).
     * 
     * @param collection the collection
     * @param key the key
     * @param maxCacheAge the maximum age of the cached document
     * @return the map, or null if not found
     */
    @CheckForNull
    Map<String, Object> find(Collection collection, String key, int maxCacheAge);

    /**
     * Get a list of documents where the key is greater than a start value and
     * less than an end value.
     * 
     * @param collection the collection
     * @param fromKey the start value (excluding)
     * @param toKey the end value (excluding)
     * @param limit the maximum number of entries to return
     * @return the list (possibly empty)
     */
    @Nonnull
    List<Map<String, Object>> query(Collection collection, String fromKey, String toKey, int limit);
    
    /**
     * Remove a document.
     *
     * @param collection the collection
     * @param key the key
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
     * @return the old document
     * @throws MicroKernelException if the operation failed.
     */    
    @Nonnull
    Map<String, Object> createOrUpdate(Collection collection, UpdateOp update)
            throws MicroKernelException;

    /**
     * Invalidate the document cache.
     */
    void invalidateCache();

    /**
     * Dispose this instance.
     */
    void dispose();

}
