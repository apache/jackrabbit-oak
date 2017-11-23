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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import org.apache.jackrabbit.oak.plugins.document.LocalDiffCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

public enum CacheType {
    
    NODE {
        @Override
        public <K> String keyToString(K key) {
            return ((PathRev) key).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <K> K keyFromString(String key) {
            return (K) PathRev.fromString(key);
        }
        @Override
        public <K> int compareKeys(K a, K b) {
            return ((PathRev) a).compareTo((PathRev) b);
        }
        @Override
        public <V> String valueToString(V value) {
            return ((DocumentNodeState) value).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <V> V valueFromString(
                DocumentNodeStore store, DocumentStore docStore, String value) {
            return (V) DocumentNodeState.fromString(store, value);
        }
        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            String path = ((PathRev) key).getPath();
            if (!store.getNodeCachePredicate().apply(path)){
                return false;
            }
            return !store.getNodeStateCache().isCached(path);
        }
    },
    
    CHILDREN {
        @Override
        public <K> String keyToString(K key) {
            return ((PathRev) key).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <K> K keyFromString(String key) {
            return (K) PathRev.fromString(key);
        }
        @Override
        public <K> int compareKeys(K a, K b) {
            return ((PathRev) a).compareTo((PathRev) b);
        }
        @Override
        public <V> String valueToString(V value) {
            return ((DocumentNodeState.Children) value).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <V> V valueFromString(
                DocumentNodeStore store, DocumentStore docStore, String value) {
            return (V) DocumentNodeState.Children.fromString(value);
        }

        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            String path = ((PathRev) key).getPath();
            if (!store.getNodeCachePredicate().apply(path)){
                return false;
            }
            return !store.getNodeStateCache().isCached(path);
        }
    }, 
    
    DIFF {
        @Override
        public <K> String keyToString(K key) {
            return ((PathRev) key).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <K> K keyFromString(String key) {
            return (K) PathRev.fromString(key);
        }
        @Override
        public <K> int compareKeys(K a, K b) {
            return ((PathRev) a).compareTo((PathRev) b);
        }            
        @Override
        public <V> String valueToString(V value) {
            return ((StringValue) value).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <V> V valueFromString(
                DocumentNodeStore store, DocumentStore docStore, String value) {
            return (V) StringValue.fromString(value);
        }
        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    },

    DOC_CHILDREN {
        @Override
        public <K> String keyToString(K key) {
            return ((StringValue) key).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <K> K keyFromString(String key) {
            return (K) StringValue.fromString(key);
        }
        @Override
        public <K> int compareKeys(K a, K b) {
            return ((StringValue) a).asString().compareTo(((StringValue) b).asString());
        }            
        @Override
        public <V> String valueToString(V value) {
            return ((NodeDocument.Children) value).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <V> V valueFromString(
                DocumentNodeStore store, DocumentStore docStore, String value) {
            return (V) NodeDocument.Children.fromString(value);
        }
        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    }, 
    
    DOCUMENT {
        @Override
        public <K> String keyToString(K key) {
            return ((StringValue) key).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <K> K keyFromString(String key) {
            return (K) StringValue.fromString(key);
        }
        @Override
        public <K> int compareKeys(K a, K b) {
            return ((StringValue) a).asString().compareTo(((StringValue) b).asString());
        }            
        @Override
        public <V> String valueToString(V value) {
            return ((NodeDocument) value).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <V> V valueFromString(
                DocumentNodeStore store, DocumentStore docStore, String value) {
            return (V) NodeDocument.fromString(docStore, value);
        }
        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    },

    PREV_DOCUMENT {
        @Override
        public <K> String keyToString(K key) {
            return ((StringValue) key).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <K> K keyFromString(String key) {
            return (K) StringValue.fromString(key);
        }
        @Override
        public <K> int compareKeys(K a, K b) {
            return ((StringValue) a).asString().compareTo(((StringValue) b).asString());
        }
        @Override
        public <V> String valueToString(V value) {
            return ((NodeDocument) value).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <V> V valueFromString(
                DocumentNodeStore store, DocumentStore docStore, String value) {
            return (V) NodeDocument.fromString(docStore, value);
        }
        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    },

    LOCAL_DIFF {
        @Override
        public <K> String keyToString(K key) {
            return ((RevisionsKey) key).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <K> K keyFromString(String key) {
            return (K) RevisionsKey.fromString(key);
        }
        @Override
        public <K> int compareKeys(K a, K b) {
            return ((RevisionsKey) a).compareTo((RevisionsKey) b);
        }
        @Override
        public <V> String valueToString(V value) {
            return ((LocalDiffCache.Diff) value).asString();
        }
        @SuppressWarnings("unchecked")
        @Override
        public <V> V valueFromString(
                DocumentNodeStore store, DocumentStore docStore, String value) {
            return (V) LocalDiffCache.Diff.fromString(value);
        }
        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    },
    
    BLOB {

        @Override
        public <K> String keyToString(K key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K> K keyFromString(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K> int compareKeys(K a, K b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> String valueToString(V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> V valueFromString(DocumentNodeStore store,
                DocumentStore docStore, String value) {
            throw new UnsupportedOperationException();
        }
        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
        
    };
    
    public static final CacheType[] VALUES = CacheType.values();

    public abstract <K> String keyToString(K key);
    public abstract <K> K keyFromString(String key);
    public abstract <K> int compareKeys(K a, K b);
    public abstract <V> String valueToString(V value);
    public abstract <V> V valueFromString(
            DocumentNodeStore store, DocumentStore docStore, String value);
    public abstract <K> boolean shouldCache(DocumentNodeStore store, K key);

}

