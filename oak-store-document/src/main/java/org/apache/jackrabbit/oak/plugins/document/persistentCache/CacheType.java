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

import java.nio.ByteBuffer;

import org.apache.jackrabbit.oak.plugins.document.LocalDiffCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MemoryDiffCache;
import org.apache.jackrabbit.oak.plugins.document.NamePathRev;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.StringDataType;

public enum CacheType {
    
    NODE {

        @Override
        public <K> void writeKey(WriteBuffer buffer, K key) {
            PathRev pr = (PathRev) key;
            DataTypeUtil.pathRevToBuffer(pr, buffer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K> K readKey(ByteBuffer buffer) {
            return (K) DataTypeUtil.pathRevFromBuffer(buffer);
        }

        @Override
        public <K> int compareKeys(K a, K b) {
            return ((PathRev) a).compareTo((PathRev) b);
        }

        @Override
        public <V> void writeValue(WriteBuffer buffer, V value) {
            DataTypeUtil.stateToBuffer((DocumentNodeState) value, buffer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> V readValue(DocumentNodeStore store,
                               DocumentStore docStore,
                               ByteBuffer buffer) {
            return (V) DataTypeUtil.stateFromBuffer(store, buffer);
        }

        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            Path path = ((PathRev) key).getPath();
            if (!store.getNodeCachePredicate().apply(path)){
                return false;
            }
            return !store.getNodeStateCache().isCached(path);
        }
    },
    
    CHILDREN {

        @Override
        public <K> void writeKey(WriteBuffer buffer, K key) {
            DataTypeUtil.namePathRevToBuffer((NamePathRev) key, buffer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K> K readKey(ByteBuffer buffer) {
            return (K) DataTypeUtil.namePathRevFromBuffer(buffer);
        }

        @Override
        public <K> int compareKeys(K a, K b) {
            return ((NamePathRev) a).compareTo((NamePathRev) b);
        }

        @Override
        public <V> void writeValue(WriteBuffer buffer, V value) {
            String s = ((DocumentNodeState.Children) value).asString();
            StringDataType.INSTANCE.write(buffer, s);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> V readValue(DocumentNodeStore store,
                               DocumentStore docStore,
                               ByteBuffer buffer) {
            return (V) DocumentNodeState.Children.fromString(readString(buffer));
        }

        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            Path path = ((NamePathRev) key).getPath();
            if (!store.getNodeCachePredicate().apply(path)){
                return false;
            }
            return !store.getNodeStateCache().isCached(path);
        }
    }, 
    
    DIFF {

        @Override
        public <K> void writeKey(WriteBuffer buffer, K key) {
            MemoryDiffCache.Key k = ((MemoryDiffCache.Key) key);
            DataTypeUtil.pathToBuffer(k.getPath(), buffer);
            DataTypeUtil.revisionVectorToBuffer(k.getFromRevision(), buffer);
            DataTypeUtil.revisionVectorToBuffer(k.getToRevision(), buffer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K> K readKey(ByteBuffer buffer) {
            Path p = DataTypeUtil.pathFromBuffer(buffer);
            RevisionVector from = DataTypeUtil.revisionVectorFromBuffer(buffer);
            RevisionVector to = DataTypeUtil.revisionVectorFromBuffer(buffer);
            return (K) new MemoryDiffCache.Key(p, from, to);
        }


        @Override
        public <K> int compareKeys(K a, K b) {
            return ((MemoryDiffCache.Key) a).compareTo((MemoryDiffCache.Key) b);
        }

        @Override
        public <V> void writeValue(WriteBuffer buffer, V value) {
            String s = ((StringValue) value).asString();
            StringDataType.INSTANCE.write(buffer, s);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> V readValue(DocumentNodeStore store,
                               DocumentStore docStore,
                               ByteBuffer buffer) {
            return (V) StringValue.fromString(readString(buffer));
        }

        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    },

    DOCUMENT {

        @Override
        public <K> void writeKey(WriteBuffer buffer, K key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K> K readKey(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K> int compareKeys(K a, K b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> void writeValue(WriteBuffer buffer, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> V readValue(DocumentNodeStore store,
                               DocumentStore docStore,
                               ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return false;
        }
    },

    PREV_DOCUMENT {

        @Override
        public <K> void writeKey(WriteBuffer buffer, K key) {
            String s = ((StringValue) key).asString();
            StringDataType.INSTANCE.write(buffer, s);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K> K readKey(ByteBuffer buffer) {
            return (K) StringValue.fromString(readString(buffer));
        }

        @Override
        public <K> int compareKeys(K a, K b) {
            return ((StringValue) a).asString().compareTo(((StringValue) b).asString());
        }

        @Override
        public <V> void writeValue(WriteBuffer buffer, V value) {
            String s = ((NodeDocument) value).asString();
            StringDataType.INSTANCE.write(buffer, s);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> V readValue(DocumentNodeStore store,
                               DocumentStore docStore,
                               ByteBuffer buffer) {
            return (V) NodeDocument.fromString(docStore, readString(buffer));
        }

        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    },

    LOCAL_DIFF {

        @Override
        public <K> void writeKey(WriteBuffer buffer, K key) {
            RevisionsKey revisionsKey = ((RevisionsKey) key);
            DataTypeUtil.revisionVectorToBuffer(revisionsKey.getRev1(), buffer);
            DataTypeUtil.revisionVectorToBuffer(revisionsKey.getRev2(), buffer);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K> K readKey(ByteBuffer buffer) {
            RevisionVector rv1 = DataTypeUtil.revisionVectorFromBuffer(buffer);
            RevisionVector rv2 = DataTypeUtil.revisionVectorFromBuffer(buffer);
            return (K) new RevisionsKey(rv1, rv2);
        }

        @Override
        public <K> int compareKeys(K a, K b) {
            return ((RevisionsKey) a).compareTo((RevisionsKey) b);
        }

        @Override
        public <V> void writeValue(WriteBuffer buffer, V value) {
            String s = ((LocalDiffCache.Diff) value).asString();
            StringDataType.INSTANCE.write(buffer, s);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <V> V readValue(DocumentNodeStore store,
                               DocumentStore docStore,
                               ByteBuffer buffer) {
            return (V) LocalDiffCache.Diff.fromString(readString(buffer));
        }

        @Override
        public <K> boolean shouldCache(DocumentNodeStore store, K key) {
            return true;
        }
    };
    
    public static final CacheType[] VALUES = CacheType.values();

    public String getMapName() {
        // add a version suffix to the name and increment the version
        // whenever the serialization for a key or value changes
        return name() + "-v1";
    }

    public abstract <K> void writeKey(WriteBuffer buffer, K key);
    public abstract <K> K readKey(ByteBuffer buffer);
    public abstract <K> int compareKeys(K a, K b);
    public abstract <V> void writeValue(WriteBuffer buffer, V value);
    public abstract <V> V readValue(
            DocumentNodeStore store, DocumentStore docStore, ByteBuffer buffer);
    public abstract <K> boolean shouldCache(DocumentNodeStore store, K key);

    private static String readString(ByteBuffer buffer) {
        return StringDataType.INSTANCE.read(buffer);
    }
}

