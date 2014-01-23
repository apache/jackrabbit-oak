/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.mongomk.cache;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import org.apache.jackrabbit.oak.plugins.mongomk.DocumentStore;
import org.apache.jackrabbit.oak.plugins.mongomk.NodeDocument;
import org.apache.jackrabbit.oak.plugins.mongomk.Revision;
import org.apache.jackrabbit.oak.plugins.mongomk.StableRevisionComparator;

import static com.google.common.base.Preconditions.checkArgument;

public class Serializers {
    /**
     * The serialization and deserialization logic would need to maintain the order
     * of read and writes
     */

    public static class RevisionSerizlizer extends Serializer<Revision> {
        @Override
        public void write(Kryo kryo, Output o, Revision r) {
            o.writeLong(r.getTimestamp(), true);
            o.writeInt(r.getCounter(), true);
            o.writeInt(r.getClusterId(), true);
            o.writeBoolean(r.isBranch());
        }

        @Override
        public Revision read(Kryo kryo, Input i, Class<Revision> revisionClass) {
            return new Revision(
                    i.readLong(true), //timestamp
                    i.readInt(true),  //counter
                    i.readInt(true),  //clusterId
                    i.readBoolean() //branch
            );
        }
    }

    public static class NodeDocumentSerializer extends Serializer<NodeDocument> {
        private final DocumentStore documentStore;

        public NodeDocumentSerializer(DocumentStore documentStore) {
            this.documentStore = documentStore;
        }

        @Override
        public void write(Kryo kryo, Output o, NodeDocument doc) {
            checkArgument(doc.isSealed(), "Cannot serialized non seal document [%s]", doc.getId());
            o.writeLong(doc.getCreated(), true);

            Set<String> keys = doc.keySet();
            o.writeInt(keys.size(), true);

            //Here assumption is that data has contents of following type
            //Primitive wrapper
            //NavigableMap of Revision -> Value
            for (String key : doc.keySet()) {
                o.writeString(key);
                Object val = doc.get(key);
                if (val instanceof NavigableMap) {
                    kryo.writeClass(o, NavigableMap.class);
                    new RevisionedMapSerializer(kryo).write(kryo, o, (Map) val);
                } else {
                    kryo.writeClass(o, val.getClass());
                    kryo.writeObject(o, val);
                }
            }
        }

        @Override
        public NodeDocument read(Kryo kryo, Input input, Class<NodeDocument> nodeDocumentClass) {
            long created = input.readLong(true);

            int mapSize = input.readInt(true);
            NodeDocument doc = new NodeDocument(documentStore, created);
            for (int i = 0; i < mapSize; i++) {
                String key = input.readString();
                Registration reg = kryo.readClass(input);
                Object value;
                if (reg.getType() == NavigableMap.class) {
                    value = new RevisionedMapSerializer(kryo).read(kryo, input, Map.class);
                } else {
                    value = kryo.readObject(input, reg.getType());
                }
                doc.put(key, value);
            }

            //Seal the doc once all changes done
            doc.seal();

            return doc;
        }

    }

    private static class RevisionedMapSerializer extends MapSerializer {

        public RevisionedMapSerializer(Kryo kryo) {
            setKeysCanBeNull(false);
            setKeyClass(Revision.class, kryo.getSerializer(Revision.class));
        }

        @SuppressWarnings("unchecked")
        protected Map create(Kryo kryo, Input input, Class<Map> type) {
            return new TreeMap(StableRevisionComparator.REVERSE);
        }
    }
}
