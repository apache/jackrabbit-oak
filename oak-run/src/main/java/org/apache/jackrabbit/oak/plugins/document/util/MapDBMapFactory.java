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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.PathComparator;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

/**
 * A MapFactory backed by MapDB, which stores the map in a temporary file.
 */
public class MapDBMapFactory extends MapFactory {

    private final AtomicInteger counter = new AtomicInteger();
    private final DB db;

    public MapDBMapFactory() {
        this.db = DBMaker.newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .asyncWriteEnable()
                .make();
    }

    @Override
    public BTreeMap<Path, Revision> create() {
        return db.createTreeMap(String.valueOf(counter.incrementAndGet()))
                .keySerializer(new PathSerializer())
                .valueSerializer(new RevisionSerializer())
                .counterEnable()
                .make();
    }

    private static class PathSerializer
            extends BTreeKeySerializer<Path>
            implements Serializable {

        @Override
        public void serialize(DataOutput out, int start, int end, Object[] keys)
                throws IOException {
            for (int i = start; i < end; i++) {
                String p = keys[i].toString();
                out.writeUTF(p);
            }
        }

        @Override
        public Object[] deserialize(DataInput in, int start, int end, int size)
                throws IOException {
            Object[] keys = new Object[size];
            for (int i = start; i < end; i++) {
                keys[i] = Path.fromString(in.readUTF());
            }
            return keys;
        }

        @Override
        public Comparator<Path> getComparator() {
            return PathComparator.INSTANCE;
        }
    }

    private static class RevisionSerializer implements Serializer<Revision>,
            Serializable {
        private static final long serialVersionUID = 8648365575103098316L;
        private int size = 8 + 4 + 4 + 1;
        public void serialize(DataOutput o, Revision r) throws IOException {
            o.writeLong(r.getTimestamp());
            o.writeInt(r.getCounter());
            o.writeInt(r.getClusterId());
            o.writeBoolean(r.isBranch());

        }

        public Revision deserialize(DataInput i, int available) throws IOException {
            return new Revision(
                    i.readLong(), //timestamp
                    i.readInt(),  //counter
                    i.readInt(),  //clusterId
                    i.readBoolean()); //branch
        }

        public int fixedSize() {
            return size;
        }
    }
}
