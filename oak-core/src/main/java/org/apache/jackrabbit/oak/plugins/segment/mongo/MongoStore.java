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
package org.apache.jackrabbit.oak.plugins.segment.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableMap.of;
import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.ReadPreference.primary;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.segment.AbstractStore;
import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongoStore extends AbstractStore {

    private final WriteConcern concern = WriteConcern.SAFE; // TODO: MAJORITY?

    private final DB db;

    private final DBCollection segments;

    private final Map<String, Journal> journals = Maps.newHashMap();

    public MongoStore(DB db, int cacheSizeMB) {
        super(cacheSizeMB);
        this.db = checkNotNull(db);
        this.segments = db.getCollection("segments");
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("root");
        journals.put("root", new MongoJournal(
                this, db.getCollection("journals"),
                concern, builder.getNodeState()));
    }

    @Override
    public void close() {
    }

    @Override
    public synchronized Journal getJournal(String name) {
        Journal journal = journals.get(name);
        if (journal == null) {
            journal = new MongoJournal(
                    this, db.getCollection("journals"), concern, name);
            journals.put(name, journal);
        }
        return journal;
    }

    @Override
    public void writeSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        if (offset != 0 || length != data.length) {
            byte[] b = new byte[length];
            System.arraycopy(data, offset, b, 0, length);
            data = b;
        }

        BasicDBObject segment = new BasicDBObject();
        segment.put("_id", segmentId.toString());
        segment.put("data", data);
        segments.insert(segment, concern);
    }

    @Override @Nonnull
    protected Segment loadSegment(UUID segmentId) {
        DBObject id = new BasicDBObject("_id", segmentId.toString());
        DBObject fields = new BasicDBObject(of("data", 1));

        DBObject segment = segments.findOne(id, fields, nearest());
        if (segment == null) {
            segment = segments.findOne(id, fields, primary());
            if (segment == null) {
                throw new IllegalStateException(
                        "Segment " + segmentId + " not found");
            }
        }

        byte[] data = (byte[]) segment.get("data");
        return createSegment(segmentId, ByteBuffer.wrap(data));
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        segments.remove(new BasicDBObject("_id", segmentId.toString()));
        super.deleteSegment(segmentId);
    }

}
