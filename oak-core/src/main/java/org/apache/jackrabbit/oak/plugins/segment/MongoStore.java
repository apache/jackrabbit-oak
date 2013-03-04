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
package org.apache.jackrabbit.oak.plugins.segment;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoStore implements SegmentStore {

    private final DBCollection segments;

    private final DBCollection journals;

    private final SegmentCache cache;

    public MongoStore(DB db, SegmentCache cache) {
        this.segments = db.getCollection("segments");
        this.journals = db.getCollection("journals");

        this.cache = cache;

        if (journals.findOne(new BasicDBObject("_id", "root")) == null) {
            SegmentWriter writer = new SegmentWriter(this, new SegmentReader(this));
            RecordId id = writer.writeNode(MemoryNodeState.EMPTY_NODE).getRecordId();
            writer.flush();
            journals.insert(new BasicDBObject(ImmutableMap.of(
                    "_id", "root", "head", id.toString())));
        }
    }

    public MongoStore(DB db, long cacheSize) {
        this(db, new SegmentCache(cacheSize));
    }


    public MongoStore(Mongo mongo, long cacheSize) {
        this(mongo.getDB("Oak"), cacheSize);
    }

    @Override
    public Journal getJournal(final String name) {
        return new Journal() {
            @Override
            public RecordId getHead() {
                DBObject journal = journals.findOne(new BasicDBObject("_id", name));
                return RecordId.fromString(journal.get("head").toString());
            }
            @Override
            public boolean setHead(RecordId base, RecordId head) {
                DBObject baseObject = new BasicDBObject(
                        ImmutableMap.of("_id", name, "head", base.toString()));
                DBObject headObject = new BasicDBObject(
                        ImmutableMap.of("_id", name, "head", head.toString()));
                return journals.findAndModify(baseObject, headObject) != null;
            }
        };
    }

    @Override
    public Segment readSegment(final UUID segmentId) {
        return cache.getSegment(segmentId, new Callable<Segment>() {
            @Override
            public Segment call() throws Exception {
                return findSegment(segmentId);
            }
        });
    }

    @Override
    public void createSegment(Segment segment) {
        cache.addSegment(segment);
        insertSegment(
                segment.getSegmentId(),
                segment.getData(),
                segment.getUUIDs());
    }

    @Override
    public void createSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        byte[] d = data;
        if (offset != 0 || length != data.length) {
            d = new byte[length];
            System.arraycopy(data, offset, d, 0, length);
        }
        insertSegment(segmentId, d, new UUID[0]);
    }

    private Segment findSegment(UUID segmentId) {
        DBObject segment = segments.findOne(
                new BasicDBObject("_id", segmentId.toString()));
        if (segment == null) {
            throw new IllegalStateException(
                    "Segment " + segmentId + " not found");
        }

        byte[] data = (byte[]) segment.get("data");
        List<?> list = (List<?>) segment.get("uuids");
        List<UUID> uuids = Lists.newArrayListWithCapacity(list.size());
        for (Object object : list) {
            uuids.add(UUID.fromString(object.toString()));
        }
        return new Segment(this, segmentId, data, uuids);
    }

    private void insertSegment(UUID segmentId, byte[] data, UUID[] uuids) {
        List<String> list = Lists.newArrayListWithCapacity(uuids.length);
        for (UUID uuid : uuids) {
            list.add(uuid.toString());
        }

        BasicDBObject segment = new BasicDBObject();
        segment.put("_id", segmentId.toString());
        segment.put("data", data);
        segment.put("uuids", list);
        segments.insert(segment);
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        segments.remove(new BasicDBObject("_id", segmentId.toString()));
        cache.removeSegment(segmentId);
    }
}
