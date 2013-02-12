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
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoStore implements SegmentStore {

    private static final int MAX_SEGMENT_SIZE = 1 << 23; // 8MB

    private static final long DEFAULT_CACHE_SIZE = 1 << 28; // 256MB

    private final DBCollection segments;

    private final DBCollection journals;

    private final LoadingCache<UUID, Segment> cache;

    public MongoStore(DB db, long cacheSize) {
        this.segments = db.getCollection("segments");
        this.journals = db.getCollection("journals");

        this.cache = CacheBuilder.newBuilder()
                .maximumWeight(cacheSize)
                .weigher(Segment.weigher())
                .build(new CacheLoader<UUID, Segment>() {
                    @Override
                    public Segment load(UUID key) throws Exception {
                        return findSegment(key);
                    }
                });
    }

    public MongoStore(DB db) {
        this(db, DEFAULT_CACHE_SIZE);
    }


    public MongoStore(Mongo mongo) {
        this(mongo.getDB("Oak"));
    }

    @Override
    public RecordId getJournalHead() {
        DBObject journal = journals.findOne(new BasicDBObject("_id", "root"));
        return RecordId.fromString(journal.get("head").toString());
    }

    @Override
    public boolean setJournalHead(RecordId head, RecordId base) {
        DBObject baseObject = new BasicDBObject(
                ImmutableMap.of("_id", "root", "head", base.toString()));
        DBObject headObject = new BasicDBObject(
                ImmutableMap.of("_id", "root", "head", head.toString()));
        return journals.update(baseObject, headObject).getN() == 1;
    }

    @Override
    public int getMaxSegmentSize() {
        return MAX_SEGMENT_SIZE;
    }

    @Override
    public Segment readSegment(UUID segmentId) {
        try {
            return cache.get(segmentId);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Failed to read segment " + segmentId, e);
        }
    }

    @Override
    public void createSegment(Segment segment) {
        cache.put(segment.getSegmentId(), segment);
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
        UUID[] uuids = new UUID[list.size()];
        for (int i = 0; i < uuids.length; i++) {
            uuids[i] = UUID.fromString(list.get(i).toString());
        }
        return new Segment(segmentId, data, uuids);
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
        cache.invalidate(segmentId);
    }
}
