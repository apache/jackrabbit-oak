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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableMap.of;
import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.ReadPreference.primary;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public class MongoStore implements SegmentStore {

    private final WriteConcern concern = WriteConcern.SAFE; // TODO: MAJORITY?

    private final DB db;

    private final DBCollection segments;

    private final Map<String, Journal> journals = Maps.newHashMap();

    private final SegmentCache cache;

    public MongoStore(DB db, SegmentCache cache) {
        this.db = checkNotNull(db);
        this.segments = db.getCollection("segments");
        this.cache = cache;
        journals.put("root", new MongoJournal(
                this, db.getCollection("journals"), concern, EMPTY_NODE));
    }

    public MongoStore(DB db, long cacheSize) {
        this(db, new SegmentCache(cacheSize));
    }


    public MongoStore(Mongo mongo, long cacheSize) {
        this(mongo.getDB("Oak"), cacheSize);
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
    public Segment readSegment(final UUID segmentId) {
        return cache.getSegment(segmentId, new Callable<Segment>() {
            @Override
            public Segment call() throws Exception {
                return findSegment(segmentId);
            }
        });
    }

    @Override
    public void createSegment(
            UUID segmentId, byte[] data, int offset, int length,
            Collection<UUID> referencedSegmentIds,
            Map<String, RecordId> strings, Map<Template, RecordId> templates) {
        byte[] d = new byte[length];
        System.arraycopy(data, offset, d, 0, length);

        cache.addSegment(new Segment(
                this, segmentId, ByteBuffer.wrap(data), referencedSegmentIds,
                Collections.<String, RecordId>emptyMap(),
                Collections.<Template, RecordId>emptyMap()));

        insertSegment(segmentId, d, referencedSegmentIds);
    }

    private Segment findSegment(UUID segmentId) {
        DBObject id = new BasicDBObject("_id", segmentId.toString());
        DBObject fields = new BasicDBObject(of("data", 1, "uuids", 1));

        DBObject segment = segments.findOne(id, fields, nearest());
        if (segment == null) {
            segment = segments.findOne(id, fields, primary());
            if (segment == null) {
                throw new IllegalStateException(
                        "Segment " + segmentId + " not found");
            }
        }

        byte[] data = (byte[]) segment.get("data");
        List<?> list = (List<?>) segment.get("uuids");
        List<UUID> uuids = Lists.newArrayListWithCapacity(list.size());
        for (Object object : list) {
            uuids.add(UUID.fromString(object.toString()));
        }
        return new Segment(
                this, segmentId, ByteBuffer.wrap(data), uuids,
                Collections.<String, RecordId>emptyMap(),
                Collections.<Template, RecordId>emptyMap());
    }

    private void insertSegment(
            UUID segmentId, byte[] data, Collection<UUID> uuids) {
        List<String> list = Lists.newArrayListWithCapacity(uuids.size());
        for (UUID uuid : uuids) {
            list.add(uuid.toString());
        }

        BasicDBObject segment = new BasicDBObject();
        segment.put("_id", segmentId.toString());
        segment.put("data", data);
        segment.put("uuids", list);
        segments.insert(segment, concern);
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        segments.remove(new BasicDBObject("_id", segmentId.toString()));
        cache.removeSegment(segmentId);
    }
}
