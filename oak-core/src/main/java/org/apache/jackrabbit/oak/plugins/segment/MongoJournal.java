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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.of;
import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.ReadPreference.primaryPreferred;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

class MongoJournal implements Journal {

    private static final long UPDATE_INTERVAL =
            TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);

    private final SegmentStore store;

    private final DBCollection journals;

    private final WriteConcern concern;

    private final String name;

    private DBObject state;

    private long stateLastUpdated;

    MongoJournal(
            SegmentStore store, DBCollection journals,
            WriteConcern concern, NodeState root) {
        this.store = checkNotNull(store);
        this.journals = checkNotNull(journals);
        this.concern = checkNotNull(concern);
        this.name = "root";

        DBObject id = new BasicDBObject("_id", "root");
        state = journals.findOne(id, null, primaryPreferred());
        if (state == null) {
            SegmentWriter writer = new SegmentWriter(store);
            RecordId head = writer.writeNode(root).getRecordId();
            writer.flush();
            state = new BasicDBObject(of(
                    "_id",  "root",
                    "head", head.toString()));
            try {
                journals.insert(state, concern);
            } catch (MongoException.DuplicateKey e) {
                // Someone else managed to concurrently create the journal,
                // so let's just re-read it from the database
                state = journals.findOne(id, null, primaryPreferred());
                checkState(state != null);
            }
        }
        stateLastUpdated = System.nanoTime();
    }

    MongoJournal(
            SegmentStore store, DBCollection journals,
            WriteConcern concern, String name) {
        this.store = checkNotNull(store);
        this.journals = checkNotNull(journals);
        this.concern = checkNotNull(concern);
        this.name = checkNotNull(name);
        checkArgument(!"root".equals(name));

        DBObject id = new BasicDBObject("_id", name);
        state = journals.findOne(id, null, primaryPreferred());
        if (state == null) {
            Journal root = store.getJournal("root");
            String head = root.getHead().toString();
            state = new BasicDBObject(of(
                    "_id",    name,
                    "parent", "root",
                    "base",   head,
                    "head",   head));
            try {
                journals.insert(state, concern);
            } catch (MongoException.DuplicateKey e) {
                // Someone else managed to concurrently create the journal,
                // so let's just re-read it from the database
                state = journals.findOne(id, null, primaryPreferred());
                checkState(state != null);
            }
        }
        stateLastUpdated = System.nanoTime();
    }

    @Override
    public RecordId getHead() {
        long now = System.nanoTime();
        if (stateLastUpdated + UPDATE_INTERVAL < now) {
            DBObject id = new BasicDBObject("_id", name);
            DBObject freshState = journals.findOne(id, null, nearest());
            if (freshState == null) {
                freshState = journals.findOne(id, null, primaryPreferred());
                checkState(freshState != null);
            }
            state = freshState;
            stateLastUpdated = now;
        }
        return RecordId.fromString(state.get("head").toString());
    }

    @Override
    public synchronized boolean setHead(RecordId base, RecordId head) {
        if (!base.equals(getHead())) {
            return false;
        }

        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.add("_id", name);
        if (state.containsField("parent")) {
            builder.add("parent", state.get("parent"));
        }
        if (state.containsField("base")) {
            builder.add("base", state.get("base"));
        }
        builder.add("head", head.toString());
        DBObject nextState = builder.get();

        WriteResult result =
                journals.update(state, nextState, false, false, concern);
        if (result.getN() == 1) {
            state = nextState;
            stateLastUpdated = System.nanoTime();
            return true;
        } else {
            // force refresh when next accessed
            stateLastUpdated -= UPDATE_INTERVAL;
            return false;
        }
    }

    @Override
    public synchronized void merge() {
        DBObject id = new BasicDBObject("_id", name);
        DBObject state = journals.findOne(id, null, primaryPreferred());
        checkState(state != null);

        if (state.containsField("parent")) {
            RecordId base = RecordId.fromString(state.get("base").toString());
            RecordId head = RecordId.fromString(state.get("head").toString());

            NodeState before = new SegmentNodeState(store, base);
            NodeState after = new SegmentNodeState(store, head);

            Journal parent = store.getJournal(state.get("parent").toString());
            SegmentWriter writer = new SegmentWriter(store);
            while (!parent.setHead(base, head)) {
                RecordId newBase = parent.getHead();
                NodeBuilder builder =
                        new SegmentNodeState(store, newBase).builder();
                after.compareAgainstBaseState(before, new MergeDiff(builder));
                RecordId newHead =
                        writer.writeNode(builder.getNodeState()).getRecordId();
                writer.flush();

                base = newBase;
                head = newHead;
            }

            base = head;

            BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
            builder.add("_id", name);
            builder.add("parent", state.get("parent"));
            builder.add("base", base.toString());
            builder.add("head", head.toString());
            // TODO: concurrent updates?
            journals.update(state, builder.get(), false, false, concern);
        }
    }

}
