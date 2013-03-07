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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

class MongoJournal implements Journal {

    private static final long UPDATE_INTERVAL =
            TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);

    private final DBCollection journals;

    private final String name;

    private long nextUpdate = System.nanoTime() - 2 * UPDATE_INTERVAL;

    private RecordId head;

    MongoJournal(DBCollection journals, String name) {
        this.journals = journals;
        this.name = name;
        head = getHead();
    }

    @Override
    public synchronized RecordId getHead() {
        long now = System.nanoTime();
        if (now >= nextUpdate) {
            DBObject journal = journals.findOne(new BasicDBObject("_id", name));
            head = RecordId.fromString(journal.get("head").toString());
            nextUpdate = now + UPDATE_INTERVAL;
        }
        return head;
    }

    @Override
    public boolean setHead(RecordId base, RecordId head) {
        DBObject baseObject = new BasicDBObject(
                ImmutableMap.of("_id", name, "head", base.toString()));
        DBObject headObject = new BasicDBObject(
                ImmutableMap.of("_id", name, "head", head.toString()));
        if (journals.findAndModify(baseObject, headObject) != null) {
            this.head = head;
            nextUpdate = System.nanoTime() + UPDATE_INTERVAL;
            return true;
        } else if (base.equals(this.head)) {
            // force an update at next getHead() call
            nextUpdate = System.nanoTime();
        }
        return false;
    }

    @Override
    public void merge() {
        throw new UnsupportedOperationException();
    }
}