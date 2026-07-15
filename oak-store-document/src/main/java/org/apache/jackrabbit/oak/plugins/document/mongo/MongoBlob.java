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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

/**
 * The {@code MongoDB} representation of a blob. Only used by MongoBlobStore
 */
public class MongoBlob implements Bson {

    public static final String KEY_ID = "_id";
    public static final String KEY_DATA = "data";
    public static final String KEY_LAST_MOD = "lastMod";
    public static final String KEY_LEVEL = "level";

    private String id;
    private byte[] data;
    private int level;
    private long lastMod;

    /**
     * Default constructor. Needed for MongoDB serialization.
     */
    public MongoBlob() {
    }

    static MongoBlob fromDocument(Document doc) {
        MongoBlob blob = new MongoBlob();
        blob.setId(doc.getString(KEY_ID));
        blob.setLevel(doc.getInteger(KEY_LEVEL, 0));
        if (doc.containsKey(KEY_LAST_MOD)) {
            blob.setLastMod(doc.getLong(KEY_LAST_MOD));
        }
        if (doc.containsKey(KEY_DATA)) {
            blob.setData(doc.get(KEY_DATA, Binary.class).getData());
        }
        return blob;
    }

    Document asDocument() {
        Document doc = new Document();
        doc.put(KEY_ID, id);
        doc.put(KEY_LEVEL, level);
        doc.put(KEY_LAST_MOD, lastMod);
        doc.put(KEY_DATA, data);
        return doc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public long getLastMod() {
        return lastMod;
    }

    public void setLastMod(long lastMod) {
        this.lastMod = lastMod;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(Class<TDocument> tDocumentClass,
                                                   CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<>(this, codecRegistry.get(MongoBlob.class));
    }
}