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

import com.mongodb.BasicDBObject;

/**
 * The {@code MongoDB} representation of a blob. Only used by MongoBlobStore
 */
public class MongoBlob extends BasicDBObject {

    public static final String KEY_ID = "_id";
    public static final String KEY_DATA = "data";
    public static final String KEY_LAST_MOD = "lastMod";
    public static final String KEY_LEVEL = "level";

    private static final long serialVersionUID = 5119970546251968672L;

    /**
     * Default constructor. Needed for MongoDB serialization.
     */
    public MongoBlob() {
    }

    public String getId() {
        return getString(KEY_ID);
    }

    public void setId(String id) {
        put(KEY_ID, id);
    }

    public byte[] getData() {
        return (byte[]) get(KEY_DATA);
    }

    public void setData(byte[] data) {
        put(KEY_DATA, data);
    }

    public int getLevel() {
        return getInt(KEY_LEVEL);
    }

    public void setLevel(int level) {
        put(KEY_LEVEL, level);
    }

    public long getLastMod() {
        return getLong(KEY_LAST_MOD);
    }

    public void setLastMod(long lastMod) {
        put(KEY_LAST_MOD, lastMod);
    }
}