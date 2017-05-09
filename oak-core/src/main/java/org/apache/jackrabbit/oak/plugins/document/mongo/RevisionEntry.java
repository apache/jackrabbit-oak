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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.bson.BSONObject;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A light-weight implementation of a MongoDB DBObject for a single revision
 * based map entry.
 */
public class RevisionEntry implements DBObject {

    private final Revision revision;

    private final Object value;

    public RevisionEntry(@Nonnull Revision revision,
                         @Nullable Object value) {
        this.revision = checkNotNull(revision);
        this.value = value;
    }

    @Override
    public void markAsPartialObject() {
    }

    @Override
    public boolean isPartialObject() {
        return false;
    }

    @Override
    public Object put(String key, Object v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(BSONObject o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(String key) {
        return revision.toString().equals(key) ? value : null;
    }

    @Override
    public Map toMap() {
        return Collections.singletonMap(revision.toString(), value);
    }

    @Override
    public Object removeField(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(String s) {
        return containsField(s);
    }

    @Override
    public boolean containsField(String s) {
        return revision.toString().equals(s);
    }

    @Override
    public Set<String> keySet() {
        return Collections.singleton(revision.toString());
    }

    @Override
    public String toString() {
        return JSON.serialize(this);
    }
}
