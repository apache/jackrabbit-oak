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
package org.apache.jackrabbit.oak.plugins.document.rdb;

/**
 * Container for the information in a RDB database column.
 */
public class RDBRow {

    private final String id, data;
    private final long modified, modcount;
    private final byte[] bdata;

    public RDBRow(String id, long modified, long modcount, String data, byte[] bdata) {
        this.id = id;
        this.modified = modified;
        this.modcount = modcount;
        this.data = data;
        this.bdata = bdata;
    }

    public String getId() {
        return id;
    }

    public String getData() {
        return data;
    }

    public long getModified() {
        return modified;
    }

    public long getModcount() {
        return modcount;
    }

    public byte[] getBdata() {
        return bdata;
    }
}
