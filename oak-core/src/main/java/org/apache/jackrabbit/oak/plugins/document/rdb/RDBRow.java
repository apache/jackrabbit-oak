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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Container for the information in a RDB database column.
 * <p>
 * Note that the String "data" and the byte[] "bdata" may be null
 * when the SQL SELECT request was conditional on "modcount" being
 * unchanged.
 */
public class RDBRow {

    public static final long LONG_UNSET = Long.MIN_VALUE;

    private final String id;
    private final boolean hasBinaryProperties, deletedOnce;
    private final long modified, modcount, cmodcount;
    private final String data;
    private final byte[] bdata;

    public RDBRow(String id, boolean hasBinaryProperties, boolean deletedOnce, Long modified, Long modcount, Long cmodcount, String data, byte[] bdata) {
        this.id = id;
        this.hasBinaryProperties = hasBinaryProperties;
        this.deletedOnce = deletedOnce;
        this.modified = modified != null ? modified.longValue() : LONG_UNSET;
        this.modcount = modcount != null ? modcount.longValue() : LONG_UNSET;
        this.cmodcount = cmodcount != null ? cmodcount.longValue() : LONG_UNSET;
        this.data = data;
        this.bdata = bdata;
    }

    @Nonnull
    public String getId() {
        return id;
    }

    public boolean hasBinaryProperties() {
        return hasBinaryProperties;
    }

    public boolean deletedOnce() {
        return deletedOnce;
    }

    @CheckForNull
    public String getData() {
        return data;
    }

    /**
     * @return {@link #LONG_UNSET} when not set in the database
     */
    public long getModified() {
        return modified;
    }

    /**
     * @return {@link #LONG_UNSET} when not set in the database
     */
    public long getModcount() {
        return modcount;
    }

    /**
     * @return {@link #LONG_UNSET} when not set in the database
     */
    public long getCollisionsModcount() {
        return cmodcount;
    }

    @CheckForNull
    public byte[] getBdata() {
        return bdata;
    }
}
