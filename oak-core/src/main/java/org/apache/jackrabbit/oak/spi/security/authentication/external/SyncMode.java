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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import org.apache.jackrabbit.util.Text;

/**
 * SyncMode... TODO: define sync-modes
 */
public class SyncMode {

    public static final int MODE_NO_SYNC = 0;
    public static final int MODE_CREATE_USER = 1;
    public static final int MODE_CREATE_GROUPS = 2;
    public static final int MODE_UPDATE = 4;

    public static final String NO_SYNC = "";
    public static final String CREATE_USER = "createUser";
    public static final String CREATE_GROUP = "createGroup";
    public static final String UPDATE = "update";

    public static final SyncMode DEFAULT_SYNC = new SyncMode(MODE_CREATE_USER|MODE_CREATE_GROUPS|MODE_UPDATE);

    private final int mode;

    private SyncMode(int mode) {
        this.mode = mode;
    }

    public boolean contains(int mode) {
        return (this.mode & mode) == mode;
    }

    public static SyncMode fromObject(Object smValue) {
        if (smValue instanceof SyncMode) {
            return (SyncMode) smValue;
        } else if (smValue instanceof String[]) {
            return fromStrings((String[]) smValue);
        } else {
            return fromStrings(Text.explode(smValue.toString(), ',', false));
        }
    }

    public static SyncMode fromString(String name) {
        int mode;
        if (CREATE_USER.equals(name)) {
            mode = MODE_CREATE_USER;
        } else if (CREATE_GROUP.equals(name)) {
            mode = MODE_CREATE_GROUPS;
        } else if (UPDATE.equals(name)) {
            mode = MODE_UPDATE;
        } else if (name.isEmpty()) {
            mode = MODE_NO_SYNC;
        }else {
            throw new IllegalArgumentException("invalid sync mode name " + name);
        }
        return fromInt(mode);
    }

    public static SyncMode fromStrings(String[] names) {
        int mode = MODE_NO_SYNC;
        for (String name : names) {
            mode |= fromString(name.trim()).mode;
        }
        return new SyncMode(mode);
    }

    private static SyncMode fromInt(int mode) {
        if (mode == DEFAULT_SYNC.mode) {
            return DEFAULT_SYNC;
        }

        if (mode < 0 || mode > DEFAULT_SYNC.mode) {
            throw new IllegalArgumentException("invalid sync mode: " + mode);
        } else {
            return new SyncMode(mode);
        }
    }
}