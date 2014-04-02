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
package org.apache.jackrabbit.oak.plugins.segment.file;

import java.util.Comparator;

class TarEntry {

    static final Comparator<TarEntry> OFFSET_ORDER = new Comparator<TarEntry>() {
        @Override
        public int compare(TarEntry a, TarEntry b) {
            if (a.offset > b.offset) {
                return 1;
            } else if (a.offset < b.offset) {
                return -1;
            } else {
                return 0;
            }
        }
    };

    static final Comparator<TarEntry> IDENTIFIER_ORDER = new Comparator<TarEntry>() {
        @Override
        public int compare(TarEntry a, TarEntry b) {
            if (a.msb > b.msb) {
                return 1;
            } else if (a.msb < b.msb) {
                return -1;
            } else if (a.lsb > b.lsb) {
                return 1;
            } else if (a.lsb < b.lsb) {
                return -1;
            } else {
                return 0;
            }
        }
    };

    private final long msb;

    private final long lsb;

    private final int offset;

    private final int size;

    TarEntry(long msb, long lsb, int offset, int size) {
        this.msb = msb;
        this.lsb = lsb;
        this.offset = offset;
        this.size = size;
    }

    long msb() {
        return msb;
    }

    long lsb() {
        return lsb;
    }

    int offset() {
        return offset;
    }

    int size() {
        return size;
    }

}