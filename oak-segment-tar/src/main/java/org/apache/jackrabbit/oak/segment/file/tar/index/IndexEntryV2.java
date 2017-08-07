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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import java.nio.ByteBuffer;

class IndexEntryV2 implements IndexEntry {

    static final int SIZE = 33;

    private final ByteBuffer index;

    private final int position;

    IndexEntryV2(ByteBuffer index, int position) {
        this.index = index;
        this.position = position;
    }

    @Override
    public long getMsb() {
        return index.getLong(position);
    }

    @Override
    public long getLsb() {
        return index.getLong(position + 8);
    }

    @Override
    public int getPosition() {
        return index.getInt(position + 16);
    }

    @Override
    public int getLength() {
        return index.getInt(position + 20);
    }

    @Override
    public int getGeneration() {
        return index.getInt(position + 24);
    }

    @Override
    public int getFullGeneration() {
        return index.getInt(position + 28);
    }

    @Override
    public boolean isCompacted() {
        return index.get(position + 32) != 0;
    }

}
