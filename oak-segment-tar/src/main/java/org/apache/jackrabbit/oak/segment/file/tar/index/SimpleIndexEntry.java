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

public class SimpleIndexEntry implements IndexEntry {

    private final long msb;

    private final long lsb;

    private final int position;

    private final int length;

    private final int generation;

    private final int fullGeneration;

    private final boolean compacted;

    public SimpleIndexEntry(long msb, long lsb, int position, int length, int generation, int fullGeneration, boolean compacted) {
        this.msb = msb;
        this.lsb = lsb;
        this.position = position;
        this.length = length;
        this.generation = generation;
        this.fullGeneration = fullGeneration;
        this.compacted = compacted;
    }

    @Override
    public long getMsb() {
        return msb;
    }

    @Override
    public long getLsb() {
        return lsb;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int getGeneration() {
        return generation;
    }

    @Override
    public int getFullGeneration() {
        return fullGeneration;
    }

    @Override
    public boolean isCompacted() {
        return compacted;
    }
}
