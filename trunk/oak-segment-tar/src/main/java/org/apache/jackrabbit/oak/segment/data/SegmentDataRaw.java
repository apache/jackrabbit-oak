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

package org.apache.jackrabbit.oak.segment.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

class SegmentDataRaw implements SegmentData {

    private final ByteBuffer buffer;

    SegmentDataRaw(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private int index(int recordReferenceOffset) {
        return SegmentDataUtils.index(buffer, recordReferenceOffset);
    }

    @Override
    public ByteBuffer readBytes(int recordReferenceOffset, int size) {
        return SegmentDataUtils.readBytes(buffer, index(recordReferenceOffset), size);
    }

    @Override
    public void hexDump(OutputStream stream) throws IOException {
        SegmentDataUtils.hexDump(buffer, stream);
    }

    @Override
    public void binDump(OutputStream stream) throws IOException {
        SegmentDataUtils.binDump(buffer, stream);
    }

    @Override
    public int size() {
        return buffer.remaining();
    }

    @Override
    public int estimateMemoryUsage() {
        return SegmentDataUtils.estimateMemoryUsage(buffer);
    }

    @Override
    public byte getVersion() {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public String getSignature() {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public int getFullGeneration() {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public boolean isCompacted() {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public int getGeneration() {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public int getSegmentReferencesCount() {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public int getRecordReferencesCount() {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public int getRecordReferenceNumber(int i) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public byte getRecordReferenceType(int i) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public int getRecordReferenceOffset(int i) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public long getSegmentReferenceMsb(int i) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public long getSegmentReferenceLsb(int i) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public long readLength(int recordReferenceOffset) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public StringData readString(int recordReferenceOffset) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public RecordIdData readRecordId(int recordReferenceOffset) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public byte readByte(int recordReferenceOffset) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public int readInt(int recordReferenceOffset) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public short readShort(int recordReferenceOffset) {
        throw new IllegalStateException("invalid operation");
    }

    @Override
    public long readLong(int recordReferenceOffset) {
        throw new IllegalStateException("invalid operation");
    }

}
