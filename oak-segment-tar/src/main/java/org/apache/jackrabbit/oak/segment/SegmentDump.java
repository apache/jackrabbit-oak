/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.Segment.MAX_SEGMENT_SIZE;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Consumer;

import com.google.common.base.Charsets;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.jackrabbit.oak.segment.RecordNumbers.Entry;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

class SegmentDump {

    private static int getAddress(int length, int offset) {
        return length - (MAX_SEGMENT_SIZE - offset);
    }

    static String dumpSegment(SegmentId id, int length, String segmentInfo, GCGeneration generation, SegmentReferences segmentReferences, RecordNumbers recordNumbers, Consumer<OutputStream> dumper) {
        StringWriter string = new StringWriter();
        try (PrintWriter writer = new PrintWriter(string)) {
            writer.format("Segment %s (%d bytes)%n", id, length);
            if (segmentInfo != null) {
                writer.format("Info: %s, Generation: %s%n", segmentInfo, generation);
            }
            if (id != null && id.isDataSegmentId()) {
                writer.println("--------------------------------------------------------------------------");
                int i = 1;
                for (SegmentId segmentId : segmentReferences) {
                    writer.format("reference %02x: %s%n", i++, segmentId);
                }
                for (Entry entry : recordNumbers) {
                    int offset = entry.getOffset();
                    writer.format("%10s record %08x: %08x @ %08x%n",
                        entry.getType(), entry.getRecordNumber(), offset, getAddress(length, offset));
                }
            }
            writer.println("--------------------------------------------------------------------------");
            dumper.accept(new WriterOutputStream(writer, Charsets.UTF_8));
            writer.println("--------------------------------------------------------------------------");
        }
        return string.toString();
    }

}

