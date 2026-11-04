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
package org.apache.jackrabbit.oak.segment.file.tar;

import java.io.IOException;

import static org.apache.jackrabbit.oak.segment.file.tar.SegmentTarWriter.ZERO_BYTES;

public interface IOWriteAccess extends IOReadAccess {

    default int appendSegmentEntry(byte[] header, byte[] data, int offset, int size, int padding) throws IOException {
        int bytesWritten = write(header);
        bytesWritten += write(data, offset, size);
        if (padding > 0) {
            bytesWritten += write(ZERO_BYTES, 0, padding);
        }
        return bytesWritten;
    }

    default int appendEntry(byte[] header, byte[] data, int padding) throws IOException {
        int bytesWritten = write(header);
        if (padding > 0) {
            bytesWritten += write(ZERO_BYTES, 0, padding);
        }
        bytesWritten += write(data);
        return bytesWritten;
    }

    int write(byte[] data) throws IOException;

    int write(byte[] data, int offset, int length) throws IOException;

    void flush() throws IOException;

    boolean isDirty();
}
