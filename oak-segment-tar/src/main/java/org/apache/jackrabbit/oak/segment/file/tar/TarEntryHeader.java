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
package org.apache.jackrabbit.oak.segment.file.tar;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Fields of a TAR entry header used by Oak. See {@code oak-doc/.../nodestore/segment/tar.md}.
 */
final class TarEntryHeader {

    static final int NAME_LENGTH = 100;

    static final int SIZE_OFFSET = 124;

    static final int SIZE_LENGTH = 12;

    private TarEntryHeader() {
    }

    static String readName(ByteBuffer header) {
        byte[] b = new byte[NAME_LENGTH];
        header.get(0, b);
        int n = 0;
        while (n < NAME_LENGTH && b[n] != 0) {
            n++;
        }
        return new String(b, 0, n, StandardCharsets.UTF_8);
    }

    static int readSize(ByteBuffer header) {
        byte[] b = new byte[SIZE_LENGTH];
        header.get(SIZE_OFFSET, b);
        int number = 0;
        for (int i = 0; i < SIZE_LENGTH; i++) {
            int digit = b[i] & 0xff;
            if ('0' <= digit && digit <= '7') {
                number = number * 8 + digit - '0';
            } else {
                break;
            }
        }
        return number;
    }
}
