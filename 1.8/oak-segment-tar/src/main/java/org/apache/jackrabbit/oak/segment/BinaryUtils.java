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

package org.apache.jackrabbit.oak.segment;

final class BinaryUtils {

    private BinaryUtils() {
        // Prevent instantiation
    }

    static int writeByte(byte[] buffer, int position, byte value) {
        buffer[position++] = value;
        return position;
    }

    static int writeShort(byte[] buffer, int position, short value) {
        position = writeByte(buffer, position, (byte) (value >> 8));
        position = writeByte(buffer, position, (byte) (value));
        return position;
    }

    static int writeInt(byte[] buffer, int position, int value) {
        position = writeShort(buffer, position, (short) (value >> 16));
        position = writeShort(buffer, position, (short) (value));
        return position;
    }

    static int writeLong(byte[] buffer, int position, long value) {
        position = writeInt(buffer, position, (int) (value >> 32));
        position = writeInt(buffer, position, (int) (value));
        return position;
    }

}
