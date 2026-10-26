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

package org.apache.jackrabbit.oak.segment.util;

import java.nio.ByteBuffer;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.jetbrains.annotations.Nullable;

/**
 * Bridges between {@link ByteBuffer} and the deprecated {@link Buffer} still
 * used by the segment persistence SPI.
 */
@SuppressWarnings("deprecation")
public final class Buffers {

    private Buffers() {
    }

    public static @Nullable Buffer toBuffer(@Nullable ByteBuffer buffer) {
        return buffer == null ? null : Buffer.wrap(buffer);
    }

    public static @Nullable ByteBuffer toByteBuffer(@Nullable Buffer buffer) {
        return buffer == null ? null : buffer.toByteBuffer();
    }
}
