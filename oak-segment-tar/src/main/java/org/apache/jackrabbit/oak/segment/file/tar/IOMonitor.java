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

import java.io.File;

/**
 * Callback interface that eases the collection of statistics about I/O
 * operations.
 */
public interface IOMonitor {

    /**
     * Called before a segment is read from the file system.
     *
     * @param file   File containing the segment.
     * @param msb    Most significant bits of the segment ID.
     * @param lsb    Least significant bits of the segment ID.
     * @param length Size of the segment.
     */
    void beforeSegmentRead(File file, long msb, long lsb, int length);

    /**
     * Called after a segment is read from the file system. This is called only
     * in case of successful operations.
     *
     * @param file    File containing the segment.
     * @param msb     Most significant bits of the segment ID.
     * @param lsb     Least significant bits of the segment ID.
     * @param length  Size of the segment.
     * @param elapsed Time spent by the read operation, in nanoseconds.
     */
    void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed);

    /**
     * Called before a segment is written to the file system.
     *
     * @param file   File containing the segment.
     * @param msb    Most significant bits of the segment ID.
     * @param lsb    Least significant bits of the segment ID.
     * @param length Size of the segment.
     */
    void beforeSegmentWrite(File file, long msb, long lsb, int length);

    /**
     * Called after a segment is written to the file system. This is called only
     * in case of successful operations.
     *
     * @param file    File containing the segment.
     * @param msb     Most significant bits of the segment ID.
     * @param lsb     Least significant bits of the segment ID.
     * @param length  Size of the segment.
     * @param elapsed Time spent by the write operation, in nanoseconds.
     */
    void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed);

}
