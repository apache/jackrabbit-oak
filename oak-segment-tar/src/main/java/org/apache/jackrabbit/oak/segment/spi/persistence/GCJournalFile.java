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
package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;
import java.util.List;

/**
 * This type abstracts the {@code gc.log} file, used to save information about
 * the segment garbage collection. Each record is represented by a single string.
 * <br><br>
 * The implementation <b>doesn't need to be</b> thread-safe.
 */
public interface GCJournalFile {

    /**
     * Write the new line to the GC journal file.
     *
     * @param line the line to write. It should contain neither special characters
     *             nor the newline {@code \n}.
     * @throws IOException
     */
    void writeLine(String line) throws IOException;

    /**
     * Return the list of all written records in the same order as they were
     * written.
     *
     * @return the list of all written lines
     * @throws IOException
     */
    List<String> readLines() throws IOException;

}
