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

import java.io.Closeable;
import java.io.IOException;

/**
 * The {@link JournalFile} writer. It allows to append a record to the journal file
 * (or create a new one, if it doesn't exist).
 * <p>
 * The implementation doesn't need to be thread-safe (eg. the caller has to take
 * care of synchronizing the {@link #writeLine(String)} method calls), but the method
 * should be:
 * <ul>
 *  <li>atomic with regards to the {@link JournalFileReader},</li>
 *  <li><b>flushed to the storage</b>.</li>
 * </ul>
 */
public interface JournalFileWriter extends Closeable {

    /**
     * Truncates the journal file. This is a maintenance operation, which may
     * break existing {@link JournalFileReader} and shouldn't be used in the
     * concurrent environment.
     *
     * @throws IOException
     */
    void truncate() throws IOException;

    /**
     * Write a new line to the journal file. This operation should be atomic,
     * eg. it's should be possible to open a new reader using
     * {@link JournalFile#openJournalReader()} in the way that it'll have access
     * to an incomplete record line.
     * <p>
     * If this method returns successfully it means that the line was persisted
     * on the non-volatile storage. For instance, on the local disk the
     * {@code flush()} should be called by the implementation.
     *
     * @param line the journal record to be written
     * @throws IOException
     */
    void writeLine(String line) throws IOException;

}
