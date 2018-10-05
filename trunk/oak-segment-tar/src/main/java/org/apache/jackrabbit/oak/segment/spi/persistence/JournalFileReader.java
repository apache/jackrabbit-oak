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
 * The {@link JournalFile} reader. It reads the journal file backwards, starting
 * from the last written line.
 * <p>
 * The implementation doesn't need to be thread-safe.
 */
public interface JournalFileReader extends Closeable {

    /**
     * Read the line from the journal, using LIFO strategy (last in, first out).
     * @return the journal record
     * @throws IOException
     */
    String readLine() throws IOException;

}
