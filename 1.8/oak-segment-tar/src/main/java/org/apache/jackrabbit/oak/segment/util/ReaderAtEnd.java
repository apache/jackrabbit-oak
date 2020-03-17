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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Read raw data from the end of an underlying data source. The data source is
 * usually a file, but any data source for which the concept of "end" makes
 * sense can be used. For example, a byte buffer could be used as the underlying
 * data source, e.g. for testing purposes.
 */
public interface ReaderAtEnd {

    /**
     * Read {@code amount} bytes from the underlying data source, starting at
     * {@code whence} bytes from the end of the data source.
     *
     * @param whence The offset from the end of the data source.
     * @param amount The amount of data to read, in bytes.
     * @return An instance of {@link ByteBuffer}.
     * @throws IOException if an error occurs while reading from the underlying
     *                     data source.
     */
    ByteBuffer readAtEnd(int whence, int amount) throws IOException;

}
