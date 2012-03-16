/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.blobs;

import java.io.InputStream;

/**
 * An interface to store and read large binary objects.
 */
public interface BlobStore {

    /**
     * Write a blob from a temporary file. The temporary file is removed
     * afterwards. A file based blob stores might simply rename the file, so
     * that no additional writes are necessary.
     *
     * @param tempFilePath the temporary file
     * @return the blob id
     */
    String addBlob(String tempFilePath) throws Exception;

    /**
     * Write a blob from an input stream.
     * This method closes the input stream.
     *
     * @param in the input stream
     * @return the blob id
     */
    String writeBlob(InputStream in) throws Exception;

    int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws Exception;

    long getBlobLength(String blobId) throws Exception;

    void close();

}
