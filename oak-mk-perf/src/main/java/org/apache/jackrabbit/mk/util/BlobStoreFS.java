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
package org.apache.jackrabbit.mk.util;

import java.io.File;
import java.io.InputStream;

import org.apache.jackrabbit.mongomk.api.BlobStore;

public class BlobStoreFS implements BlobStore {

    public BlobStoreFS(String rootPath) {
        File rootDir = new File(rootPath);
        if (!rootDir.isDirectory()) {
            rootDir.mkdirs();
        }

    }

    public long getBlobLength(String blobId) throws Exception {
        return 0;
    }

    public int readBlob(String blobId, long blobOffset, byte[] buffer,
            int bufferOffset, int length) throws Exception {
        return 0;
    }

    public String writeBlob(InputStream is) throws Exception {
        return null;
    }

}