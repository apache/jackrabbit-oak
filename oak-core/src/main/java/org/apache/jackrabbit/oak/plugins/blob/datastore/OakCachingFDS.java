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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.CachingFDS;
import org.apache.jackrabbit.core.data.DataStoreException;

/**
 * Overrides the implementation of
 * {@link org.apache.jackrabbit.core.data.CachingDataStore#getOrCreateReferenceKey}.
 */
public class OakCachingFDS extends CachingFDS {
    /** The path for FS Backend **/
    private String fsBackendPath;

    public void setFsBackendPath(String fsBackendPath) {
        this.fsBackendPath = fsBackendPath;
    }

    @Override
    protected byte[] getOrCreateReferenceKey() throws DataStoreException {
        File file = new File(fsBackendPath, "reference.key");
        try {
            if (file.exists()) {
                return FileUtils.readFileToByteArray(file);
            } else {
                byte[] key = new byte[256];
                new SecureRandom().nextBytes(key);
                FileUtils.writeByteArrayToFile(file, key);
                return key;
            }
        } catch (IOException e) {
            throw new DataStoreException(
                "Unable to access reference key file " + file.getPath(), e);
        }
    }
}
