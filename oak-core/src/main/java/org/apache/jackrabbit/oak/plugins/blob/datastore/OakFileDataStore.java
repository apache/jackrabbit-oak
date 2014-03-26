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

import com.google.common.io.BaseEncoding;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;

/**
 *  Oak specific extension of JR2 FileDataStore which enables
 *  provisioning the signing key via OSGi config
 */
public class OakFileDataStore extends FileDataStore {
    private byte[] signingKey;

    @Override
    protected byte[] getOrCreateReferenceKey() throws DataStoreException {
        if(signingKey != null){
            return signingKey;
        }
        return super.getOrCreateReferenceKey();
    }

    /**
     * Set Base64 encoded signing key
     */
    public void setSigningKey(String encodedKey) {
        this.signingKey = BaseEncoding.base64().decode(encodedKey);
    }
}
