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
package org.apache.jackrabbit.oak.spi.blob;

import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.codec.binary.Hex.encodeHexString;

/**
 */
public abstract class AbstractSharedBackend implements SharedBackend {
    private static Logger LOG = LoggerFactory.getLogger(AbstractSharedBackend.class);

    private static final String ALGORITHM = "HmacSHA1";

    /**
     * Cached copy of the reference key of this data store. Initialized in
     * {@link #getReferenceKey()} when the key is first accessed.
     */
    private byte[] referenceKey = null;


    protected String getReferenceFromIdentifier(DataIdentifier identifier) {
        try {
            String id = identifier.toString();

            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(getReferenceKey(), ALGORITHM));
            byte[] hash = mac.doFinal(id.getBytes("UTF-8"));

            return id + ':' + encodeHexString(hash);
        } catch (Exception e) {
            LOG.error("Failed to hash identifier using MAC (Message Authentication Code) algorithm.", e);
        }
        return null;
    }

    /**
     * Returns the reference key of this backend. If one does not already
     * exist, it is automatically created in an implementation-specific way.
     * The default implementation simply creates a temporary random key that's
     * valid only until the data store gets restarted. Subclasses can override
     * and/or decorate this method to support a more persistent reference key.
     * <p>
     * This method is called only once during the lifetime of a backend
     * instance and the return value is cached in memory, so it's no problem
     * if the implementation is slow.
     *
     * @return reference key
     * @throws DataStoreException if the key is not available
     */
    public byte[] getOrCreateReferenceKey() throws DataStoreException {
        byte[] referenceKeyValue = new byte[256];
        new SecureRandom().nextBytes(referenceKeyValue);
        return referenceKeyValue;
    }

    //-----------------------------------------------------------< private >--

    /**
     * Returns the reference key of this data store. Synchronized to
     * control concurrent access to the cached {@link #referenceKey} value.
     *
     * @return reference key
     * @throws DataStoreException if the key is not available
     */
    private synchronized byte[] getReferenceKey() throws DataStoreException {
        if (referenceKey == null) {
            referenceKey = getOrCreateReferenceKey();
        }
        return referenceKey;
    }
}
