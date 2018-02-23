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

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class DataStoreReference {
    private static Logger LOG = LoggerFactory.getLogger(DataStoreReference.class);

    public static DataIdentifier getIdentifierFromReference(String reference) {
        if (reference != null) {
            int colon = reference.indexOf(':');
            if (colon != -1) {
                return new DataIdentifier(reference.substring(0, colon));
            }
        }
        return null;
    }

    private static final String ALGORITHM = "HmacSHA1";
    public static String getReferenceFromIdentifier(DataIdentifier identifier,
                                                    byte[] referenceKey) {
        try {
            String id = identifier.toString();

            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(referenceKey, ALGORITHM));
            byte[] hash = mac.doFinal(id.getBytes("UTF-8"));

            return id + ':' + encodeHexString(hash);
        } catch (Exception e) {
            LOG.error("Failed to hash identifier using MAC (Message Authentication Code) algorithm.", e);
        }
        return null;
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static String encodeHexString(byte[] value) {
        char[] buffer = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            buffer[2 * i] = HEX[(value[i] >> 4) & 0x0f];
            buffer[2 * i + 1] = HEX[value[i] & 0x0f];
        }
        return new String(buffer);
    }
}
