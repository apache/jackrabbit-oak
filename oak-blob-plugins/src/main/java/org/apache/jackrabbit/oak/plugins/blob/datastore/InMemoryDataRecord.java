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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents binary data which is backed by a byte[] (in memory).
 */
public class InMemoryDataRecord implements DataRecord {

    /**
     * Logger instance for this class
     */
    private static Logger log = LoggerFactory.getLogger(InMemoryDataRecord.class);

    /**
     * the prefix of the string representation of this value
     */
    private static final String PREFIX = "0x";

    /**
     * the data
     */
    private final byte[] data;

    private DataIdentifier identifier;

    /**
     * empty array
     */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * empty instance
     */
    private static final InMemoryDataRecord EMPTY = new InMemoryDataRecord(EMPTY_BYTE_ARRAY);

    /**
     * Creates a new instance from a
     * <code>byte[]</code> array.
     *
     * @param data the byte array
     */
    private InMemoryDataRecord(byte[] data) {
        this.data = data;
    }

    /**
     * Creates a new instance from a
     * <code>byte[]</code> array.
     *
     * @param data the byte array
     */
    static InMemoryDataRecord getInstance(byte[] data) {
        if (data.length == 0) {
            return EMPTY;
        } else {
            return new InMemoryDataRecord(data);
        }
    }

    /**
     * Checks if String can be converted to an instance of this class.
     * @param id DataRecord identifier
     * @return true if it can be converted
     */
    public static boolean isInstance(String id) {
        return id.startsWith(PREFIX);
    }

    /**
     * Convert a String to an instance of this class.
     * @param id DataRecord identifier
     * @return the instance
     */
    public static InMemoryDataRecord getInstance(String id) throws IllegalArgumentException {
        assert id.startsWith(PREFIX);
        id = id.substring(PREFIX.length());
        int len = id.length();
        if (len % 2 != 0) {
            String msg = "unable to deserialize byte array " + id + " , length=" + id.length();
            log.debug(msg);
            throw new IllegalArgumentException(msg);
        }
        len /= 2;
        byte[] data = new byte[len];
        try {
            for (int i = 0; i < len; i++) {
                data[i] = (byte) ((Character.digit(id.charAt(2 * i), 16) << 4) | (Character.digit(id.charAt(2 * i + 1), 16)));
            }
        } catch (NumberFormatException e) {
            String msg = "unable to deserialize byte array " + id;
            log.debug(msg);
            throw new IllegalArgumentException(msg);
        }
        return InMemoryDataRecord.getInstance(data);
    }

    @Override
    public DataIdentifier getIdentifier() {
        if (identifier == null) {
            identifier = new DataIdentifier(toString());
        }
        return identifier;
    }

    @Override
    public String getReference() {
        return null;
    }

    @Override
    public long getLength() throws DataStoreException {
        return data.length;
    }

    public InputStream getStream() {
        return new ByteArrayInputStream(data);
    }

    @Override
    public long getLastModified() {
        return 0;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder(PREFIX.length() + 2 * data.length);
        buff.append(PREFIX);
        for (byte aData : data) {
            int c = aData & 0xff;
            buff.append(Integer.toHexString(c >> 4));
            buff.append(Integer.toHexString(c & 0xf));
        }
        return buff.toString();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof InMemoryDataRecord) {
            InMemoryDataRecord other = (InMemoryDataRecord) obj;
            return Arrays.equals(data, other.data);
        }
        return false;
    }

    /**
     * Returns zero to satisfy the Object equals/hashCode contract.
     * This class is mutable and not meant to be used as a hash key.
     *
     * @return always zero
     * @see Object#hashCode()
     */
    public int hashCode() {
        return 0;
    }
}
