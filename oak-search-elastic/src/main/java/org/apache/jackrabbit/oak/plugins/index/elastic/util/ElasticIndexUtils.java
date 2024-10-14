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
package org.apache.jackrabbit.oak.plugins.index.elastic.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticIndexUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexUtils.class);

    /**
     * Transforms a path into an _id compatible with Elasticsearch specification. The path cannot be larger than 512
     * bytes. For performance reasons paths that are already compatible are returned untouched. Otherwise, SHA-256
     * algorithm is used to return a transformed path (32 bytes max).
     *
     * @param path the document path
     * @return the Elasticsearch compatible path
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html">Mapping _id field</a>
     */
    public static String idFromPath(@NotNull String path) {
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        if (pathBytes.length > 512) {
            try {
                return new String(MessageDigest.getInstance("SHA-256").digest(pathBytes));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
        return path;
    }

    /**
     * Generates a consistent short value based on a given string using the SHA-256 algorithm.
     * The generated value  is a 16-bit integer ranging from -32,768 to 32,767.
     *
     * @param input the input string to be hashed
     * @return a short value from the input string
     * @throws IllegalStateException if the SHA-256 algorithm is not available
     */
    public static short getRandomShortFromString(String input) {
        try {
            // Get a SHA-256 MessageDigest instance
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            // Compute the hash of the input string
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));

            // Convert the first 2 bytes of the hash to an integer value
            int hashValue = (hash[0] & 0xFF) << 8 | (hash[1] & 0xFF);

            return (short) hashValue;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Converts a given byte array (of doubles) to a list of doubles
     * @param array given byte array
     * @return list of doubles
     */
    public static List<Double> toDoubles(byte[] array) {
        int blockSize = Double.SIZE / Byte.SIZE;
        ByteBuffer wrap = ByteBuffer.wrap(array);
        if (array.length % blockSize != 0) {
            LOG.warn("Unexpected byte array length {}", array.length);
        }
        int capacity = array.length / blockSize;
        List<Double> doubles = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            double e = wrap.getDouble(i * blockSize);
            doubles.add(e);
        }
        return doubles;
    }

    /**
     * Converts a given list of double values into a byte array
     * @param values given list of doubles
     * @return byte array
     */
    public static byte[] toByteArray(List<Double> values) {
        int blockSize = Double.SIZE / Byte.SIZE;
        byte[] bytes = new byte[values.size() * blockSize];
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        for (int i = 0, j = 0; i < values.size(); i++, j += blockSize) {
            wrap.putDouble(values.get(i));
        }
        return bytes;
    }

}
