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

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ElasticIndexUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexUtils.class);

    /**
     * Convert a JCR property name to a Elasticsearch field name.
     * Notice that "|" is not allowed in JCR names.
     *
     * @param propertyName the property name
     * @return the field name
     */
    public static String fieldName(String propertyName) {
        if(propertyName.startsWith(":")) {
            // there are some hardcoded field names
            return propertyName;
        }
        String fieldName = propertyName;
        boolean escape = false;
        if (fieldName.isBlank()) {
            // empty field name or field names that only consist of spaces
            escape = true;
        } else {
            // 99.99% property names are OK,
            // so we loop over the characters first
            for (int i = 0; i < fieldName.length() && !escape ; i++) {
                switch (fieldName.charAt(i)) {
                case '|':
                case '.':
                case '^':
                case '_':
                    escape = true;
                }
            }
        }
        if (escape) {
            StringBuilder buff = new StringBuilder(fieldName.length());
            for (int i = 0; i < fieldName.length(); i++) {
                char c = fieldName.charAt(i);
                switch (c) {
                case '|':
                    buff.append("||");
                    break;
                case '.':
                case '^':
                    buff.append('|').append(Integer.toHexString(c)).append('|');
                    break;
                default:
                    buff.append(c);
                }
            }
            fieldName = buff.toString();
            // internal field start with a _
            // we also support empty or just spaces
            if (fieldName.startsWith("_") || fieldName.isBlank()) {
                fieldName = "|" + fieldName;
            }
        }
        return fieldName;
    }

    /**
     * Convert an elasticsearch field name to a JCR property name.
     *
     * @param fieldName the field name
     * @return the property name
     */
    public static String propertyNameFromFieldName(String fieldName) {
        if (fieldName.indexOf('|') < 0) {
            return fieldName;
        }
        if (fieldName.startsWith("|")) {
            if (fieldName.equals("|")) {
                return "";
            } if (fieldName.startsWith("|_") || fieldName.substring(1).isBlank()) {
                fieldName = fieldName.substring(1);
            }
        }
        StringBuilder buff = new StringBuilder(fieldName.length());
        for (int i = 0; i < fieldName.length(); i++) {
            char c = fieldName.charAt(i);
            switch (c) {
            case '|':
                String next = fieldName.substring(i + 1);
                if (next.startsWith("|")) {
                    buff.append('|');
                    i++;
                } else {
                    int end = next.indexOf('|');
                    if (end < 0) {
                        buff.append(next);
                        break;
                    }
                    String code = next.substring(0, end);
                    try {
                        buff.append((char) Integer.parseInt(code, 16));
                    } catch (NumberFormatException e) {
                        buff.append(code);
                    }
                    i += code.length() + 1;
                }
                break;
            default:
                buff.append(c);
            }
        }
        return buff.toString();
    }

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
     * Converts a given byte array (of doubles) to a list of floats
     * @param array given byte array
     * @return list of floats
     */
    public static List<Float> toFloats(byte[] array) {
        int blockSize = Float.BYTES;
        ByteBuffer wrap = ByteBuffer.wrap(array);
        if (array.length % blockSize != 0) {
            LOG.warn("Unexpected byte array length {}", array.length);
        }
        int capacity = array.length / blockSize;
        List<Float> floats = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            float e = wrap.getFloat(i * blockSize);
            floats.add(e);
        }
        return floats;
    }

    /**
     * Converts a given list of float values into a byte array
     * @param values given list of floats
     * @return byte array
     */
    public static byte[] toByteArray(List<Float> values) {
        byte[] bytes = new byte[values.size() * Float.BYTES];
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        for (int i = 0; i < values.size(); i++) {
            wrap.putFloat(values.get(i));
        }
        return bytes;
    }

}
