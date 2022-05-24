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

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.JsonpSerializable;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import jakarta.json.stream.JsonGenerator;

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
    
    /**
     * Provides a string with the serialisation of the object.
     * Typically, used to obtain the DSL representation of Elasticsearch requests or partial requests.
     *
     * TODO: remove this when <a href="https://github.com/elastic/elasticsearch-java/issues/101">#101</a> gets implemented
     *
     * @return Json serialisation as a string.
     */
    public static String toString(JsonpSerializable query) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonpMapper mapper = new JacksonJsonpMapper();
        JsonGenerator generator = mapper.jsonProvider().createGenerator(baos);
        query.serialize(generator, mapper);
        generator.close();
        try {
            return baos.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Error creating request entity", e);
        }
    }
}
