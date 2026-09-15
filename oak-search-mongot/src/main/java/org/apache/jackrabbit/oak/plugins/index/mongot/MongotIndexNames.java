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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

final class MongotIndexNames {

    private MongotIndexNames() {
    }

    static String collectionName(String indexPath) {
        return "oak_" + digest(indexPath);
    }

    static String collectionName(String indexPath, long collectionSeed) {
        String base = collectionName(indexPath);
        return collectionSeed == 0
                ? base
                : base + "__" + Long.toUnsignedString(collectionSeed, 16);
    }

    static String searchIndexName(String indexPath) {
        return "search_" + digest(indexPath);
    }

    private static String digest(String indexPath) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                    .digest(indexPath.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }
}
