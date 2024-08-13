/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentPropertyStateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentPropertyStateFactory.class);

    public static PropertyState createPropertyState(DocumentNodeStore store, String name, String value, Compression compression) {

        if (compression != null && !compression.equals(Compression.NONE) &&
                CompressedDocumentPropertyState.getCompressionThreshold() != -1
                && value.length() > CompressedDocumentPropertyState.getCompressionThreshold()) {
            try {
                return new CompressedDocumentPropertyState(store, name, value, compression);
            } catch (Exception e) {
                LOG.warn("Failed to compress property {} value: ", name, e);
                return new DocumentPropertyState(store, name, value);
            }

        } else {
            return new DocumentPropertyState(store, name, value);
        }
    }

    public static PropertyState createPropertyState(DocumentNodeStore store, String name, String value) {
        return createPropertyState(store, name, value, Compression.GZIP);
    }
}
