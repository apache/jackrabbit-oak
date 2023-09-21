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
package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getMetadataFileName;

public class IndexStoreMetadataOperatorImpl<M> implements IndexStoreMetadataOperator<M> {
    private static final Logger log = LoggerFactory.getLogger(IndexStoreMetadataOperatorImpl.class);

    @Override
    public File createMetadataFile(M m, File file, Compression algorithm) throws IOException {
        File metadataFile;
        if (file.isDirectory()) {
            metadataFile = new File(file, getMetadataFileName(algorithm));
        } else {
            metadataFile = file;
        }

        try (BufferedWriter metadataWriter = FlatFileStoreUtils.createWriter(metadataFile, algorithm)) {
            writeMetadataToFile(metadataWriter, m);
        }
        log.info("Created metadataFile:{} ", metadataFile.getPath());
        return metadataFile;
    }

    private void writeMetadataToFile(BufferedWriter bufferedWriter, M m) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(bufferedWriter, m);
    }

    /*
        deserialization with generic type doesnot happen automatically. So we explicitly added this 3rd argument to
        provide JavaType info to jackson.
     */
    @Override
    public M getIndexStoreMetadata(File metadataFile, Compression algorithm, TypeReference<M> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JavaType javaType = mapper.getTypeFactory().constructType(clazz);
        try (BufferedReader metadataFilebufferedReader = FlatFileStoreUtils.createReader(metadataFile, algorithm)){
            return mapper.readValue(metadataFilebufferedReader.readLine(), javaType);
        }
    }

}
