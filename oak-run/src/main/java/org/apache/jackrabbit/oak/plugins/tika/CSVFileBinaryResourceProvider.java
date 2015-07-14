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

package org.apache.jackrabbit.oak.plugins.tika;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Closer;
import com.google.common.primitives.Longs;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.notNull;
import static org.apache.jackrabbit.JcrConstants.JCR_ENCODING;
import static org.apache.jackrabbit.JcrConstants.JCR_MIMETYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PATH;

class CSVFileBinaryResourceProvider implements BinaryResourceProvider, Closeable {
    private static final String BLOB_ID = "blobId";
    private static final String LENGTH = "length";
    static final CSVFormat FORMAT = CSVFormat.DEFAULT
            .withCommentMarker('#')
            .withHeader(
                    BLOB_ID,
                    LENGTH,
                    JCR_MIMETYPE,
                    JCR_ENCODING,
                    JCR_PATH
            )
            .withNullString("") //Empty string are considered as null
            .withIgnoreSurroundingSpaces()
            .withSkipHeaderRecord();
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File dataFile;
    private final BlobStore blobStore;
    private final Closer closer = Closer.create();

    public CSVFileBinaryResourceProvider(File dataFile, @Nullable BlobStore blobStore) {
        checkArgument(dataFile.exists(), "Data file %s does not exist", dataFile);
        this.dataFile = dataFile;
        this.blobStore = blobStore;
    }

    @Override
    public FluentIterable<BinaryResource> getBinaries(final String path) throws IOException {
        CSVParser parser = CSVParser.parse(dataFile, Charsets.UTF_8, FORMAT);
        closer.register(parser);
        return FluentIterable.from(parser)
                .transform(new RecordTransformer())
                .filter(notNull())
                .filter(new Predicate<BinaryResource>() {
                    @Override
                    public boolean apply(BinaryResource input) {
                        return PathUtils.isAncestor(path, input.getPath());
                    }
                });
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    private class RecordTransformer implements Function<CSVRecord, BinaryResource> {

        @Nullable
        @Override
        public BinaryResource apply(CSVRecord input) {
            String path = input.get(JCR_PATH);
            String mimeType = input.get(JCR_MIMETYPE);
            String encoding = input.get(JCR_ENCODING);
            String blobId = input.get(BLOB_ID);
            String length = input.get(LENGTH);
            Long len = length != null ? Longs.tryParse(length) : null;
            if (path == null || blobId == null || mimeType == null) {
                log.warn("Ignoring invalid record {}. Either of mimeType, blobId or path is null", input);
                return null;
            }

            return new BinaryResource(new BlobStoreByteSource(blobStore, blobId, len),
                    mimeType, encoding, path, blobId);
        }
    }

}
