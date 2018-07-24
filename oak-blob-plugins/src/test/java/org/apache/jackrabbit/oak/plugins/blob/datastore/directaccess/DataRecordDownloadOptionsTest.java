/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.junit.Test;

public class DataRecordDownloadOptionsTest {
    private static final String MIME_TYPE_IMAGE_PNG = "image/png";
    private static final String MIME_TYPE_TEXT_PLAIN = "text/plain";
    private static final String ENCODING_UTF_8 = "utf-8";
    private static final String ENCODING_ISO_8859_1 = "ISO-8859-1";
    private static final String FILE_NAME_IMAGE = "amazing summer sunset.png";
    private static final String FILE_NAME_TEXT = "journal_entry_01-01-2000.txt";
    private static final String DISPOSITION_TYPE_INLINE = "inline";
    private static final String DISPOSITION_TYPE_ATTACHMENT = "attachment";

    private void verifyOptions(DataRecordDownloadOptions options,
                               String mimeType,
                               String encoding,
                               String fileName,
                               String dispositionType) {
        assertNotNull(options);
        if (null != mimeType) {
            assertEquals(mimeType, options.getMediaType());
        }
        else {
            assertNull(options.getMediaType());
        }
        if (null != encoding) {
            assertEquals(encoding, options.getCharacterEncoding());
        }
        else {
            assertNull(options.getCharacterEncoding());
        }
        if (null != dispositionType) {
            assertEquals(dispositionType, options.getDispositionType());
        }
        else {
            assertEquals(DISPOSITION_TYPE_ATTACHMENT, options.getDispositionType());
        }
        if (null != fileName) {
            assertEquals(fileName, options.getFileName());
        }
        else {
            assertNull(options.getFileName());
        }
    }

    private void verifyContentTypeHeader(DataRecordDownloadOptions options,
                                         String contentTypeHeader) {
        if (Strings.isNullOrEmpty(contentTypeHeader)) {
            assertNull(options.getContentTypeHeader());
        }
        else {
            assertEquals(contentTypeHeader, options.getContentTypeHeader());
        }
    }

    private void verifyContentDispositionHeader(DataRecordDownloadOptions options,
                                                String contentDispositionHeader) {
        if (Strings.isNullOrEmpty(contentDispositionHeader)) {
            assertNull(options.getContentDispositionHeader());
        }
        else {
            assertEquals(contentDispositionHeader, options.getContentDispositionHeader());
        }
    }

    private DataRecordDownloadOptions getOptions(String mimeType,
                                                 String encoding,
                                                 String fileName,
                                                 String dispositionType) {
        BinaryDownloadOptions.BinaryDownloadOptionsBuilder builder =
                BinaryDownloadOptions.builder();
        if (! Strings.isNullOrEmpty(mimeType)) {
            builder = builder.withMediaType(mimeType);
        }
        if (! Strings.isNullOrEmpty(encoding)) {
            builder = builder.withCharacterEncoding(encoding);
        }
        if (! Strings.isNullOrEmpty(fileName)) {
            builder = builder.withFileName(fileName);
        }
        if (!Strings.isNullOrEmpty(dispositionType)) {
            if (dispositionType.equals("attachment")) {
                builder = builder.withDispositionTypeAttachment();
            } else {
                builder = builder.withDispositionTypeInline();
            }
        }
        BinaryDownloadOptions options = builder.build();
        return DataRecordDownloadOptions.fromBlobDownloadOptions(
                new BlobDownloadOptions(options.getMediaType(),
                        options.getCharacterEncoding(),
                        options.getFileName(),
                        options.getDispositionType())
        );
    }

    private String getContentTypeHeader(String mimeType, String encoding) {
        return Strings.isNullOrEmpty(mimeType) ?
                null :
                (Strings.isNullOrEmpty(encoding) ?
                        mimeType :
                        Joiner.on("; charset=").join(mimeType, encoding)
                );
    }

    private String getContentDispositionHeader(String fileName, String dispositionType) {
        if (Strings.isNullOrEmpty(fileName)) {
            if (dispositionType.equals(DISPOSITION_TYPE_ATTACHMENT)) {
                return DISPOSITION_TYPE_ATTACHMENT;
            }
            return null;
        }

        if (Strings.isNullOrEmpty(dispositionType)) {
            dispositionType = DISPOSITION_TYPE_INLINE;
        }
        String fileNameStar = new String(fileName.getBytes(StandardCharsets.UTF_8));
        return String.format("%s; filename=\"%s\"; filename*=UTF-8''%s",
                dispositionType, fileName, fileNameStar);
    }

    @Test
    public void testConstruct() {
        BlobDownloadOptions blobDownloadOptions =
                new BlobDownloadOptions(
                        MIME_TYPE_TEXT_PLAIN,
                        ENCODING_UTF_8,
                        FILE_NAME_TEXT,
                        DISPOSITION_TYPE_ATTACHMENT
                );
        DataRecordDownloadOptions options =
                DataRecordDownloadOptions.fromBlobDownloadOptions(blobDownloadOptions);

        verifyOptions(options,
                MIME_TYPE_TEXT_PLAIN,
                ENCODING_UTF_8,
                FILE_NAME_TEXT,
                DISPOSITION_TYPE_ATTACHMENT);
    }

    @Test
    public void testDefault() {
        verifyOptions(DataRecordDownloadOptions.DEFAULT,
                null,
                null,
                null,
                DISPOSITION_TYPE_INLINE);
        verifyOptions(DataRecordDownloadOptions
                .fromBlobDownloadOptions(BlobDownloadOptions.DEFAULT),
                null,
                null,
                null,
                DISPOSITION_TYPE_INLINE);
        BinaryDownloadOptions binaryDownloadOptions = BinaryDownloadOptions.DEFAULT;
        BlobDownloadOptions blobDownloadOptions = new BlobDownloadOptions(
                binaryDownloadOptions.getMediaType(),
                binaryDownloadOptions.getCharacterEncoding(),
                binaryDownloadOptions.getFileName(),
                binaryDownloadOptions.getDispositionType()
        );
        verifyOptions(DataRecordDownloadOptions.fromBlobDownloadOptions(blobDownloadOptions),
                null,
                null,
                null,
                DISPOSITION_TYPE_INLINE);
    }

    @Test
    public void testConstructFromNullThrowsException() {
        try {
            DataRecordDownloadOptions.fromBlobDownloadOptions(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testFromBinaryDownloadOptions() {
        BinaryDownloadOptions binaryDownloadOptions =
                BinaryDownloadOptions.builder()
                        .withMediaType(MIME_TYPE_TEXT_PLAIN)
                        .withCharacterEncoding(ENCODING_UTF_8)
                        .withFileName(FILE_NAME_TEXT)
                        .withDispositionTypeAttachment()
                        .build();
        BlobDownloadOptions blobDownloadOptions =
                new BlobDownloadOptions(
                        binaryDownloadOptions.getMediaType(),
                        binaryDownloadOptions.getCharacterEncoding(),
                        binaryDownloadOptions.getFileName(),
                        binaryDownloadOptions.getDispositionType()
                );
    }

    @Test
    public void testGetContentTypeHeader() {
        for (String mimeType : Lists.newArrayList(MIME_TYPE_TEXT_PLAIN, MIME_TYPE_IMAGE_PNG)) {
            for (String encoding : Lists.newArrayList(ENCODING_UTF_8, ENCODING_ISO_8859_1)) {
                verifyContentTypeHeader(
                        getOptions(mimeType, encoding, null, null),
                        getContentTypeHeader(mimeType, encoding)
                );
            }
        }
    }

    @Test
    public void testGetContentTypeHeaderWithNoEncoding() {
        verifyContentTypeHeader(
                getOptions(MIME_TYPE_IMAGE_PNG, null, null, null),
                MIME_TYPE_IMAGE_PNG
        );
    }

    @Test
    public void testGetContentTypeHeaderWithNoMimeType() {
        verifyContentTypeHeader(
                getOptions(null, ENCODING_ISO_8859_1, null, null),
                null
        );
    }

    @Test
    public void testGetContentTypeHeaderWithNoMimeTypeOrEncoding() {
        verifyContentTypeHeader(
                getOptions(null, null, null, null),
                null
        );
    }

    @Test
    public void testGetContentDisposition() {
        for (String fileName : Lists.newArrayList(FILE_NAME_IMAGE, FILE_NAME_TEXT)) {
            for (String dispositionType : Lists.newArrayList(DISPOSITION_TYPE_INLINE, DISPOSITION_TYPE_ATTACHMENT)) {
                verifyContentDispositionHeader(
                        getOptions(null, null, fileName, dispositionType),
                        getContentDispositionHeader(fileName, dispositionType)
                );
            }
        }
    }

    @Test
    public void testGetContentDispositionWithNoDispositionType() {
        // Ensures that the default disposition type is "inline"
        verifyContentDispositionHeader(
                getOptions(null, null, FILE_NAME_IMAGE, null),
                getContentDispositionHeader(FILE_NAME_IMAGE, DISPOSITION_TYPE_INLINE)
        );
    }

    @Test
    public void testGetContentDispositionWithNoFileName() {
        verifyContentDispositionHeader(
                getOptions(null, null, null, DISPOSITION_TYPE_INLINE),
                null
        );
        verifyContentDispositionHeader(
                getOptions(null, null, null, DISPOSITION_TYPE_ATTACHMENT),
                DISPOSITION_TYPE_ATTACHMENT
        );
    }

    @Test
    public void testGetContentDispositionWithNoDispositionTypeOrFileName() {
        verifyContentDispositionHeader(
                getOptions(null, null, null, null),
                null
        );
    }
}
