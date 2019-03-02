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
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.junit.Test;

public class DataRecordDownloadOptionsTest {
    private static final String MEDIA_TYPE_IMAGE_PNG = "image/png";
    private static final String MEDIA_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CHARACTER_ENCODING_UTF_8 = "utf-8";
    private static final String CHARACTER_ENCODING_ISO_8859_1 = "ISO-8859-1";
    private static final String FILE_NAME_IMAGE = "amazing summer sunset.png";
    private static final String FILE_NAME_TEXT = "journal_entry_01-01-2000.txt";
    private static final String DISPOSITION_TYPE_INLINE = "inline";
    private static final String DISPOSITION_TYPE_ATTACHMENT = "attachment";

    private void verifyOptions(DataRecordDownloadOptions options,
                               String mediaType,
                               String characterEncoding,
                               String fileName,
                               String dispositionType) {
        assertNotNull(options);
        if (null != mediaType) {
            assertEquals(mediaType, options.getMediaType());
        }
        else {
            assertNull(options.getMediaType());
        }
        if (null != characterEncoding) {
            assertEquals(characterEncoding, options.getCharacterEncoding());
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

    private DataRecordDownloadOptions getOptions(String mediaType,
                                                 String characterEncoding,
                                                 String fileName,
                                                 String dispositionType) {
        if (null == dispositionType) {
            dispositionType = DataRecordDownloadOptions.DISPOSITION_TYPE_INLINE;
        }
        return DataRecordDownloadOptions.fromBlobDownloadOptions(
                new BlobDownloadOptions(mediaType,
                        characterEncoding,
                        fileName,
                        dispositionType)
        );
    }

    private String getContentTypeHeader(String mediaType, String characterEncoding) {
        return Strings.isNullOrEmpty(mediaType) ?
                null :
                (Strings.isNullOrEmpty(characterEncoding) ?
                        mediaType :
                        Joiner.on("; charset=").join(mediaType, characterEncoding)
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
//        This proper behavior is disabled due to https://github.com/Azure/azure-sdk-for-java/issues/2900
//        (see also https://issues.apache.org/jira/browse/OAK-8013).  We can re-enable the full test
//        once the issue is resolved.  -MR
//        return String.format("%s; filename=\"%s\"; filename*=UTF-8''%s",
//                dispositionType, fileName, fileNameStar);
        return String.format("%s; filename=\"%s\"", dispositionType, fileName);
    }

    @Test
    public void testConstruct() {
        BlobDownloadOptions blobDownloadOptions =
                new BlobDownloadOptions(
                        MEDIA_TYPE_TEXT_PLAIN,
                        CHARACTER_ENCODING_UTF_8,
                        FILE_NAME_TEXT,
                        DISPOSITION_TYPE_ATTACHMENT
                );
        DataRecordDownloadOptions options =
                DataRecordDownloadOptions.fromBlobDownloadOptions(blobDownloadOptions);

        verifyOptions(options,
                MEDIA_TYPE_TEXT_PLAIN,
                CHARACTER_ENCODING_UTF_8,
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
    public void testGetContentTypeHeader() {
        for (String mediaType : Lists.newArrayList(MEDIA_TYPE_TEXT_PLAIN, MEDIA_TYPE_IMAGE_PNG)) {
            for (String characterEncoding : Lists.newArrayList(CHARACTER_ENCODING_UTF_8, CHARACTER_ENCODING_ISO_8859_1)) {
                verifyContentTypeHeader(
                        getOptions(mediaType, characterEncoding, null, null),
                        getContentTypeHeader(mediaType, characterEncoding)
                );
            }
        }
    }

    @Test
    public void testGetContentTypeHeaderWithNoCharacterEncoding() {
        verifyContentTypeHeader(
                getOptions(MEDIA_TYPE_IMAGE_PNG, null, null, null),
                MEDIA_TYPE_IMAGE_PNG
        );
    }

    @Test
    public void testGetContentTypeHeaderWithNoMediaType() {
        verifyContentTypeHeader(
                getOptions(null, CHARACTER_ENCODING_ISO_8859_1, null, null),
                null
        );
    }

    @Test
    public void testGetContentTypeHeaderWithNoMediaTypeOrCharacterEncoding() {
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
