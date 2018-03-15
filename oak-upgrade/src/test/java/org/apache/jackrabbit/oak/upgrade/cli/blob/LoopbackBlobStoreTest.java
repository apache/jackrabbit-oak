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
package org.apache.jackrabbit.oak.upgrade.cli.blob;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

@SuppressWarnings("UnusedLabel")
@RunWith(JUnitParamsRunner.class)
public class LoopbackBlobStoreTest {

    @Test(expected = UnsupportedOperationException.class)
    public void writingBinariesIsNotSupported() throws IOException {
        given:
        {
            final BlobStore blobStore = new LoopbackBlobStore();

            when:
            {
                final String test = "Test";
                blobStore.writeBlob(adaptToUtf8InputStream(test));
            }
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void writingBinariesWithBlobOptsIsNotSupported() throws IOException {
        given:
        {
            final BlobStore blobStore = new LoopbackBlobStore();
            final BlobOptions blobOptions = new BlobOptions();

            when:
            {
                blobStore.writeBlob(adaptToUtf8InputStream("Test"),
                        blobOptions);
            }
        }
    }


    @Test
    @Parameters(method = "blobIds")
    public void getBlobIdShouldReturnTheSameValuePassedExceptOfNull(
            final String blobId) {
        given:
        {
            final BlobStore blobStore = new LoopbackBlobStore();
            expect:
            {
                assertEquals(blobId, blobStore.getBlobId(blobId));
            }
        }
    }


    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void getBlobIdShouldThrowAnExceptionWhenNullIsPassed() {
        given:
        {
            final BlobStore blobStore = new LoopbackBlobStore();
            when:
            {
                blobStore.getBlobId(null);
            }
        }
    }


    @Test
    @Parameters(method = "blobIds")
    public void getReferenceShouldReturnTheSameValuePassedExceptOfNull(
            final String blobId) {
        given:
        {
            final BlobStore blobStore = new LoopbackBlobStore();
            where:
            {
                expect:
                {
                    assertEquals(blobId, blobStore.getReference(blobId));
                }
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = NullPointerException.class)
    public void getReferenceShouldThrowAnExceptionWhenNullIsPassed() {
        given:
        {
            final BlobStore blobStore = new LoopbackBlobStore();
            when:
            {
                blobStore.getReference(null);
            }
        }
    }

    @Test
    @Parameters(method = "blobIds")
    public void getBlobLengthShouldAlwaysReturnRealLengthOfBlobThatWillBeReturned(
            final String blobId) throws IOException {
        given:
        {
            final BlobStore store = new LoopbackBlobStore();
            expect:
            {
                assertEquals(blobId.getBytes().length, store.getBlobLength(blobId));
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void getBlobLengthShouldAlwaysThrowAnExceptionWhenNullBlobIdIsPassed()
            throws IOException {
        given:
        {
            final BlobStore store = new LoopbackBlobStore();
            when:
            {
                store.getBlobLength(null);
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void getInputStreamShouldAlwaysThrowAnExceptionWhenNullBlobIdIsPassed()
            throws IOException {
        given:
        {
            final BlobStore store = new LoopbackBlobStore();
            when:
            {
                store.getInputStream(null);
            }
        }
    }

    @Test
    @Parameters(method = "blobIds")
    public void shouldAlwaysReturnStreamOfRequestedBlobIdUtf8BinRepresentation(
            final String blobId) throws IOException {
        given:
        {
            final String encoding = "UTF-8";
            final BlobStore store = new LoopbackBlobStore();
            when:
            {
                final InputStream inputStream = store.getInputStream(blobId);
                then:
                {
                    assertNotNull(inputStream);
                }
                and:
                {
                    final String actualInputStreamAsString = IOUtils.toString(
                            inputStream, encoding);
                    then:
                    {
                        assertEquals(actualInputStreamAsString, blobId);
                    }
                }
            }
        }
    }

    @Test
    @Parameters(method = "blobIdsReads")
    public void shouldAlwaysFillBufferWithRequestedBlobIdUtf8BinRepresentation(
            final String blobId,
            int offsetToRead,
            int bufSize,
            int bufOffset,
            int lengthToRead,
            final String expectedBufferContent,
            final int expectedNumberOfBytesRead) throws IOException {
        given:
        {
            final String encoding = "UTF-8";
            final BlobStore blobStore = new LoopbackBlobStore();
            final byte[] buffer = new byte[bufSize];
            when:
            {
                final int numberOfBytesRead = blobStore.readBlob(
                        blobId, offsetToRead, buffer, bufOffset, lengthToRead);
                and:
                {
                    final String actualInputStreamAsString = IOUtils.toString(
                            buffer, encoding);
                    then:
                    {
                        assertEquals(numberOfBytesRead,
                                expectedNumberOfBytesRead);
                        assertEquals(expectedBufferContent,
                                encodeBufferFreeSpace(actualInputStreamAsString));
                    }
                }
            }
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    @Parameters(method = "blobIdsFailedBufferReadsCases")
    public void getInputStreamShouldAlwaysReturnExceptionIfBufferTooSmall(
            final String blobId,
            int offsetToRead,
            int bufSize,
            int bufOffset,
            int lengthToRead) throws IOException {
        given:
        {
            final BlobStore store = new LoopbackBlobStore();
            final byte[] buffer = new byte[bufSize];
            when:
            {
                store.readBlob(
                        blobId, offsetToRead, buffer, bufOffset, lengthToRead);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "blobIdsFailedOffsetReadsCases")
    public void getInputStreamShouldAlwaysReturnExceptionIfBinaryOffsetIsBad(
            final String blobId,
            int offsetToRead,
            int bufSize,
            int bufOffset,
            int lengthToRead) throws IOException {
        given:
        {
            final BlobStore store = new LoopbackBlobStore();
            final byte[] buffer = new byte[bufSize];
            when:
            {
                store.readBlob(
                        blobId, offsetToRead, buffer, bufOffset, lengthToRead);
            }
        }
    }

    @SuppressWarnings("unused")
    private Object blobIdsReads() {
        return new Object[]{
                //blobId, offsetToRead, bufSize, bufOffset, lengthToRead, expectedBufferContent, expectedNumOfBytesRead
                new Object[]{
                        "",                                 0,  0, 0,   0, "",                      0},
                new Object[]{
                        "",                                 0,  0, 0,   1, "",                      0},
                new Object[]{
                        "IDX1",                             0,  4, 0,   4, "IDX1",                  4},
                new Object[]{
                        "IDX1",                             4,  0, 0,   4, "",                      0},
                new Object[]{
                        "IDX1",                             4,  4, 0,   4, "####",                  0},
                new Object[]{
                        "IDX1",                             0,  5, 0,   4, "IDX1#",                 4},
                new Object[]{
                        "IDX1",                             1,  4, 0,   3, "DX1#",                  3},
                new Object[]{
                        "IDX1",                             1,  4, 0,   4, "DX1#",                  3},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZYZYYXYZYZYXYZQ", 10, 20, 3,  10, "###XXXXYYZYZY#######", 10},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZY",              10, 20, 3,  10, "###XXXXYYZY#########",  8},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZY",              10, 20, 3,  10, "###XXXXYYZY#########",  8},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZY",              10, 11, 3,  10, "###XXXXYYZY",           8},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZY",              10, 11, 2,  10, "##XXXXYYZY#",           8},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZY",              10, 11, 1,  10, "#XXXXYYZY##",           8},
        };
    }

    @SuppressWarnings("unused")
    private Object blobIdsFailedBufferReadsCases() {
        return new Object[]{
                //blobId, offsetToRead, bufferSize, bufferOffset, lengthToRead
                new Object[]{
                        " ",                                0,  0,  0,   1},
                new Object[]{
                        "IDX1",                             0,  3,  0,   4},
                new Object[]{
                        "IDX1",                             1,  3,  2,   3},
                new Object[]{
                        "IDX1",                             1,  2,  0,   3},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZY",              10,  0, 30,  10},
        };
    }

    @SuppressWarnings("unused")
    private Object blobIdsFailedOffsetReadsCases() {
        return new Object[]{
                //blobId, offsetToRead, bufferSize, bufferOffset, lengthToRead
                new Object[]{
                        "",                                 1,  50, 0,   0},
                new Object[]{
                        "IDX1",                             5,  50, 0,   3},
                new Object[]{
                        "IDX1",                             6,  50, 0,   4},
                new Object[]{
                        "ID2XXXXXXXXXXXYYZY",              30,  50, 1,  10},
        };
    }


    @SuppressWarnings("unused")
    private Object blobIds() {
        return new Object[]{
                new Object[]{""},
                new Object[]{"IDX1"},
                new Object[]{"ID2XXXXXXXXXXXYYZYZYYXYZYZYXYZQ"},
                new Object[]{"ABCQ"}
        };
    }

    private String encodeBufferFreeSpace(final String actualInputStreamAsString) {
        return actualInputStreamAsString.replace('\0', '#');
    }

    private InputStream adaptToUtf8InputStream(final String string)
            throws IOException {
        return IOUtils.toInputStream(string,
                "UTF-8");
    }

}

