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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class AheadOfTimeBlobDownloadingFlatFileStoreTest {

    @Test
    public void isEnabledForIndexes() {
        assertFalse(AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForIndexes(
                "",
                List.of("/oak:index/fooA-34")
        ));

        assertTrue(AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForIndexes(
                "/oak:index/foo",
                List.of("/oak:index/fooA-34")
        ));

        assertTrue(AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForIndexes(
                "/oak:index/foo",
                List.of("/oak:index/anotherIndex", "/oak:index/fooA-34")
        ));

        assertFalse(AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForIndexes(
                "/oak:index/foo",
                List.of("/oak:index/anotherIndex")
        ));

        assertTrue(AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForIndexes(
                "/oak:index/fooA-,/oak:index/fooB-",
                List.of("/oak:index/fooA-34")
        ));

        assertTrue(AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForIndexes(
                "/oak:index/fooA-, /oak:index/fooB-",
                List.of("/oak:index/anotherIndex", "/oak:index/fooA-34")
        ));

        assertFalse(AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForIndexes(
                "/oak:index/fooA-",
                List.of()
        ));
    }
}