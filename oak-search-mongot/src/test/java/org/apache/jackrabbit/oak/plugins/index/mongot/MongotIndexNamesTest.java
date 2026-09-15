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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class MongotIndexNamesTest {

    @Test
    public void derivesStableSafeNamesFromRepositoryIndexPath() {
        String collectionName = MongotIndexNames.collectionName("/oak:index/siteSearch");

        assertEquals("oak_aeda7eb399971cf7e5f0c1c8cc1b55539b757575d2268baef3cdaa95da106b0c",
                collectionName);
        assertFalse(collectionName.contains("."));
        assertFalse(collectionName.contains("$"));
        assertTrue(collectionName.length() < 120);
        assertEquals(collectionName, MongotIndexNames.collectionName("/oak:index/siteSearch"));
        assertNotEquals(collectionName, MongotIndexNames.collectionName("/oak:index/otherSearch"));
    }

    @Test
    public void keepsCollectionAndSearchIndexNamespacesDistinct() {
        String indexPath = "/oak:index/siteSearch";

        assertEquals("search_aeda7eb399971cf7e5f0c1c8cc1b55539b757575d2268baef3cdaa95da106b0c",
                MongotIndexNames.searchIndexName(indexPath));
        assertNotEquals(MongotIndexNames.collectionName(indexPath), MongotIndexNames.searchIndexName(indexPath));
    }

    @Test
    public void addsAStableSafeSuffixForNonZeroCollectionGenerations() {
        String indexPath = "/oak:index/siteSearch";
        String base = MongotIndexNames.collectionName(indexPath);

        assertEquals(base, MongotIndexNames.collectionName(indexPath, 0));
        assertEquals(base + "__2a", MongotIndexNames.collectionName(indexPath, 42));
        assertEquals(base + "__ffffffffffffffff", MongotIndexNames.collectionName(indexPath, -1));
        assertNotEquals(MongotIndexNames.collectionName(indexPath, 1),
                MongotIndexNames.collectionName(indexPath, 2));
    }
}
