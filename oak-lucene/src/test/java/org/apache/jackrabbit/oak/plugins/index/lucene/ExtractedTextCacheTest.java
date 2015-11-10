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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExtractedTextCacheTest {

    @Test
    public void cacheDisabling() throws Exception {
        ExtractedTextCache cache = new ExtractedTextCache(0, 0);
        assertNull(cache.getCacheStats());
    }

    @Test
    public void cacheEnabled() throws Exception {
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);
        assertNotNull(cache.getCacheStats());

        Blob b = new IdBlob("hello", "a");
        String text = cache.get("/a", "foo", b, false);
        assertNull(text);

        cache.put(b, new ExtractedText(ExtractedText.ExtractionResult.SUCCESS, "test hello"));

        text = cache.get("/a", "foo", b, false);
        assertEquals("test hello", text);
    }

    @Test
    public void cacheEnabledNonIdBlob() throws Exception {
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);

        Blob b = new ArrayBasedBlob("hello".getBytes());
        String text = cache.get("/a", "foo", b, false);
        assertNull(text);

        cache.put(b, new ExtractedText(ExtractedText.ExtractionResult.SUCCESS, "test hello"));

        text = cache.get("/a", "foo", b, false);
        assertNull(text);
    }

    @Test
    public void cacheEnabledErrorInTextExtraction() throws Exception {
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);

        Blob b = new IdBlob("hello", "a");
        String text = cache.get("/a", "foo", b, false);
        assertNull(text);

        cache.put(b, new ExtractedText(ExtractedText.ExtractionResult.ERROR, "test hello"));

        text = cache.get("/a", "foo", b, false);
        assertNull(text);
    }


    private static class IdBlob extends ArrayBasedBlob {
        final String id;

        public IdBlob(String value, String id) {
            super(value.getBytes());
            this.id = id;
        }

        @Override
        public String getContentIdentity() {
            return id;
        }
    }
}
