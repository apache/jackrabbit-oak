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
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText.ExtractionResult;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

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

        cache.put(b, new ExtractedText(ExtractionResult.SUCCESS, "test hello"));

        text = cache.get("/a", "foo", b, false);
        assertEquals("test hello", text);
    }

    @Test
    public void cacheEnabledNonIdBlob() throws Exception {
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);

        Blob b = new ArrayBasedBlob("hello".getBytes());
        String text = cache.get("/a", "foo", b, false);
        assertNull(text);

        cache.put(b, new ExtractedText(ExtractionResult.SUCCESS, "test hello"));

        text = cache.get("/a", "foo", b, false);
        assertNull(text);
    }

    @Test
    public void cacheEnabledErrorInTextExtraction() throws Exception {
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);

        Blob b = new IdBlob("hello", "a");
        String text = cache.get("/a", "foo", b, false);
        assertNull(text);

        cache.put(b, new ExtractedText(ExtractionResult.ERROR, "test hello"));

        text = cache.get("/a", "foo", b, false);
        assertEquals(LuceneIndexEditor.TEXT_EXTRACTION_ERROR, text);
    }

    @Test
    public void preExtractionNoReindexNoProvider() throws Exception{
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);

        Blob b = new IdBlob("hello", "a");
        String text = cache.get("/a", "foo", b, true);
        assertNull(text);
    }

    @Test
    public void preExtractionNoReindex() throws Exception{
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);
        PreExtractedTextProvider provider = mock(PreExtractedTextProvider.class);

        cache.setExtractedTextProvider(provider);
        Blob b = new IdBlob("hello", "a");
        String text = cache.get("/a", "foo", b, false);
        assertNull(text);

        verifyZeroInteractions(provider);
    }

    @Test
    public void preExtractionReindex() throws Exception{
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100);
        PreExtractedTextProvider provider = mock(PreExtractedTextProvider.class);

        cache.setExtractedTextProvider(provider);
        when(provider.getText(anyString(), any(Blob.class)))
                .thenReturn(new ExtractedText(ExtractionResult.SUCCESS, "bar"));
        Blob b = new IdBlob("hello", "a");
        String text = cache.get("/a", "foo", b, true);
        assertEquals("bar", text);
    }

    @Test
    public void preExtractionAlwaysUse() throws Exception{
        ExtractedTextCache cache = new ExtractedTextCache(10 * FileUtils.ONE_MB, 100, true, null);
        PreExtractedTextProvider provider = mock(PreExtractedTextProvider.class);

        cache.setExtractedTextProvider(provider);
        when(provider.getText(anyString(), any(Blob.class)))
                .thenReturn(new ExtractedText(ExtractionResult.SUCCESS, "bar"));
        Blob b = new IdBlob("hello", "a");
        String text = cache.get("/a", "foo", b, false);
        assertEquals("bar", text);
    }

    @Test
    public void rememberTimeout() throws Exception{
        ExtractedTextCache cache = new ExtractedTextCache(0, 0, false, null);
        Blob b = new IdBlob("hello", "a");
        cache.put(b, ExtractedText.ERROR);
        assertNull(cache.get("/a", "foo", b, false));
        cache.putTimeout(b, ExtractedText.ERROR);
        assertEquals(LuceneIndexEditor.TEXT_EXTRACTION_ERROR, cache.get("/a", "foo", b, false));
    }

    @Test
    public void process() throws Throwable {
        ExtractedTextCache cache = new ExtractedTextCache(0, 0, false, null);
        try {
            cache.process("test", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    throw new OutOfMemoryError();
                }
            });
            fail();
        } catch (OutOfMemoryError e) {
            // expected
        }
        assertEquals(0, cache.getStatsMBean().getTimeoutCount());
        cache.setExtractionTimeoutMillis(10);
        long time = System.currentTimeMillis();
        try {
            cache.process("test", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    // this happens in the background, so doesn't block the test
                    Thread.sleep(10000);
                    return null;
                }
            });
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        time = System.currentTimeMillis() - time;
        assertTrue("" + time, time < 5000);
        assertEquals(1, cache.getStatsMBean().getTimeoutCount());
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
