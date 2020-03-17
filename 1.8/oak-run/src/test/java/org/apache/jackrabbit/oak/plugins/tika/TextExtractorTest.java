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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TextWriter;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class TextExtractorTest {

    @Test
    public void basicWorking() throws Exception {
        MapTextWriter writer = new MapTextWriter();
        TextExtractor extractor = new TextExtractor(writer);

        List<BinaryResource> binaries = asList(
                bin("hello", "text/plain", "a"),
                bin("foo", "text/plain", "b")
        );

        extractor.extract(binaries);

        extractor.close();
        assertEquals(2, writer.data.size());
        assertEquals("foo", writer.data.get("b").trim());
    }

    private static BinaryResource bin(String text, String mime, String id) {
        return new BinaryResource(ByteSource.wrap(text.getBytes()), mime, null, id, id);
    }

    private static class MapTextWriter implements TextWriter {
        final Map<String, String> data = Maps.newConcurrentMap();

        @Override
        public void write(String blobId, String text) throws IOException {
            data.put(blobId, text);
        }

        @Override
        public void markEmpty(String blobId) {

        }

        @Override
        public void markError(String blobId) {

        }

        @Override
        public boolean isProcessed(String blobId) {
            return data.containsKey(blobId);
        }
    }
}
