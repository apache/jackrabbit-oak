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

package org.apache.jackrabbit.oak.plugins.index.search.spi.binary;

import java.io.StringReader;
import java.util.Set;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.tika.mime.MediaType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TikaParserConfigTest {

    @Test
    public void emptyParser() throws Exception{
        String json = """
                {
                  "detectors": [
                    "type-detector"
                  ],
                  "parsers": [
                    "default-parser",
                    {
                      "empty-parser": {
                        "_mime-include": [
                          "application/x-archive",
                          "application/x-bzip"
                        ]
                      }
                    }
                  ]
                }""";
        Set<MediaType> types = TikaParserConfig.getNonIndexedMediaTypes(
                new ReaderInputStream(new StringReader(json), "UTF-8"));
        assertEquals(2, types.size());
        assertTrue(types.contains(MediaType.parse("application/x-archive")));
    }

}
