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

package org.apache.jackrabbit.oak.plugins.index.lucene.binary;

import java.io.StringReader;
import java.util.Set;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.tika.mime.MediaType;
import org.junit.Test;

import static org.junit.Assert.*;

public class TikaParserConfigTest {

    @Test
    public void emptyParser() throws Exception{
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<properties>\n" +
                "  <detectors>\n" +
                "    <detector class=\"org.apache.tika.detect.TypeDetector\"/>\n" +
                "  </detectors>\n" +
                "  <parsers>\n" +
                "    <parser class=\"org.apache.tika.parser.DefaultParser\"/>\n" +
                "    <parser class=\"org.apache.tika.parser.EmptyParser\">\n" +
                "      <mime>application/x-archive</mime>\n" +
                "      <mime>application/x-bzip</mime>\n" +
                "    </parser>\n" +
                "  </parsers>\n" +
                "</properties>";

        Set<MediaType> types = TikaParserConfig.getNonIndexedMediaTypes(
                new ReaderInputStream(new StringReader(xml), "UTF-8"));
        assertEquals(2, types.size());
        assertTrue(types.contains(MediaType.parse("application/x-archive")));
    }

}