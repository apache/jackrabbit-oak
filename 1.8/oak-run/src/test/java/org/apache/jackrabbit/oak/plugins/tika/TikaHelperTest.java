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

import java.io.File;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TikaHelperTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void supportedTypes() throws Exception {
        TikaHelper tika = new TikaHelper(null);
        assertTrue(tika.isSupportedMediaType("text/plain"));
    }

    @Test
    public void indexedTypes() throws Exception {
        File config = temporaryFolder.newFile();
        String configText = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<properties>\n" +
                "  <detectors>\n" +
                "    <detector class=\"org.apache.tika.detect.DefaultDetector\"/>\n" +
                "  </detectors>\n" +
                "  <parsers>\n" +
                "    <parser class=\"org.apache.tika.parser.DefaultParser\"/>\n" +
                "    <parser class=\"org.apache.tika.parser.EmptyParser\">\n" +
                "      <mime>application/xml</mime>\n" +
                "    </parser>\n" +
                "  </parsers>\n" +
                "</properties>";
        Files.write(configText, config, Charsets.UTF_8);
        TikaHelper tika = new TikaHelper(config);
        assertFalse(tika.isIndexed("application/xml"));
    }

}