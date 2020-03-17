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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;

import com.google.common.base.Strings;
import org.apache.tika.exception.TikaException;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class TikaParserConfig {
    private static final String EMPTY_PARSER = "org.apache.tika.parser.EmptyParser";

    /**
     * Determines the set of MediaType which have been configured with an EmptyParser.
     *
     * @param configStream stream for tika config
     * @return set of MediaTypes which are not indexed
     */
    public static Set<MediaType> getNonIndexedMediaTypes(InputStream configStream) throws
            TikaException, IOException, SAXException {
        Set<MediaType> result = new HashSet<>();
        Element element = getBuilder().parse(configStream).getDocumentElement();
        NodeList nodes = element.getElementsByTagName("parsers");
        if (nodes.getLength() == 1) {
            Node parentNode = nodes.item(0);
            NodeList parsersNodes = parentNode.getChildNodes();
            for (int i = 0; i < parsersNodes.getLength(); i++) {
                Node node = parsersNodes.item(i);
                if (node instanceof Element) {
                    String className = ((Element) node).getAttribute("class");
                    if (EMPTY_PARSER.equals(className)) {
                        NodeList mimes = ((Element) node).getElementsByTagName("mime");
                        parseMimeTypes(result, mimes);
                    }
                }
            }
        }
        return result;
    }


    private static void parseMimeTypes(Set<MediaType> result, NodeList mimes) {
        /*
        <parser class="org.apache.tika.parser.EmptyParser">
            <mime>application/x-archive</mime>
            <mime>application/x-bzip</mime>
            <mime>application/x-bzip2</mime>
        </parser>
        */
        for (int j = 0; j < mimes.getLength(); j++) {
            Node mime = mimes.item(j);
            if (mime instanceof Element) {
                String mimeValue = mime.getTextContent();
                mimeValue = Strings.emptyToNull(mimeValue);
                if (mimeValue != null) {
                    MediaType mediaType = MediaType.parse(mimeValue.trim());
                    if (mediaType != null) {
                        result.add(mediaType);
                    }
                }
            }
        }
    }

    private static DocumentBuilder getBuilder() throws TikaException {
        return new ParseContext().getDocumentBuilder();
    }
}
