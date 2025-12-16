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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class TikaParserConfig {

    private static final Logger log = LoggerFactory.getLogger(TikaParserConfig.class);

    /**
     * Determines the set of MediaType which have been configured with an EmptyParser.
     *
     * @param configStream stream for tika config
     * @return set of MediaTypes which are not indexed
     */
    public static Set<MediaType> getNonIndexedMediaTypes(InputStream configStream) throws
            TikaException, IOException, SAXException {
        Set<MediaType> result = new HashSet<>();
        TikaConfig config = new TikaConfig(configStream);
        if (config.getParser() instanceof org.apache.tika.parser.CompositeParser) {
            // pick the (decorated) empty parser
            Optional<Parser> emptyParser = ((org.apache.tika.parser.CompositeParser) config.getParser()).getAllComponentParsers().stream()
                    .filter(p -> isEmptyParser(p))
                    .findFirst();
            if (emptyParser.isPresent()) {
                emptyParser.get().getSupportedTypes(new ParseContext()).forEach(result::add);
            }
        } else {
            log.debug("Tika CompositeParser not used, no parsers configured via custom tika config");
        }
        return result;
    }

    /**
     * Returns true if the given parser is an EmptyParser or decorates an EmptyParser.
     * @param parser
     * @return {@code true} if the given parser is an EmptyParser or decorates an EmptyParser
     */
    private static boolean isEmptyParser(Parser parser) {
        if (parser instanceof org.apache.tika.parser.EmptyParser) {
            return true;
        } else if (parser instanceof org.apache.tika.parser.ParserDecorator) {
            return isEmptyParser(((ParserDecorator) parser).getWrappedParser());
        }
        return false;
    }
}
