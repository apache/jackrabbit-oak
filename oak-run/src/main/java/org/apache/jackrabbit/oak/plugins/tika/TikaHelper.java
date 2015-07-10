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
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

class TikaHelper {
    private static final String DEFAULT_TIKA_CONFIG = "/org/apache/jackrabbit/oak/plugins/index/lucene/tika-config.xml";
    private static final Logger log = LoggerFactory.getLogger(TikaHelper.class);

    private final AutoDetectParser parser;
    private final Set<MediaType> supportedMediaTypes;
    private static AtomicBoolean supportedTypesLogged = new AtomicBoolean();

    public TikaHelper(@Nullable File tikaConfig) throws IOException {
        try {
            parser =  new AutoDetectParser(getTikaConfig(tikaConfig));
            supportedMediaTypes = parser.getSupportedTypes(new ParseContext());
            logSupportedTypesOnce(supportedMediaTypes);
        } catch (TikaException e) {
            throw new RuntimeException(e);
        } catch (SAXException e) {
            throw new RuntimeException(e);
        }
    }

    public Parser getParser() {
        return parser;
    }

    public boolean isSupportedMediaType(String type) {
        return supportedMediaTypes.contains(MediaType.parse(type));
    }

    /**
     * This method should only be used for information purpose and not be relied
     * upon to determine if the given type is indexed or not. It relies on Tika
     * implementation detail to determine if a given type is meant to be indexed
     *
     * @param type mimeType to check
     * @return true if the given type is supported and indexed
     */
    public boolean isIndexed(String type) {
        if (!isSupportedMediaType(type)){
            return false;
        }

        MediaType mediaType = MediaType.parse(type);
        Parser p = getSupportingParser(parser, mediaType);
        if (p == null){
            return false;
        }
        p = unwrap(p);
        if (p instanceof EmptyParser){
            return false;
        }
        return true;
    }

    private static TikaConfig getTikaConfig(File tikaConfig) throws TikaException, IOException, SAXException {
        TikaConfig config;
        if (tikaConfig == null) {
            URL configUrl = TextExtractor.class.getResource(DEFAULT_TIKA_CONFIG);
            if (configUrl != null) {
                log.info("Loading default Tika config from {}", configUrl);
                config = new TikaConfig(configUrl);
            } else {
                log.info("Using default Tika config");
                config = TikaConfig.getDefaultConfig();
            }
        } else {
            log.info("Loading external Tika config from {}", tikaConfig);
            config = new TikaConfig(tikaConfig);
        }
        return config;
    }

    private static Parser getSupportingParser(Parser p, MediaType mediaType){
        if (p instanceof CompositeParser){
            Map<MediaType, Parser> parsers = ((CompositeParser) p).getParsers();
            return getSupportingParser(parsers.get(mediaType), mediaType);
        }
        return p;
    }

    private static Parser unwrap(Parser p){
        if (p instanceof ParserDecorator){
            return unwrap(((ParserDecorator) p).getWrappedParser());
        }
        return p;
    }

    private static void logSupportedTypesOnce(Set<MediaType> supportedMediaTypes) {
        boolean alreadyLogged = supportedTypesLogged.getAndSet(true);
        if (!alreadyLogged) {
            log.info("Supported media types {}", supportedMediaTypes);
        }
    }
}
