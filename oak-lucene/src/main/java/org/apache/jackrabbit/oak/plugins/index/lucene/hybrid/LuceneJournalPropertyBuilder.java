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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalProperty;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LuceneJournalPropertyBuilder implements JournalPropertyBuilder<LuceneDocumentHolder>{
    private final static Logger log = LoggerFactory.getLogger(LuceneJournalPropertyBuilder.class);
    //Use HashMultimap to ensure that indexPath is not duplicated per node path
    private final Multimap<String, String> indexedNodes = HashMultimap.create();
    private boolean limitWarningLogged = false;
    private final int maxSize;

    LuceneJournalPropertyBuilder(int maxSize) {
        this.maxSize = maxSize;
    }

    //~---------------------------------< serialize >
    @Override
    public void addProperty(@Nullable LuceneDocumentHolder docHolder) {
        if (docHolder != null){
            for (LuceneDocInfo d : docHolder.getAllLuceneDocInfo()){
                if (sizeWithinLimits()) {
                    indexedNodes.put(d.getDocPath(), d.getIndexPath());
                }
            }
        }
    }

    @Override
    public String buildAsString() {
        JsopWriter json = new JsopBuilder();
        json.object();
        for (Map.Entry<String, Collection<String>> e : indexedNodes.asMap().entrySet()){
            json.key(e.getKey()).array();
            for (String v : e.getValue()){
                json.value(v);
            }
            json.endArray();
        }
        json.endObject();
        return json.toString();
    }

    //~---------------------------------< deserialize >

    @Override
    public void addSerializedProperty(@Nullable String json) {
        if (json == null || json.isEmpty()){
            return;
        }
        //TODO Add support for overflow
        JsopReader reader = new JsopTokenizer(json);
        reader.read('{');
        if (!reader.matches('}')) {
            do {
                String path = reader.readString();
                reader.read(':');
                reader.read('[');
                for (boolean first = true; !reader.matches(']'); first = false) {
                    if (!first) {
                        reader.read(',');
                    }
                    if (sizeWithinLimits()) {
                        indexedNodes.put(path, reader.readString());
                    }
                }
            } while (reader.matches(','));
            reader.read('}');
        }
        reader.read(JsopReader.END);
    }

    @Override
    public JournalProperty build() {
        return new IndexedPaths(indexedNodes);
    }

    private boolean sizeWithinLimits() {
        if (indexedNodes.size() >= maxSize){
            if (!limitWarningLogged){
                log.warn("Max size of {} reached. Further addition of index path data would be dropped", maxSize);
                limitWarningLogged = true;
            }
            return false;
        }
        return true;
    }
}
