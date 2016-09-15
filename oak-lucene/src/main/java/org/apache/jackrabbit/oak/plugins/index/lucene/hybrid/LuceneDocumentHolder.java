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
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LuceneDocumentHolder {
    private static final Logger log = LoggerFactory.getLogger(LuceneDocumentHolder.class);
    public static final String NAME = "oak.lucene.documentHolder";

    private final ListMultimap<String, LuceneDoc> nrtIndexedList = ArrayListMultimap.create();
    private final ListMultimap<String, LuceneDoc> syncIndexedList = ArrayListMultimap.create();
    private boolean limitWarningLogged;

    public List<LuceneDoc> getNRTIndexedDocList(String indexPath) {
        return nrtIndexedList.get(indexPath);
    }

    public Iterable<LuceneDoc> getNRTIndexedDocs(){
        return nrtIndexedList.values();
    }

    public List<LuceneDoc> getSyncIndexedDocList(String indexPath) {
        return syncIndexedList.get(indexPath);
    }

    public Map<String, Collection<LuceneDoc>> getSyncIndexedDocs(){
        return syncIndexedList.asMap();
    }

    public boolean checkLimitAndLogWarning(int maxSize){
        if (nrtIndexedList.size() >= maxSize){
            if (!limitWarningLogged){
                log.warn("Number of in memory documents meant for hybrid indexing has " +
                        "exceeded limit [{}]. Some documents would be dropped", maxSize);
                limitWarningLogged = true;
            }
            return true;
        }
        return false;
    }
}
