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

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class LuceneDocumentHolder implements JournalProperty{
    private static final Logger log = LoggerFactory.getLogger(LuceneDocumentHolder.class);
    public static final String NAME = "luceneDocs";

    private final ListMultimap<String, LuceneDoc> nrtIndexedList = ArrayListMultimap.create();
    private final ListMultimap<String, LuceneDoc> syncIndexedList = ArrayListMultimap.create();
    private final ListMultimap<String, String> queuedNrtIndexedPath = ArrayListMultimap.create();
    private final ListMultimap<String, LuceneDoc> queuedSyncIndexedPath = ArrayListMultimap.create();
    private final int inMemoryDocsLimit;
    private final IndexingQueue documentQueue;
    private boolean limitWarningLogged;
    private boolean docAddedToQueue;
    private boolean schedulingDone;

    public LuceneDocumentHolder(@Nonnull IndexingQueue documentQueue, int inMemoryDocsLimit) {
        this.documentQueue = checkNotNull(documentQueue);
        this.inMemoryDocsLimit = inMemoryDocsLimit;
    }

    public Iterable<LuceneDoc> getNRTIndexedDocs(){
        return nrtIndexedList.values();
    }

    public Map<String, Collection<LuceneDoc>> getSyncIndexedDocs(){
        for (Map.Entry<String, LuceneDoc> e : queuedSyncIndexedPath.entries()){
            if (!e.getValue().isProcessed()){
                syncIndexedList.put(e.getKey(), e.getValue());
            }
        }
        return syncIndexedList.asMap();
    }

    public void add(boolean sync, LuceneDoc doc) {
        doc = checkNotNull(doc);
        //First try adding to queue in non blocking manner
        if (documentQueue.addIfNotFullWithoutWait(doc)){
            if (sync){
                queuedSyncIndexedPath.put(doc.indexPath, doc);
            } else {
                queuedNrtIndexedPath.put(doc.indexPath, doc.docPath);
            }
            docAddedToQueue = true;
        } else {
            //Queue is full so keep it in memory
            if (sync) {
                syncIndexedList.put(doc.indexPath, doc);
            } else {
                if (queueSizeWithinLimits()) {
                    nrtIndexedList.put(doc.indexPath, doc);
                }
            }
        }
    }

    public void done(String indexPath) {
        //Hints the queue to process the queued docs in batch
        if (docAddedToQueue && !schedulingDone) {
            documentQueue.scheduleQueuedDocsProcessing();
            schedulingDone = true;
        }
    }

    /**
     * Returns an iterable for all indexed paths handled by this holder instance. The paths
     * may be directly forwarded to the queue or held in memory for later processing
     */
    Iterable<? extends LuceneDocInfo> getAllLuceneDocInfo(){
        return Iterables.concat(nrtIndexedList.values(), syncIndexedList.values(),
                asLuceneDocInfo(queuedNrtIndexedPath), queuedSyncIndexedPath.values());
    }

    private boolean queueSizeWithinLimits(){
        if (nrtIndexedList.size() >= inMemoryDocsLimit){
            if (!limitWarningLogged){
                log.warn("Number of in memory documents meant for hybrid indexing has " +
                        "exceeded limit [{}]. Some documents would be dropped", inMemoryDocsLimit);
                limitWarningLogged = true;
            }
            return false;
        }
        return true;
    }

    private static Iterable<? extends LuceneDocInfo> asLuceneDocInfo(ListMultimap<String, String> docs) {
        return Iterables.transform(docs.entries(), new Function<Map.Entry<String, String>, LuceneDocInfo>() {
            @Override
            public LuceneDocInfo apply(final Map.Entry<String, String> input) {
                return new LuceneDocInfo() {
                    @Override
                    public String getIndexPath() {
                        return input.getKey();
                    }

                    @Override
                    public String getDocPath() {
                        return input.getValue();
                    }
                };
            }
        });
    }
}
