/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.index;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a DocFieldConsumer that inverts each field, separately, from a Document, and accepts a
 * InvertedTermsConsumer to process those terms.
 */

final class DocInverter extends DocFieldConsumer {

    final InvertedDocConsumer consumer;
    final InvertedDocEndConsumer endConsumer;

    final DocumentsWriterPerThread.DocState docState;

    public DocInverter(DocumentsWriterPerThread.DocState docState, InvertedDocConsumer consumer,
        InvertedDocEndConsumer endConsumer) {
        this.docState = docState;
        this.consumer = consumer;
        this.endConsumer = endConsumer;
    }

    @Override
    void flush(Map<String, DocFieldConsumerPerField> fieldsToFlush, SegmentWriteState state)
        throws IOException {

        Map<String, InvertedDocConsumerPerField> childFieldsToFlush = new HashMap<String, InvertedDocConsumerPerField>();
        Map<String, InvertedDocEndConsumerPerField> endChildFieldsToFlush = new HashMap<String, InvertedDocEndConsumerPerField>();

        for (Map.Entry<String, DocFieldConsumerPerField> fieldToFlush : fieldsToFlush.entrySet()) {
            DocInverterPerField perField = (DocInverterPerField) fieldToFlush.getValue();
            childFieldsToFlush.put(fieldToFlush.getKey(), perField.consumer);
            endChildFieldsToFlush.put(fieldToFlush.getKey(), perField.endConsumer);
        }

        consumer.flush(childFieldsToFlush, state);
        endConsumer.flush(endChildFieldsToFlush, state);
    }

    @Override
    public void startDocument() throws IOException {
        consumer.startDocument();
        endConsumer.startDocument();
    }

    @Override
    public void finishDocument() throws IOException {
        // TODO: allow endConsumer.finishDocument to also return
        // a DocWriter
        endConsumer.finishDocument();
        consumer.finishDocument();
    }

    @Override
    void abort() {
        try {
            consumer.abort();
        } finally {
            endConsumer.abort();
        }
    }

    @Override
    public DocFieldConsumerPerField addField(FieldInfo fi) {
        return new DocInverterPerField(this, fi);
    }
}
