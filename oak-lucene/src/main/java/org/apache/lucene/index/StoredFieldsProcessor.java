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
import java.util.Arrays;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * This is a StoredFieldsConsumer that writes stored fields.
 */
final class StoredFieldsProcessor extends StoredFieldsConsumer {

    StoredFieldsWriter fieldsWriter;
    final DocumentsWriterPerThread docWriter;
    int lastDocID;

    final DocumentsWriterPerThread.DocState docState;
    final Codec codec;

    public StoredFieldsProcessor(DocumentsWriterPerThread docWriter) {
        this.docWriter = docWriter;
        this.docState = docWriter.docState;
        this.codec = docWriter.codec;
    }

    private int numStoredFields;
    private IndexableField[] storedFields = new IndexableField[1];
    private FieldInfo[] fieldInfos = new FieldInfo[1];

    public void reset() {
        numStoredFields = 0;
        Arrays.fill(storedFields, null);
        Arrays.fill(fieldInfos, null);
    }

    @Override
    public void startDocument() {
        reset();
    }

    @Override
    public void flush(SegmentWriteState state) throws IOException {
        int numDocs = state.segmentInfo.getDocCount();
        if (numDocs > 0) {
            // It's possible that all documents seen in this segment
            // hit non-aborting exceptions, in which case we will
            // not have yet init'd the FieldsWriter:
            initFieldsWriter(state.context);
            fill(numDocs);
        }
        if (fieldsWriter != null) {
            boolean success = false;
            try {
                fieldsWriter.finish(state.fieldInfos, numDocs);
                success = true;
            } finally {
                if (success) {
                    IOUtils.close(fieldsWriter);
                } else {
                    IOUtils.closeWhileHandlingException(fieldsWriter);
                }
            }
        }
    }

    private synchronized void initFieldsWriter(IOContext context) throws IOException {
        if (fieldsWriter == null) {
            fieldsWriter = codec.storedFieldsFormat()
                                .fieldsWriter(docWriter.directory, docWriter.getSegmentInfo(),
                                    context);
            lastDocID = 0;
        }
    }


    @Override
    void abort() {
        reset();

        if (fieldsWriter != null) {
            fieldsWriter.abort();
            fieldsWriter = null;
            lastDocID = 0;
        }
    }

    /**
     * Fills in any hole in the docIDs
     */
    void fill(int docID) throws IOException {
        // We must "catch up" for all docs before us
        // that had no stored fields:
        while (lastDocID < docID) {
            fieldsWriter.startDocument(0);
            lastDocID++;
            fieldsWriter.finishDocument();
        }
    }

    @Override
    void finishDocument() throws IOException {
        assert docWriter.testPoint("StoredFieldsWriter.finishDocument start");

        initFieldsWriter(IOContext.DEFAULT);
        fill(docState.docID);

        if (fieldsWriter != null && numStoredFields > 0) {
            fieldsWriter.startDocument(numStoredFields);
            for (int i = 0; i < numStoredFields; i++) {
                fieldsWriter.writeField(fieldInfos[i], storedFields[i]);
            }
            fieldsWriter.finishDocument();
            lastDocID++;
        }

        reset();
        assert docWriter.testPoint("StoredFieldsWriter.finishDocument end");
    }

    @Override
    public void addField(int docID, IndexableField field, FieldInfo fieldInfo) {
        if (field.fieldType().stored()) {
            if (numStoredFields == storedFields.length) {
                int newSize = ArrayUtil.oversize(numStoredFields + 1,
                    RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                IndexableField[] newArray = new IndexableField[newSize];
                System.arraycopy(storedFields, 0, newArray, 0, numStoredFields);
                storedFields = newArray;

                FieldInfo[] newInfoArray = new FieldInfo[newSize];
                System.arraycopy(fieldInfos, 0, newInfoArray, 0, numStoredFields);
                fieldInfos = newInfoArray;
            }

            storedFields[numStoredFields] = field;
            fieldInfos[numStoredFields] = fieldInfo;
            numStoredFields++;

            assert docState.testPoint("StoredFieldsWriterPerThread.processFields.writeField");
        }
    }
}
