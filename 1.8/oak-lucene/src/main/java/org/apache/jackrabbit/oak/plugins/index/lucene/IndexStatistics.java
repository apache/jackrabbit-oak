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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.Maps;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames.isPropertyField;

/**
 * This class would populate some statistics from a reader. We want to be careful here such that
 * we only collect statistics which don't incur reads from the index i.e. we would only collect
 * stats that lucene would already have read into memory when the reader was opened.
 */
public class IndexStatistics {
    private static final Logger LOG = LoggerFactory.getLogger(IndexStatistics.class);
    private final int numDocs;
    private final Map<String, Integer> numDocsForField;
    private final boolean safelyInitialized;

    // For ease of tests as there didn't seem an easy way to make an IndexReader delegator
    // that would fail calls to reader on-demand.
    static boolean failReadingFields = false;
    static boolean failReadingSyntheticallyFalliableField = false;

    static final String SYNTHETICALLY_FALLIABLE_FIELD = "synthetically-falliable-field";

    /**
     * @param reader {@link IndexReader} for which statistics need to be collected.
     */
    IndexStatistics(IndexReader reader) {
        numDocs = reader.numDocs();

        Map<String, Integer> numDocsForField = Maps.newHashMap();

        Fields fields = null;
        try {
            if (failReadingFields) {
                throw new IOException("Synthetically fail to read fields");
            }
            fields = MultiFields.getFields(reader);
        } catch (IOException e) {
            LOG.warn("Couldn't open fields for reader ({}). Won't extract doc count per field", reader);
            numDocsForField = null;
        }


        if (fields != null) {
            for(String f : fields) {
                if (isPropertyField(f)) {
                    int docCntForField = -1;
                    try {
                        if (failReadingSyntheticallyFalliableField && SYNTHETICALLY_FALLIABLE_FIELD.equals(f)) {
                            throw new IOException("Synthetically fail to read count for field jcr:title");
                        }
                        docCntForField = reader.getDocCount(f);
                    } catch (IOException e) {
                        LOG.warn("Couldn't read doc count for field {} via reader ({}).");
                    }
                    numDocsForField.put(f, docCntForField);
                }
            }
        }

        if (numDocsForField != null) {
            this.numDocsForField = Collections.unmodifiableMap(numDocsForField);
            this.safelyInitialized = true;
        } else {
            this.numDocsForField = Collections.emptyMap();
            this.safelyInitialized = false;
        }
    }

    /**
     * @return number of documents in the index
     */
    public int numDocs() {
        return numDocs;
    }

    /**
     * @param field Index field for which number of indexed documents are to be return
     * @return number of indexed documents (without subtracting potentially deleted ones)
     *         for the given {@code field}.<br>
     *         -1: if index codec doesn't store doc-count-for-field statistics, OR <br>
     *             &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;reader threw an exception while reading fields, OR <br>
     *             &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;exception thrown while reading count for the field, OR <br>
     *             &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;doc-count is asked for a non-property field.
     */
    public int getDocCountFor(String field) {
        if (!safelyInitialized) {
            return -1;
        }

        int docCntForField = isPropertyField(field) ? 0 : -1;
        if (numDocsForField.containsKey(field)) {
            docCntForField = numDocsForField.get(field);
        }

        return docCntForField;
    }

}
