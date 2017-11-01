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

/**
 * This class would populate some statistics from a reader. We want to be careful here such that
 * we only collect statistics which don't incur reads from the index i.e. we would only collect
 * stats that lucene would already have read into memory when the reader was opened.
 */
public class IndexStatistics {
    static final Logger LOG = LoggerFactory.getLogger(IndexStatistics.class);
    private final int numDocs;
    private final Map<String, Integer> numDocsForField;

    /**
     * @param reader {@link IndexReader} for which statistics need to be collected.
     */
    public IndexStatistics(IndexReader reader) {
        numDocs = reader.numDocs();

        Map<String, Integer> numDocsForField = Maps.newHashMap();

        Fields fields = null;
        try {
            fields = MultiFields.getFields(reader);
        } catch (IOException e) {
            LOG.warn("Couldn't open fields for reader ({}). Won't extract doc count per field", reader);
            numDocsForField = null;
        }

        if (fields != null) {
            for(String f : fields) {
                if (isPropertyField(f)) {
                    int docCntForField = numDocs;
                    try {
                        docCntForField = reader.getDocCount(f);
                    } catch (IOException e) {
                        LOG.warn("Couldn't read doc count for field {} via reader ({}). Would use numDocs for this field");
                    }
                    numDocsForField.put(f, docCntForField);
                }
            }
        }

        if (numDocsForField != null) {
            this.numDocsForField = Collections.unmodifiableMap(numDocsForField);
        } else {
            this.numDocsForField = null;
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
     *         for the given {@code field}.
     */
    public int getDocCountFor(String field) {
        int docCntForField = isPropertyField(field) ? 0 : -1;
        if (numDocsForField.containsKey(field)) {
            docCntForField = numDocsForField.get(field);
        }

        return docCntForField;
    }

    private boolean isPropertyField(String field) {
        return !field.startsWith(FieldNames.ANALYZED_FIELD_PREFIX)
                && !field.startsWith(FieldNames.FULLTEXT_RELATIVE_NODE)
                && !field.startsWith(":")
                && !field.endsWith("_facet");
    }
}
