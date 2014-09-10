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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import javax.management.NotCompliantMBeanException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

public class LuceneIndexMBeanImpl extends AnnotatedStandardMBean implements LuceneIndexMBean {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IndexTracker indexTracker;

    public LuceneIndexMBeanImpl(IndexTracker indexTracker) throws NotCompliantMBeanException {
        super(LuceneIndexMBean.class);
        this.indexTracker = indexTracker;
    }

    @Override
    public TabularData getIndexStats() throws IOException {
        TabularDataSupport tds;
        try {
            TabularType tt = new TabularType(LuceneIndexMBeanImpl.class.getName(),
                    "Lucene Index Stats", IndexStats.TYPE, new String[]{"path"});
            tds = new TabularDataSupport(tt);
            Set<String> indexes = indexTracker.getIndexNodePaths();
            for (String path : indexes) {
                IndexNode indexNode = null;
                try {
                    indexNode = indexTracker.acquireIndexNode(path);
                    if (indexNode != null) {
                        IndexStats stats = new IndexStats(path, indexNode.getSearcher().getIndexReader());
                        tds.put(stats.toCompositeData());
                    }
                } finally {
                    if (indexNode != null) {
                        indexNode.release();
                    }
                }
            }
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
        return tds;
    }

    public void dumpIndexContent(String sourcePath, String destPath) throws IOException {
        IndexNode indexNode = null;
        try {
            if(sourcePath == null){
                sourcePath = "/";
            }

            indexNode = indexTracker.acquireIndexNode(sourcePath);
            if (indexNode != null) {
                log.info("Dumping Lucene directory content for [{}] to [{}]", sourcePath, destPath);
                Directory source = getDirectory(indexNode.getSearcher().getIndexReader());
                checkNotNull(source, "IndexSearcher not backed by DirectoryReader");
                Directory dest = FSDirectory.open(new File(destPath));
                for (String file : source.listAll()) {
                    source.copy(dest, file, file, IOContext.DEFAULT);
                }
            }
        } finally {
            if (indexNode != null) {
                indexNode.release();
            }
        }
    }

    private static class IndexStats {
        static final String[] FIELD_NAMES = new String[]{
                "path",
                "indexSizeStr",
                "indexSize",
                "numDocs",
                "maxDoc",
                "numDeletedDocs",
        };

        static final String[] FIELD_DESCRIPTIONS = new String[]{
                "Path",
                "Index size in human readable format",
                "Index size in bytes",
                "Number of documents in this index.",
                "The time and date for when the longest query took place",
                "Number of deleted documents",
        };

        @SuppressWarnings("rawtypes")
        static final OpenType[] FIELD_TYPES = new OpenType[]{
                SimpleType.STRING,
                SimpleType.STRING,
                SimpleType.LONG,
                SimpleType.INTEGER,
                SimpleType.INTEGER,
                SimpleType.INTEGER,
        };

        static final CompositeType TYPE = createCompositeType();

        static CompositeType createCompositeType() {
            try {
                return new CompositeType(
                        IndexStats.class.getName(),
                        "Composite data type for Lucene Index statistics",
                        IndexStats.FIELD_NAMES,
                        IndexStats.FIELD_DESCRIPTIONS,
                        IndexStats.FIELD_TYPES);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }

        private final String path;
        private final long indexSize;
        private final int numDocs;
        private final int maxDoc;
        private final int numDeletedDocs;
        private final String indexSizeStr;

        public IndexStats(String path, IndexReader indexReader) throws IOException {
            this.path = path;
            numDocs = indexReader.numDocs();
            maxDoc = indexReader.maxDoc();
            numDeletedDocs = indexReader.numDeletedDocs();
            indexSize = dirSize(getDirectory(indexReader));
            indexSizeStr = humanReadableByteCount(indexSize);
        }

        CompositeDataSupport toCompositeData() {
            Object[] values = new Object[]{
                    path,
                    indexSizeStr,
                    indexSize,
                    numDocs,
                    maxDoc,
                    numDeletedDocs
            };
            try {
                return new CompositeDataSupport(TYPE, FIELD_NAMES, values);
            } catch (OpenDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    //~---------------------------------------------------------< Internal >

    private static Directory getDirectory(IndexReader reader) {
        if (reader instanceof DirectoryReader) {
            return ((DirectoryReader) reader).directory();
        }
        return null;
    }

    private static long dirSize(Directory directory) throws IOException {
        long totalFileSize = 0L;
        String[] files = directory.listAll();
        if (files == null) {
            return totalFileSize;
        }
        for (String file : files) {
            totalFileSize += directory.fileLength(file);
        }
        return totalFileSize;
    }
}
