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

import javax.annotation.Nullable;

import org.apache.lucene.index.IndexableField;

import static com.google.common.base.Preconditions.checkNotNull;

class LuceneDoc implements LuceneDocInfo {
    final String indexPath;
    final String docPath;
    final Iterable<? extends IndexableField> doc;
    final boolean delete;
    private volatile boolean processed;

    public static LuceneDoc forUpdate(String indexPath, String path, Iterable<? extends IndexableField> doc){
        return new LuceneDoc(indexPath, path, checkNotNull(doc), false);
    }

    public static LuceneDoc forDelete(String indexPath, String path){
        return new LuceneDoc(indexPath, path, null, true);
    }

    private LuceneDoc(String indexPath, String path, @Nullable Iterable<? extends IndexableField> doc, boolean delete) {
        this.docPath = checkNotNull(path);
        this.indexPath = checkNotNull(indexPath);
        this.doc = doc;
        this.delete = delete;
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", indexPath, docPath);
    }

    public boolean isProcessed() {
        return processed;
    }

    public void markProcessed(){
        processed = true;
    }

    //~-------------------------------< LuceneDocInfo >

    @Override
    public String getIndexPath() {
        return indexPath;
    }

    @Override
    public String getDocPath() {
        return docPath;
    }
}
