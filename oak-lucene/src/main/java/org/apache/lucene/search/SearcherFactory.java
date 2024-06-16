/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.search;

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
import java.util.concurrent.ExecutorService; // javadocs

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter; // javadocs
import org.apache.lucene.index.IndexWriterConfig; // javadocs
import org.apache.lucene.search.similarities.Similarity; // javadocs

/**
 * Factory class used by {@link SearcherManager} to
 * create new IndexSearchers. The default implementation just creates 
 * an IndexSearcher with no custom behavior:
 * 
 * <pre class="prettyprint">
 *   public IndexSearcher newSearcher(IndexReader r) throws IOException {
 *     return new IndexSearcher(r);
 *   }
 * </pre>
 * 
 * You can pass your own factory instead if you want custom behavior, such as:
 * <ul>
 *   <li>Setting a custom scoring model: {@link IndexSearcher#setSimilarity(Similarity)}
 *   <li>Parallel per-segment search: {@link IndexSearcher#IndexSearcher(IndexReader, ExecutorService)}
 *   <li>Return custom subclasses of IndexSearcher (for example that implement distributed scoring)
 *   <li>Run queries to warm your IndexSearcher before it is used. Note: when using near-realtime search
 *       you may want to also {@link IndexWriterConfig#setMergedSegmentWarmer(IndexWriter.IndexReaderWarmer)} to warm
 *       newly merged segments in the background, outside of the reopen path.
 * </ul>
 * @lucene.experimental
 */
public class SearcherFactory {
  /** 
   * Returns a new IndexSearcher over the given reader. 
   */
  public IndexSearcher newSearcher(IndexReader reader) throws IOException {
    return new IndexSearcher(reader);
  }
}
