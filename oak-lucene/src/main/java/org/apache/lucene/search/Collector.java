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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;

/**
 * <p>Expert: Collectors are primarily meant to be used to
 * gather raw results from a search, and implement sorting
 * or custom result filtering, collation, etc. </p>
 *
 * <p>Lucene's core collectors are derived from Collector.
 * Likely your application can use one of these classes, or
 * subclass {@link TopDocsCollector}, instead of
 * implementing Collector directly:
 *
 * <ul>
 *      
 *   <li>{@link TopDocsCollector} is an abstract base class
 *   that assumes you will retrieve the top N docs,
 *   according to some criteria, after collection is
 *   done.  </li>
 *
 *   <li>{@link TopScoreDocCollector} is a concrete subclass
 *   {@link TopDocsCollector} and sorts according to score +
 *   docID.  This is used internally by the {@link
 *   IndexSearcher} search methods that do not take an
 *   explicit {@link Sort}. It is likely the most frequently
 *   used collector.</li>
 *
 *   <li>{@link TopFieldCollector} subclasses {@link
 *   TopDocsCollector} and sorts according to a specified
 *   {@link Sort} object (sort by field).  This is used
 *   internally by the {@link IndexSearcher} search methods
 *   that take an explicit {@link Sort}.
 *
 *   <li>{@link TimeLimitingCollector}, which wraps any other
 *   Collector and aborts the search if it's taken too much
 *   time.</li>
 *
 *   <li>{@link PositiveScoresOnlyCollector} wraps any other
 *   Collector and prevents collection of hits whose score
 *   is &lt;= 0.0</li>
 *
 * </ul>
 *
 * <p>Collector decouples the score from the collected doc:
 * the score computation is skipped entirely if it's not
 * needed.  Collectors that do need the score should
 * implement the {@link #setScorer} method, to hold onto the
 * passed {@link Scorer} instance, and call {@link
 * Scorer#score()} within the collect method to compute the
 * current hit's score.  If your collector may request the
 * score for a single hit multiple times, you should use
 * {@link ScoreCachingWrappingScorer}. </p>
 * 
 * <p><b>NOTE:</b> The doc that is passed to the collect
 * method is relative to the current reader. If your
 * collector needs to resolve this to the docID space of the
 * Multi*Reader, you must re-base it by recording the
 * docBase from the most recent setNextReader call.  Here's
 * a simple example showing how to collect docIDs into a
 * BitSet:</p>
 * 
 * <pre class="prettyprint">
 * IndexSearcher searcher = new IndexSearcher(indexReader);
 * final BitSet bits = new BitSet(indexReader.maxDoc());
 * searcher.search(query, new Collector() {
 *   private int docBase;
 * 
 *   <em>// ignore scorer</em>
 *   public void setScorer(Scorer scorer) {
 *   }
 *
 *   <em>// accept docs out of order (for a BitSet it doesn't matter)</em>
 *   public boolean acceptsDocsOutOfOrder() {
 *     return true;
 *   }
 * 
 *   public void collect(int doc) {
 *     bits.set(doc + docBase);
 *   }
 * 
 *   public void setNextReader(AtomicReaderContext context) {
 *     this.docBase = context.docBase;
 *   }
 * });
 * </pre>
 *
 * <p>Not all collectors will need to rebase the docID.  For
 * example, a collector that simply counts the total number
 * of hits would skip it.</p>
 * 
 * <p><b>NOTE:</b> Prior to 2.9, Lucene silently filtered
 * out hits with score <= 0.  As of 2.9, the core Collectors
 * no longer do that.  It's very unusual to have such hits
 * (a negative query boost, or function query returning
 * negative custom scores, could cause it to happen).  If
 * you need that behavior, use {@link
 * PositiveScoresOnlyCollector}.</p>
 *
 * @lucene.experimental
 * 
 * @since 2.9
 */
public abstract class Collector {
  
  /**
   * Called before successive calls to {@link #collect(int)}. Implementations
   * that need the score of the current document (passed-in to
   * {@link #collect(int)}), should save the passed-in Scorer and call
   * scorer.score() when needed.
   */
  public abstract void setScorer(Scorer scorer) throws IOException;
  
  /**
   * Called once for every document matching a query, with the unbased document
   * number.
   * <p>Note: The collection of the current segment can be terminated by throwing
   * a {@link CollectionTerminatedException}. In this case, the last docs of the
   * current {@link AtomicReaderContext} will be skipped and {@link IndexSearcher}
   * will swallow the exception and continue collection with the next leaf.
   * <p>
   * Note: This is called in an inner search loop. For good search performance,
   * implementations of this method should not call {@link IndexSearcher#doc(int)} or
   * {@link org.apache.lucene.index.IndexReader#document(int)} on every hit.
   * Doing so can slow searches by an order of magnitude or more.
   */
  public abstract void collect(int doc) throws IOException;

  /**
   * Called before collecting from each {@link AtomicReaderContext}. All doc ids in
   * {@link #collect(int)} will correspond to {@link IndexReaderContext#reader}.
   * 
   * Add {@link AtomicReaderContext#docBase} to the current  {@link IndexReaderContext#reader}'s
   * internal document id to re-base ids in {@link #collect(int)}.
   * 
   * @param context
   *          next atomic reader context
   */
  public abstract void setNextReader(AtomicReaderContext context) throws IOException;

  /**
   * Return <code>true</code> if this collector does not
   * require the matching docIDs to be delivered in int sort
   * order (smallest to largest) to {@link #collect}.
   *
   * <p> Most Lucene Query implementations will visit
   * matching docIDs in order.  However, some queries
   * (currently limited to certain cases of {@link
   * BooleanQuery}) can achieve faster searching if the
   * <code>Collector</code> allows them to deliver the
   * docIDs out of order.</p>
   *
   * <p> Many collectors don't mind getting docIDs out of
   * order, so it's important to return <code>true</code>
   * here.
   */
  public abstract boolean acceptsDocsOutOfOrder();
  
}
