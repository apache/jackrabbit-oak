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

/** A Scorer for OR like queries, counterpart of <code>ConjunctionScorer</code>.
 * This Scorer implements {@link Scorer#advance(int)} and uses advance() on the given Scorers. 
 */
class DisjunctionSumScorer extends DisjunctionScorer { 

  /** The number of subscorers that provide the current match. */
  protected int nrMatchers = -1;

  protected double score = Float.NaN;
  private final float[] coord;
  
  /** Construct a <code>DisjunctionScorer</code>.
   * @param weight The weight to be used.
   * @param subScorers Array of at least two subscorers.
   * @param coord Table of coordination factors
   */
  DisjunctionSumScorer(Weight weight, Scorer[] subScorers, float[] coord) throws IOException {
    super(weight, subScorers);

    if (numScorers <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }
    this.coord = coord;
  }
  
  @Override
  protected void afterNext() throws IOException {
    final Scorer sub = subScorers[0];
    doc = sub.docID();
    if (doc != NO_MORE_DOCS) {
      score = sub.score();
      nrMatchers = 1;
      countMatches(1);
      countMatches(2);
    }
  }
  
  // TODO: this currently scores, but so did the previous impl
  // TODO: remove recursion.
  // TODO: if we separate scoring, out of here, 
  // then change freq() to just always compute it from scratch
  private void countMatches(int root) throws IOException {
    if (root < numScorers && subScorers[root].docID() == doc) {
      nrMatchers++;
      score += subScorers[root].score();
      countMatches((root<<1)+1);
      countMatches((root<<1)+2);
    }
  }
  
  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #nextDoc()} is called the first time.
   */
  @Override
  public float score() throws IOException { 
    return (float)score * coord[nrMatchers]; 
  }

  @Override
  public int freq() throws IOException {
    return nrMatchers;
  }
}
