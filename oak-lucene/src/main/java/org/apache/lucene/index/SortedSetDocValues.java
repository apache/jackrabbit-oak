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

import org.apache.lucene.util.BytesRef;

/**
 * A per-document set of presorted byte[] values.
 * <p>
 * Per-Document values in a SortedDocValues are deduplicated, dereferenced,
 * and sorted into a dictionary of unique values. A pointer to the
 * dictionary value (ordinal) can be retrieved for each document. Ordinals
 * are dense and in increasing sorted order.
 */
public abstract class SortedSetDocValues {
  
  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected SortedSetDocValues() {}

  /** When returned by {@link #nextOrd()} it means there are no more 
   * ordinals for the document.
   */
  public static final long NO_MORE_ORDS = -1;

  /** 
   * Returns the next ordinal for the current document (previously
   * set by {@link #setDocument(int)}.
   * @return next ordinal for the document, or {@link #NO_MORE_ORDS}. 
   *         ordinals are dense, start at 0, then increment by 1 for 
   *         the next value in sorted order. 
   */
  public abstract long nextOrd();
  
  /** 
   * Sets iteration to the specified docID 
   * @param docID document ID 
   */
  public abstract void setDocument(int docID);

  /** Retrieves the value for the specified ordinal.
   * @param ord ordinal to lookup
   * @param result will be populated with the ordinal's value
   * @see #nextOrd
   */
  public abstract void lookupOrd(long ord, BytesRef result);

  /**
   * Returns the number of unique values.
   * @return number of unique values in this SortedDocValues. This is
   *         also equivalent to one plus the maximum ordinal.
   */
  public abstract long getValueCount();


  /** An empty SortedDocValues which returns {@link #NO_MORE_ORDS} for every document */
  public static final SortedSetDocValues EMPTY = new SortedSetDocValues() {

    @Override
    public long nextOrd() {
      return NO_MORE_ORDS;
    }

    @Override
    public void setDocument(int docID) {}

    @Override
    public void lookupOrd(long ord, BytesRef result) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public long getValueCount() {
      return 0;
    }
  };

  /** If {@code key} exists, returns its ordinal, else
   *  returns {@code -insertionPoint-1}, like {@code
   *  Arrays.binarySearch}.
   *
   *  @param key Key to look up
   **/
  public long lookupTerm(BytesRef key) {
    BytesRef spare = new BytesRef();
    long low = 0;
    long high = getValueCount()-1;

    while (low <= high) {
      long mid = (low + high) >>> 1;
      lookupOrd(mid, spare);
      int cmp = spare.compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1);  // key not found.
  }
  
  /** 
   * Returns a {@link TermsEnum} over the values.
   * The enum supports {@link TermsEnum#ord()} and {@link TermsEnum#seekExact(long)}.
   */
  public TermsEnum termsEnum() {
    return new SortedSetDocValuesTermsEnum(this);
  }
}
