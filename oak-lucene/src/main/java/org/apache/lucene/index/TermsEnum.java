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
import java.util.Comparator;

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/** Iterator to seek ({@link #seekCeil(BytesRef)}, {@link
 * #seekExact(BytesRef)}) or step through ({@link
 * #next} terms to obtain frequency information ({@link
 * #docFreq}), {@link DocsEnum} or {@link
 * DocsAndPositionsEnum} for the current term ({@link
 * #docs}.
 * 
 * <p>Term enumerations are always ordered by
 * {@link #getComparator}.  Each term in the enumeration is
 * greater than the one before it.</p>
 *
 * <p>The TermsEnum is unpositioned when you first obtain it
 * and you must first successfully call {@link #next} or one
 * of the <code>seek</code> methods.
 *
 * @lucene.experimental */
public abstract class TermsEnum implements BytesRefIterator {

  private AttributeSource atts = null;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected TermsEnum() {
  }

  /** Returns the related attributes. */
  public AttributeSource attributes() {
    if (atts == null) atts = new AttributeSource();
    return atts;
  }
  
  /** Represents returned result from {@link #seekCeil}. */
  public static enum SeekStatus {
    /** The term was not found, and the end of iteration was hit. */
    END,
    /** The precise term was found. */
    FOUND,
    /** A different term was found after the requested term */
    NOT_FOUND
  };

  /** Attempts to seek to the exact term, returning
   *  true if the term is found.  If this returns false, the
   *  enum is unpositioned.  For some codecs, seekExact may
   *  be substantially faster than {@link #seekCeil}. */
  public boolean seekExact(BytesRef text) throws IOException {
    return seekCeil(text) == SeekStatus.FOUND;
  }

  /** Seeks to the specified term, if it exists, or to the
   *  next (ceiling) term.  Returns SeekStatus to
   *  indicate whether exact term was found, a different
   *  term was found, or EOF was hit.  The target term may
   *  be before or after the current term.  If this returns
   *  SeekStatus.END, the enum is unpositioned. */
  public abstract SeekStatus seekCeil(BytesRef text) throws IOException;

  /** Seeks to the specified term by ordinal (position) as
   *  previously returned by {@link #ord}.  The target ord
   *  may be before or after the current ord, and must be
   *  within bounds. */
  public abstract void seekExact(long ord) throws IOException;

  /**
   * Expert: Seeks a specific position by {@link TermState} previously obtained
   * from {@link #termState()}. Callers should maintain the {@link TermState} to
   * use this method. Low-level implementations may position the TermsEnum
   * without re-seeking the term dictionary.
   * <p>
   * Seeking by {@link TermState} should only be used iff the state was obtained 
   * from the same {@link TermsEnum} instance. 
   * <p>
   * NOTE: Using this method with an incompatible {@link TermState} might leave
   * this {@link TermsEnum} in undefined state. On a segment level
   * {@link TermState} instances are compatible only iff the source and the
   * target {@link TermsEnum} operate on the same field. If operating on segment
   * level, TermState instances must not be used across segments.
   * <p>
   * NOTE: A seek by {@link TermState} might not restore the
   * {@link AttributeSource}'s state. {@link AttributeSource} states must be
   * maintained separately if this method is used.
   * @param term the term the TermState corresponds to
   * @param state the {@link TermState}
   * */
  public void seekExact(BytesRef term, TermState state) throws IOException {
    if (!seekExact(term)) {
      throw new IllegalArgumentException("term=" + term + " does not exist");
    }
  }

  /** Returns current term. Do not call this when the enum
   *  is unpositioned. */
  public abstract BytesRef term() throws IOException;

  /** Returns ordinal position for current term.  This is an
   *  optional method (the codec may throw {@link
   *  UnsupportedOperationException}).  Do not call this
   *  when the enum is unpositioned. */
  public abstract long ord() throws IOException;

  /** Returns the number of documents containing the current
   *  term.  Do not call this when the enum is unpositioned.
   *  {@link SeekStatus#END}.*/
  public abstract int docFreq() throws IOException;

  /** Returns the total number of occurrences of this term
   *  across all documents (the sum of the freq() for each
   *  doc that has this term).  This will be -1 if the
   *  codec doesn't support this measure.  Note that, like
   *  other term measures, this measure does not take
   *  deleted documents into account. */
  public abstract long totalTermFreq() throws IOException;

  /** Get {@link DocsEnum} for the current term.  Do not
   *  call this when the enum is unpositioned.  This method
   *  will not return null.
   *  
   * @param liveDocs unset bits are documents that should not
   * be returned
   * @param reuse pass a prior DocsEnum for possible reuse */
  public final DocsEnum docs(Bits liveDocs, DocsEnum reuse) throws IOException {
    return docs(liveDocs, reuse, DocsEnum.FLAG_FREQS);
  }

  /** Get {@link DocsEnum} for the current term, with
   *  control over whether freqs are required.  Do not
   *  call this when the enum is unpositioned.  This method
   *  will not return null.
   *  
   * @param liveDocs unset bits are documents that should not
   * be returned
   * @param reuse pass a prior DocsEnum for possible reuse
   * @param flags specifies which optional per-document values
   *        you require; see {@link DocsEnum#FLAG_FREQS} 
   * @see #docs(Bits, DocsEnum, int) */
  public abstract DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException;

  /** Get {@link DocsAndPositionsEnum} for the current term.
   *  Do not call this when the enum is unpositioned.  This
   *  method will return null if positions were not
   *  indexed.
   *  
   *  @param liveDocs unset bits are documents that should not
   *  be returned
   *  @param reuse pass a prior DocsAndPositionsEnum for possible reuse
   *  @see #docsAndPositions(Bits, DocsAndPositionsEnum, int) */
  public final DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse) throws IOException {
    return docsAndPositions(liveDocs, reuse, DocsAndPositionsEnum.FLAG_OFFSETS | DocsAndPositionsEnum.FLAG_PAYLOADS);
  }

  /** Get {@link DocsAndPositionsEnum} for the current term,
   *  with control over whether offsets and payloads are
   *  required.  Some codecs may be able to optimize their
   *  implementation when offsets and/or payloads are not required.
   *  Do not call this when the enum is unpositioned.  This
   *  will return null if positions were not indexed.

   *  @param liveDocs unset bits are documents that should not
   *  be returned
   *  @param reuse pass a prior DocsAndPositionsEnum for possible reuse
   *  @param flags specifies which optional per-position values you
   *         require; see {@link DocsAndPositionsEnum#FLAG_OFFSETS} and 
   *         {@link DocsAndPositionsEnum#FLAG_PAYLOADS}. */
  public abstract DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException;

  /**
   * Expert: Returns the TermsEnums internal state to position the TermsEnum
   * without re-seeking the term dictionary.
   * <p>
   * NOTE: A seek by {@link TermState} might not capture the
   * {@link AttributeSource}'s state. Callers must maintain the
   * {@link AttributeSource} states separately
   * 
   * @see TermState
   * @see #seekExact(BytesRef, TermState)
   */
  public TermState termState() throws IOException {
    return new TermState() {
      @Override
      public void copyFrom(TermState other) {
        throw new UnsupportedOperationException();
      }
    };
  }

  /** An empty TermsEnum for quickly returning an empty instance e.g.
   * in {@link org.apache.lucene.search.MultiTermQuery}
   * <p><em>Please note:</em> This enum should be unmodifiable,
   * but it is currently possible to add Attributes to it.
   * This should not be a problem, as the enum is always empty and
   * the existence of unused Attributes does not matter.
   */
  public static final TermsEnum EMPTY = new TermsEnum() {    
    @Override
    public SeekStatus seekCeil(BytesRef term) { return SeekStatus.END; }
    
    @Override
    public void seekExact(long ord) {}
    
    @Override
    public BytesRef term() {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return null;
    }
      
    @Override
    public int docFreq() {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public long totalTermFreq() {
      throw new IllegalStateException("this method should never be called");
    }
      
    @Override
    public long ord() {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) {
      throw new IllegalStateException("this method should never be called");
    }
      
    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) {
      throw new IllegalStateException("this method should never be called");
    }
      
    @Override
    public BytesRef next() {
      return null;
    }
    
    @Override // make it synchronized here, to prevent double lazy init
    public synchronized AttributeSource attributes() {
      return super.attributes();
    }

    @Override
    public TermState termState() {
      throw new IllegalStateException("this method should never be called");
    }

    @Override
    public void seekExact(BytesRef term, TermState state) {
      throw new IllegalStateException("this method should never be called");
    }
  };
}
