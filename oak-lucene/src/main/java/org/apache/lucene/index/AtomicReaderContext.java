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

import java.util.Collections;
import java.util.List;

/**
 * {@link IndexReaderContext} for {@link AtomicReader} instances.
 */
public final class AtomicReaderContext extends IndexReaderContext {
  /** The readers ord in the top-level's leaves array */
  public final int ord;
  /** The readers absolute doc base */
  public final int docBase;
  
  private final AtomicReader reader;
  private final List<AtomicReaderContext> leaves;
  
  /**
   * Creates a new {@link AtomicReaderContext} 
   */    
  AtomicReaderContext(CompositeReaderContext parent, AtomicReader reader,
      int ord, int docBase, int leafOrd, int leafDocBase) {
    super(parent, ord, docBase);
    this.ord = leafOrd;
    this.docBase = leafDocBase;
    this.reader = reader;
    this.leaves = isTopLevel ? Collections.singletonList(this) : null;
  }
  
  AtomicReaderContext(AtomicReader atomicReader) {
    this(null, atomicReader, 0, 0, 0, 0);
  }
  
  @Override
  public List<AtomicReaderContext> leaves() {
    if (!isTopLevel) {
      throw new UnsupportedOperationException("This is not a top-level context.");
    }
    assert leaves != null;
    return leaves;
  }
  
  @Override
  public List<IndexReaderContext> children() {
    return null;
  }
  
  @Override
  public AtomicReader reader() {
    return reader;
  }
}