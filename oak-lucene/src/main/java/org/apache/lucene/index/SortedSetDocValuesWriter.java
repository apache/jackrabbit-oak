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

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.AppendingDeltaPackedLongBuffer;
import org.apache.lucene.util.packed.AppendingPackedLongBuffer;
import org.apache.lucene.util.packed.PackedInts;

/** Buffers up pending byte[]s per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
class SortedSetDocValuesWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private AppendingPackedLongBuffer pending; // stream of all termIDs
  private AppendingDeltaPackedLongBuffer pendingCounts; // termIDs per doc
  private final Counter iwBytesUsed;
  private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
  private final FieldInfo fieldInfo;
  private int currentDoc;
  private int currentValues[] = new int[8];
  private int currentUpto = 0;
  private int maxCount = 0;

  public SortedSetDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = new AppendingPackedLongBuffer(PackedInts.COMPACT);
    pendingCounts = new AppendingDeltaPackedLongBuffer(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (value == null) {
      throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
    }
    
    if (docID != currentDoc) {
      finishCurrentDoc();
    }

    // Fill in any holes:
    while(currentDoc < docID) {
      pendingCounts.add(0); // no values
      currentDoc++;
    }

    addOneValue(value);
    updateBytesUsed();
  }
  
  // finalize currentDoc: this deduplicates the current term ids
  private void finishCurrentDoc() {
    Arrays.sort(currentValues, 0, currentUpto);
    int lastValue = -1;
    int count = 0;
    for (int i = 0; i < currentUpto; i++) {
      int termID = currentValues[i];
      // if its not a duplicate
      if (termID != lastValue) {
        pending.add(termID); // record the term id
        count++;
      }
      lastValue = termID;
    }
    // record the number of unique term ids for this doc
    pendingCounts.add(count);
    maxCount = Math.max(maxCount, count);
    currentUpto = 0;
    currentDoc++;
  }

  @Override
  public void finish(int maxDoc) {
    finishCurrentDoc();
    
    // fill in any holes
    for (int i = currentDoc; i < maxDoc; i++) {
      pendingCounts.add(0); // no values
    }
  }

  private void addOneValue(BytesRef value) {
    int termID = hash.add(value);
    if (termID < 0) {
      termID = -termID-1;
    } else {
      // reserve additional space for each unique value:
      // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
      //    TODO: can this same OOM happen in THPF?
      // 2. when flushing, we need 1 int per value (slot in the ordMap).
      iwBytesUsed.addAndGet(2 * RamUsageEstimator.NUM_BYTES_INT);
    }
    
    if (currentUpto == currentValues.length) {
      currentValues = ArrayUtil.grow(currentValues, currentValues.length+1);
      // reserve additional space for max # values per-doc
      // when flushing, we need an int[] to sort the mapped-ords within the doc
      iwBytesUsed.addAndGet((currentValues.length - currentUpto) * 2 * RamUsageEstimator.NUM_BYTES_INT);
    }
    
    currentValues[currentUpto] = termID;
    currentUpto++;
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void flush(SegmentWriteState state, DocValuesConsumer dvConsumer) throws IOException {
    final int maxDoc = state.segmentInfo.getDocCount();
    final int maxCountPerDoc = maxCount;
    assert pendingCounts.size() == maxDoc;
    final int valueCount = hash.size();

    final int[] sortedValues = hash.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    final int[] ordMap = new int[valueCount];

    for(int ord=0;ord<valueCount;ord++) {
      ordMap[sortedValues[ord]] = ord;
    }

    dvConsumer.addSortedSetField(fieldInfo,

                              // ord -> value
                              new Iterable<BytesRef>() {
                                @Override
                                public Iterator<BytesRef> iterator() {
                                  return new ValuesIterator(sortedValues, valueCount);
                                }
                              },
                              
                              // doc -> ordCount
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  return new OrdCountIterator(maxDoc);
                                }
                              },

                              // ords
                              new Iterable<Number>() {
                                @Override
                                public Iterator<Number> iterator() {
                                  return new OrdsIterator(ordMap, maxCountPerDoc);
                                }
                              });
  }

  @Override
  public void abort() {
  }
  
  // iterates over the unique values we have in ram
  private class ValuesIterator implements Iterator<BytesRef> {
    final int sortedValues[];
    final BytesRef scratch = new BytesRef();
    final int valueCount;
    int ordUpto;
    
    ValuesIterator(int sortedValues[], int valueCount) {
      this.sortedValues = sortedValues;
      this.valueCount = valueCount;
    }

    @Override
    public boolean hasNext() {
      return ordUpto < valueCount;
    }

    @Override
    public BytesRef next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      hash.get(sortedValues[ordUpto], scratch);
      ordUpto++;
      return scratch;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  // iterates over the ords for each doc we have in ram
  private class OrdsIterator implements Iterator<Number> {
    final AppendingPackedLongBuffer.Iterator iter = pending.iterator();
    final AppendingDeltaPackedLongBuffer.Iterator counts = pendingCounts.iterator();
    final int ordMap[];
    final long numOrds;
    long ordUpto;
    
    final int currentDoc[];
    int currentUpto;
    int currentLength;
    
    OrdsIterator(int ordMap[], int maxCount) {
      this.currentDoc = new int[maxCount];
      this.ordMap = ordMap;
      this.numOrds = pending.size();
    }
    
    @Override
    public boolean hasNext() {
      return ordUpto < numOrds;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      while (currentUpto == currentLength) {
        // refill next doc, and sort remapped ords within the doc.
        currentUpto = 0;
        currentLength = (int) counts.next();
        for (int i = 0; i < currentLength; i++) {
          currentDoc[i] = ordMap[(int) iter.next()];
        }
        Arrays.sort(currentDoc, 0, currentLength);
      }
      int ord = currentDoc[currentUpto];
      currentUpto++;
      ordUpto++;
      // TODO: make reusable Number
      return ord;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
  
  private class OrdCountIterator implements Iterator<Number> {
    final AppendingDeltaPackedLongBuffer.Iterator iter = pendingCounts.iterator();
    final int maxDoc;
    int docUpto;
    
    OrdCountIterator(int maxDoc) {
      this.maxDoc = maxDoc;
      assert pendingCounts.size() == maxDoc;
    }
    
    @Override
    public boolean hasNext() {
      return docUpto < maxDoc;
    }

    @Override
    public Number next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      docUpto++;
      // TODO: make reusable Number
      return iter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
