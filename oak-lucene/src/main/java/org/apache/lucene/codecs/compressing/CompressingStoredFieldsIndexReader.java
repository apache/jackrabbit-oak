/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.codecs.compressing;

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
import java.util.Arrays;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Random-access reader for {@link CompressingStoredFieldsIndexWriter}.
 * @lucene.internal
 */
public final class CompressingStoredFieldsIndexReader implements Cloneable {

  static long moveLowOrderBitToSign(long n) {
    return ((n >>> 1) ^ -(n & 1));
  }

  final int maxDoc;
  final int[] docBases;
  final long[] startPointers;
  final int[] avgChunkDocs;
  final long[] avgChunkSizes;
  final PackedInts.Reader[] docBasesDeltas; // delta from the avg
  final PackedInts.Reader[] startPointersDeltas; // delta from the avg

  // It is the responsibility of the caller to close fieldsIndexIn after this constructor
  // has been called
  CompressingStoredFieldsIndexReader(IndexInput fieldsIndexIn, SegmentInfo si) throws IOException {
    maxDoc = si.getDocCount();
    int[] docBases = new int[16];
    long[] startPointers = new long[16];
    int[] avgChunkDocs = new int[16];
    long[] avgChunkSizes = new long[16];
    PackedInts.Reader[] docBasesDeltas = new PackedInts.Reader[16];
    PackedInts.Reader[] startPointersDeltas = new PackedInts.Reader[16];

    final int packedIntsVersion = fieldsIndexIn.readVInt();

    int blockCount = 0;

    for (;;) {
      final int numChunks = fieldsIndexIn.readVInt();
      if (numChunks == 0) {
        break;
      }
      if (blockCount == docBases.length) {
        final int newSize = ArrayUtil.oversize(blockCount + 1, 8);
        docBases = Arrays.copyOf(docBases, newSize);
        startPointers = Arrays.copyOf(startPointers, newSize);
        avgChunkDocs = Arrays.copyOf(avgChunkDocs, newSize);
        avgChunkSizes = Arrays.copyOf(avgChunkSizes, newSize);
        docBasesDeltas = Arrays.copyOf(docBasesDeltas, newSize);
        startPointersDeltas = Arrays.copyOf(startPointersDeltas, newSize);
      }

      // doc bases
      docBases[blockCount] = fieldsIndexIn.readVInt();
      avgChunkDocs[blockCount] = fieldsIndexIn.readVInt();
      final int bitsPerDocBase = fieldsIndexIn.readVInt();
      if (bitsPerDocBase > 32) {
        throw new CorruptIndexException("Corrupted bitsPerDocBase (resource=" + fieldsIndexIn + ")");
      }
      docBasesDeltas[blockCount] = PackedInts.getReaderNoHeader(fieldsIndexIn, PackedInts.Format.PACKED, packedIntsVersion, numChunks, bitsPerDocBase);

      // start pointers
      startPointers[blockCount] = fieldsIndexIn.readVLong();
      avgChunkSizes[blockCount] = fieldsIndexIn.readVLong();
      final int bitsPerStartPointer = fieldsIndexIn.readVInt();
      if (bitsPerStartPointer > 64) {
        throw new CorruptIndexException("Corrupted bitsPerStartPointer (resource=" + fieldsIndexIn + ")");
      }
      startPointersDeltas[blockCount] = PackedInts.getReaderNoHeader(fieldsIndexIn, PackedInts.Format.PACKED, packedIntsVersion, numChunks, bitsPerStartPointer);

      ++blockCount;
    }

    this.docBases = Arrays.copyOf(docBases, blockCount);
    this.startPointers = Arrays.copyOf(startPointers, blockCount);
    this.avgChunkDocs = Arrays.copyOf(avgChunkDocs, blockCount);
    this.avgChunkSizes = Arrays.copyOf(avgChunkSizes, blockCount);
    this.docBasesDeltas = Arrays.copyOf(docBasesDeltas, blockCount);
    this.startPointersDeltas = Arrays.copyOf(startPointersDeltas, blockCount);
  }

  private int block(int docID) {
    int lo = 0, hi = docBases.length - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int midValue = docBases[mid];
      if (midValue == docID) {
        return mid;
      } else if (midValue < docID) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return hi;
  }

  private int relativeDocBase(int block, int relativeChunk) {
    final int expected = avgChunkDocs[block] * relativeChunk;
    final long delta = moveLowOrderBitToSign(docBasesDeltas[block].get(relativeChunk));
    return expected + (int) delta;
  }

  private long relativeStartPointer(int block, int relativeChunk) {
    final long expected = avgChunkSizes[block] * relativeChunk;
    final long delta = moveLowOrderBitToSign(startPointersDeltas[block].get(relativeChunk));
    return expected + delta;
  }

  private int relativeChunk(int block, int relativeDoc) {
    int lo = 0, hi = docBasesDeltas[block].size() - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int midValue = relativeDocBase(block, mid);
      if (midValue == relativeDoc) {
        return mid;
      } else if (midValue < relativeDoc) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return hi;
  }

  long getStartPointer(int docID) {
    if (docID < 0 || docID >= maxDoc) {
      throw new IllegalArgumentException("docID out of range [0-" + maxDoc + "]: " + docID);
    }
    final int block = block(docID);
    final int relativeChunk = relativeChunk(block, docID - docBases[block]);
    return startPointers[block] + relativeStartPointer(block, relativeChunk);
  }

  @Override
  public CompressingStoredFieldsIndexReader clone() {
    return this;
  }
  
  long ramBytesUsed() {
    long res = 0;
    
    for(PackedInts.Reader r : docBasesDeltas) {
      res += r.ramBytesUsed();
    }
    for(PackedInts.Reader r : startPointersDeltas) {
      res += r.ramBytesUsed();
    }

    res += RamUsageEstimator.sizeOf(docBases);
    res += RamUsageEstimator.sizeOf(startPointers);
    res += RamUsageEstimator.sizeOf(avgChunkDocs); 
    res += RamUsageEstimator.sizeOf(avgChunkSizes);

    return res;
  }

}
