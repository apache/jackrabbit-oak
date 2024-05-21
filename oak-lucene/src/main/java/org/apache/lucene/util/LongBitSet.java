/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.util;

import java.util.Arrays;

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

/**
 * BitSet of fixed length (numBits), backed by accessible ({@link #getBits}) long[], accessed with a
 * long index. Use it only if you intend to store more than 2.1B bits, otherwise you should use
 * {@link FixedBitSet}.
 *
 * @lucene.internal
 */
public final class LongBitSet {

    private final long[] bits;
    private final long numBits;
    private final int numWords;

    /**
     * If the given {@link LongBitSet} is large enough to hold {@code numBits}, returns the given
     * bits, otherwise returns a new {@link LongBitSet} which can hold the requested number of
     * bits.
     *
     * <p>
     * <b>NOTE:</b> the returned bitset reuses the underlying {@code long[]} of
     * the given {@code bits} if possible. Also, calling {@link #length()} on the returned bits may
     * return a value greater than {@code numBits}.
     */
    public static LongBitSet ensureCapacity(LongBitSet bits, long numBits) {
        if (numBits < bits.length()) {
            return bits;
        } else {
            int numWords = bits2words(numBits);
            long[] arr = bits.getBits();
            if (numWords >= arr.length) {
                arr = ArrayUtil.grow(arr, numWords + 1);
            }
            return new LongBitSet(arr, arr.length << 6);
        }
    }

    /**
     * returns the number of 64 bit words it would take to hold numBits
     */
    public static int bits2words(long numBits) {
        int numLong = (int) (numBits >>> 6);
        if ((numBits & 63) != 0) {
            numLong++;
        }
        return numLong;
    }

    public LongBitSet(long numBits) {
        this.numBits = numBits;
        bits = new long[bits2words(numBits)];
        numWords = bits.length;
    }

    public LongBitSet(long[] storedBits, long numBits) {
        this.numWords = bits2words(numBits);
        if (numWords > storedBits.length) {
            throw new IllegalArgumentException(
                "The given long array is too small  to hold " + numBits + " bits");
        }
        this.numBits = numBits;
        this.bits = storedBits;
    }

    /**
     * Returns the number of bits stored in this bitset.
     */
    public long length() {
        return numBits;
    }

    /**
     * Expert.
     */
    public long[] getBits() {
        return bits;
    }

    /**
     * Returns number of set bits.  NOTE: this visits every long in the backing bits array, and the
     * result is not internally cached!
     */
    public long cardinality() {
        return BitUtil.pop_array(bits, 0, bits.length);
    }

    public boolean get(long index) {
        assert index >= 0 && index < numBits : "index=" + index;
        int i = (int) (index >> 6);               // div 64
        // signed shift will keep a negative index and force an
        // array-index-out-of-bounds-exception, removing the need for an explicit check.
        int bit = (int) (index & 0x3f);           // mod 64
        long bitmask = 1L << bit;
        return (bits[i] & bitmask) != 0;
    }

    public void set(long index) {
        assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
        int wordNum = (int) (index >> 6);      // div 64
        int bit = (int) (index & 0x3f);     // mod 64
        long bitmask = 1L << bit;
        bits[wordNum] |= bitmask;
    }

    public boolean getAndSet(long index) {
        assert index >= 0 && index < numBits;
        int wordNum = (int) (index >> 6);      // div 64
        int bit = (int) (index & 0x3f);     // mod 64
        long bitmask = 1L << bit;
        boolean val = (bits[wordNum] & bitmask) != 0;
        bits[wordNum] |= bitmask;
        return val;
    }

    public void clear(long index) {
        assert index >= 0 && index < numBits;
        int wordNum = (int) (index >> 6);
        int bit = (int) (index & 0x03f);
        long bitmask = 1L << bit;
        bits[wordNum] &= ~bitmask;
    }

    public boolean getAndClear(long index) {
        assert index >= 0 && index < numBits;
        int wordNum = (int) (index >> 6);      // div 64
        int bit = (int) (index & 0x3f);     // mod 64
        long bitmask = 1L << bit;
        boolean val = (bits[wordNum] & bitmask) != 0;
        bits[wordNum] &= ~bitmask;
        return val;
    }

    /**
     * Returns the index of the first set bit starting at the index specified. -1 is returned if
     * there are no more set bits.
     */
    public long nextSetBit(long index) {
        assert index >= 0 && index < numBits;
        int i = (int) (index >> 6);
        final int subIndex = (int) (index & 0x3f);      // index within the word
        long word = bits[i] >> subIndex;  // skip all the bits to the right of index

        if (word != 0) {
            return index + Long.numberOfTrailingZeros(word);
        }

        while (++i < numWords) {
            word = bits[i];
            if (word != 0) {
                return (i << 6) + Long.numberOfTrailingZeros(word);
            }
        }

        return -1;
    }

    /**
     * Returns the index of the last set bit before or on the index specified. -1 is returned if
     * there are no more set bits.
     */
    public long prevSetBit(long index) {
        assert index >= 0 && index < numBits : "index=" + index + " numBits=" + numBits;
        int i = (int) (index >> 6);
        final int subIndex = (int) (index & 0x3f);  // index within the word
        long word = (bits[i] << (63 - subIndex));  // skip all the bits to the left of index

        if (word != 0) {
            return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
        }

        while (--i >= 0) {
            word = bits[i];
            if (word != 0) {
                return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
            }
        }

        return -1;
    }

    /**
     * this = this OR other
     */
    public void or(LongBitSet other) {
        assert
            other.numWords <= numWords :
            "numWords=" + numWords + ", other.numWords=" + other.numWords;
        int pos = Math.min(numWords, other.numWords);
        while (--pos >= 0) {
            bits[pos] |= other.bits[pos];
        }
    }

    /**
     * this = this XOR other
     */
    public void xor(LongBitSet other) {
        assert
            other.numWords <= numWords :
            "numWords=" + numWords + ", other.numWords=" + other.numWords;
        int pos = Math.min(numWords, other.numWords);
        while (--pos >= 0) {
            bits[pos] ^= other.bits[pos];
        }
    }

    /**
     * returns true if the sets have any elements in common
     */
    public boolean intersects(LongBitSet other) {
        int pos = Math.min(numWords, other.numWords);
        while (--pos >= 0) {
            if ((bits[pos] & other.bits[pos]) != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * this = this AND other
     */
    public void and(LongBitSet other) {
        int pos = Math.min(numWords, other.numWords);
        while (--pos >= 0) {
            bits[pos] &= other.bits[pos];
        }
        if (numWords > other.numWords) {
            Arrays.fill(bits, other.numWords, numWords, 0L);
        }
    }

    /**
     * this = this AND NOT other
     */
    public void andNot(LongBitSet other) {
        int pos = Math.min(numWords, other.bits.length);
        while (--pos >= 0) {
            bits[pos] &= ~other.bits[pos];
        }
    }

    // NOTE: no .isEmpty() here because that's trappy (ie,
    // typically isEmpty is low cost, but this one wouldn't
    // be)

    /**
     * Flips a range of bits
     *
     * @param startIndex lower index
     * @param endIndex   one-past the last bit to flip
     */
    public void flip(long startIndex, long endIndex) {
        assert startIndex >= 0 && startIndex < numBits;
        assert endIndex >= 0 && endIndex <= numBits;
        if (endIndex <= startIndex) {
            return;
        }

        int startWord = (int) (startIndex >> 6);
        int endWord = (int) ((endIndex - 1) >> 6);

        /*** Grrr, java shifting wraps around so -1L>>>64 == -1
         * for that reason, make sure not to use endmask if the bits to flip will
         * be zero in the last word (redefine endWord to be the last changed...)
         long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
         long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
         ***/

        long startmask = -1L << startIndex;
        long endmask =
            -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

        if (startWord == endWord) {
            bits[startWord] ^= (startmask & endmask);
            return;
        }

        bits[startWord] ^= startmask;

        for (int i = startWord + 1; i < endWord; i++) {
            bits[i] = ~bits[i];
        }

        bits[endWord] ^= endmask;
    }

    /**
     * Sets a range of bits
     *
     * @param startIndex lower index
     * @param endIndex   one-past the last bit to set
     */
    public void set(long startIndex, long endIndex) {
        assert startIndex >= 0 && startIndex < numBits;
        assert endIndex >= 0 && endIndex <= numBits;
        if (endIndex <= startIndex) {
            return;
        }

        int startWord = (int) (startIndex >> 6);
        int endWord = (int) ((endIndex - 1) >> 6);

        long startmask = -1L << startIndex;
        long endmask =
            -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

        if (startWord == endWord) {
            bits[startWord] |= (startmask & endmask);
            return;
        }

        bits[startWord] |= startmask;
        Arrays.fill(bits, startWord + 1, endWord, -1L);
        bits[endWord] |= endmask;
    }

    /**
     * Clears a range of bits.
     *
     * @param startIndex lower index
     * @param endIndex   one-past the last bit to clear
     */
    public void clear(long startIndex, long endIndex) {
        assert startIndex >= 0 && startIndex < numBits;
        assert endIndex >= 0 && endIndex <= numBits;
        if (endIndex <= startIndex) {
            return;
        }

        int startWord = (int) (startIndex >> 6);
        int endWord = (int) ((endIndex - 1) >> 6);

        long startmask = -1L << startIndex;
        long endmask =
            -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

        // invert masks since we are clearing
        startmask = ~startmask;
        endmask = ~endmask;

        if (startWord == endWord) {
            bits[startWord] &= (startmask | endmask);
            return;
        }

        bits[startWord] &= startmask;
        Arrays.fill(bits, startWord + 1, endWord, 0L);
        bits[endWord] &= endmask;
    }

    @Override
    public LongBitSet clone() {
        long[] bits = new long[this.bits.length];
        System.arraycopy(this.bits, 0, bits, 0, bits.length);
        return new LongBitSet(bits, numBits);
    }

    /**
     * returns true if both sets have the same bits set
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LongBitSet)) {
            return false;
        }
        LongBitSet other = (LongBitSet) o;
        if (numBits != other.length()) {
            return false;
        }
        return Arrays.equals(bits, other.bits);
    }

    @Override
    public int hashCode() {
        long h = 0;
        for (int i = numWords; --i >= 0; ) {
            h ^= bits[i];
            h = (h << 1) | (h >>> 63); // rotate left
        }
        // fold leftmost bits into right and add a constant to prevent
        // empty sets from returning 0, which is too common.
        return (int) ((h >> 32) ^ h) + 0x98761234;
    }
}
