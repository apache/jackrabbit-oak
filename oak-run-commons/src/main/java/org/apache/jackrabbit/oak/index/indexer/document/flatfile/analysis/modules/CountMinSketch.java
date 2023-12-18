package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

/**
 * The count-min sketch.
 */
public class CountMinSketch {

    // number of hash functions (depth, number of rows)
    private final int k;

    // number of buckets (width, number of columns)
    private final int m;

    // how many bits to shift after each hash function
    // (to reduce the number of hash functions required)
    private final int shift;

    private final long[][] data;

    /**
     * Create a new instance.
     *
     * @param k the number of hash functions
     * @param m the number of buckets per hash function (must be a power of 2)
     */
    public CountMinSketch(int k, int m) {
        if (Integer.bitCount(m) != 1) {
            throw new IllegalArgumentException("Must be a power of 2: " + m);
        }
        if ((k & 1) == 0) {
            throw new IllegalArgumentException("Must be odd: " + k);
        }
        this.shift = Integer.bitCount(m - 1);
        if (shift * k > 64) {
            throw new IllegalArgumentException("Too many hash functions or buckets: " + k + " / " + m);
        }
        this.m = m;
        this.k = k;
        data = new long[k][m];
    }
    
    public long addAndEstimate(long hash) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < k; i++) {
            long x = data[i][(int) (hash & (m - 1))]++;
            min = Math.min(min, x);
            hash >>>= shift;
        }
        return min;
    }

}