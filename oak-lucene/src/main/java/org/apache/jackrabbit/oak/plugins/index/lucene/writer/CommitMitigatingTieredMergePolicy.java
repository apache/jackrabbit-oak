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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This {@link MergePolicy} extends Lucene's {@link org.apache.lucene.index.TieredMergePolicy} by providing mitigation
 *  to the aggressiveness of merges in case the index is under high commit load.
 *  That's because in the case of Oak we currently have that we store Lucene indexes in storage systems which require
 *  themselves some garbage collection task to be executed to get rid of deleted / unused files, similarly to what Lucene's
 *  merge does.
 *  So the bottom line is that with this {@link MergePolicy} we should have less but bigger merges, only after commit rate
 *  is under a certain threshold (in terms of added docs per sec and MBs per sec).
 *
 *  Auto tuning params:
 *  In this merge policy we would like to avoid having to adjust parameters by hand, but rather have them "auto tune".
 *  This means that the no. of merges should be mitigated with respect to a max commit rate (docs, mb), but also adapt to
 *  the average commit rate and anyway do not let the no. of segments explode.
 *
 */
public class CommitMitigatingTieredMergePolicy extends MergePolicy {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /** Default noCFSRatio.  If a merge's size is &gt;= 10% of
     *  the index, then we disable compound file for it.
     *  @see MergePolicy#setNoCFSRatio */
    public static final double DEFAULT_NO_CFS_RATIO = 0.1;

    private static final double DEFAULT_MAX_COMMIT_RATE_DOCS = 1000;
    private static final double DEFAULT_MAX_COMMIT_RATE_MB = 5;
    private static final int DEFAULT_MAX_NO_OF_SEGS_FOR_MITIGATION = 20;

    private int maxMergeAtOnce = 10;
    private long maxMergedSegmentBytes = 5 * 1024 * 1024 * 1024L;
    private int maxMergeAtOnceExplicit = 30;

    private long floorSegmentBytes = 2 * 1024 * 1024L;
    private double segsPerTier = 10.0;
    private double forceMergeDeletesPctAllowed = 10.0;
    private double reclaimDeletesWeight = 1.5;

    private double maxCommitRateDocs = DEFAULT_MAX_COMMIT_RATE_DOCS;
    private double maxCommitRateMB = DEFAULT_MAX_COMMIT_RATE_MB;
    private int maxNoOfSegsForMitigation = DEFAULT_MAX_NO_OF_SEGS_FOR_MITIGATION;

    private double docCount = 0d;
    private double mb = 0d;
    private double time = System.currentTimeMillis();

    /**
     * initial values for time series
     */
    private double avgCommitRateDocs = 0d;
    private double avgCommitRateMB = 0d;
    private double avgSegs = 0;

    /**
     * length of time series analysis for commit rate and no. of segments
     */
    private double timeSeriesLength = 50d;

    /**
     * current step in current time series batch
     */
    private double timeSeriesCount = 0d;

    /**
     * single exponential smoothing ratio (0 < alpha < 1)
     *
     * values towards 0 tend to give more weight to past inputs
     * values close to 1 weigh recent values more
     */
    private double alpha = 0.7;

    /** Sole constructor, setting all settings to their
     *  defaults. */
    public CommitMitigatingTieredMergePolicy() {
        super(DEFAULT_NO_CFS_RATIO, MergePolicy.DEFAULT_MAX_CFS_SEGMENT_SIZE);
    }

    /** Maximum number of segments to be merged at a time
     *  during "normal" merging.  For explicit merging (eg,
     *  forceMerge or forceMergeDeletes was called), see {@link
     *  #setMaxMergeAtOnceExplicit}.  Default is 10.
     *
     * @param v the max no. of merges
     * @return this merge policy instance
     *
     **/
    public CommitMitigatingTieredMergePolicy setMaxMergeAtOnce(int v) {
        if (v < 2) {
            throw new IllegalArgumentException("maxMergeAtOnce must be > 1 (got " + v + ")");
        }
        maxMergeAtOnce = v;
        return this;
    }

    /**
     * Maximum number of segments allowed for mitigation to happen.
     * This is supposed to avoid having too few merges due to high commit rates
     * @param maxNoOfSegsForMitigation max no. of segments per mitigation
     * @return this merge policy instance
     */
    public CommitMitigatingTieredMergePolicy setMaxNoOfSegsForMitigation(int maxNoOfSegsForMitigation) {
        this.maxNoOfSegsForMitigation = maxNoOfSegsForMitigation;
        return this;
    }

    /**
     * set the maximum no. of docs per sec accepted for a merge to happen
     *
     * @param maxCommitRate maxCommitRate maximum commit rate (docs/sec)
     * @return this merge policy instance
     */
    public CommitMitigatingTieredMergePolicy setMaxCommitRateDocs(double maxCommitRate) {
        this.maxCommitRateDocs = maxCommitRate;
        return this;
    }

    /** Returns the current maxMergeAtOnce setting.
     *
     * @return the max merge at once that can be performed
     * @see #setMaxMergeAtOnce */
    public int getMaxMergeAtOnce() {
        return maxMergeAtOnce;
    }

    // TODO: should addIndexes do explicit merging, too?  And,
    // if user calls IW.maybeMerge "explicitly"

    /** Maximum number of segments to be merged at a time,
     *  during forceMerge or forceMergeDeletes. Default is 30.
     *
     * @param v the no. of max merges
     * @return this merge policy instance
     **/
    public CommitMitigatingTieredMergePolicy setMaxMergeAtOnceExplicit(int v) {
        if (v < 2) {
            throw new IllegalArgumentException("maxMergeAtOnceExplicit must be > 1 (got " + v + ")");
        }
        maxMergeAtOnceExplicit = v;
        return this;
    }

    /** Returns the current maxMergeAtOnceExplicit setting.
     * @return the maxMergeAtOnceExplicit value
     *
     * @see #setMaxMergeAtOnceExplicit */
    public int getMaxMergeAtOnceExplicit() {
        return maxMergeAtOnceExplicit;
    }

    /** Maximum sized segment to produce during
     *  normal merging.  This setting is approximate: the
     *  estimate of the merged segment size is made by summing
     *  sizes of to-be-merged segments (compensating for
     *  percent deleted docs).  Default is 5 GB.
     *
     * @param v the maximum segment size in MB
     * @return this merge policy instance
     **/
    public CommitMitigatingTieredMergePolicy setMaxMergedSegmentMB(double v) {
        if (v < 0.0) {
            throw new IllegalArgumentException("maxMergedSegmentMB must be >=0 (got " + v + ")");
        }
        v *= 1024 * 1024;
        maxMergedSegmentBytes = (v > Long.MAX_VALUE) ? Long.MAX_VALUE : (long) v;
        return this;
    }

    /** Returns the current maxMergedSegmentMB setting.
     *
     * @return the max merged segment size in MB
     *
     * @see #getMaxMergedSegmentMB */
    public double getMaxMergedSegmentMB() {
        return maxMergedSegmentBytes / 1024 / 1024.;
    }

    /** Controls how aggressively merges that reclaim more
     *  deletions are favored.  Higher values will more
     *  aggressively target merges that reclaim deletions, but
     *  be careful not to go so high that way too much merging
     *  takes place; a value of 3.0 is probably nearly too
     *  high.  A value of 0.0 means deletions don't impact
     *  merge selection.
     *
     *  @param v the reclaim deletes weight
     *  @return this merge policy instance
     **/
    public CommitMitigatingTieredMergePolicy setReclaimDeletesWeight(double v) {
        if (v < 0.0) {
            throw new IllegalArgumentException("reclaimDeletesWeight must be >= 0.0 (got " + v + ")");
        }
        reclaimDeletesWeight = v;
        return this;
    }

    /** See {@link #setReclaimDeletesWeight}.
     * @return the reclaim deletes weight
     *
     **/
    public double getReclaimDeletesWeight() {
        return reclaimDeletesWeight;
    }

    /** Segments smaller than this are "rounded up" to this
     *  size, ie treated as equal (floor) size for merge
     *  selection.  This is to prevent frequent flushing of
     *  tiny segments from allowing a long tail in the index.
     *  Default is 2 MB.
     *
     * @param v floor segment size in MB
     * @return this merge policy instance
     *
     **/
    public CommitMitigatingTieredMergePolicy setFloorSegmentMB(double v) {
        if (v <= 0.0) {
            throw new IllegalArgumentException("floorSegmentMB must be >= 0.0 (got " + v + ")");
        }
        v *= 1024 * 1024;
        floorSegmentBytes = (v > Long.MAX_VALUE) ? Long.MAX_VALUE : (long) v;
        return this;
    }

    /** Returns the current floorSegmentMB.
     * @return the floor segment size in MB
     *
     *  @see #setFloorSegmentMB */
    public double getFloorSegmentMB() {
        return floorSegmentBytes / (1024 * 1024.);
    }

    /** When forceMergeDeletes is called, we only merge away a
     *  segment if its delete percentage is over this
     *  threshold.  Default is 10%.
     *
     * @param v the force merge deletes allowed percentage
     * @return this merge policy instance
     *
     **/
    public CommitMitigatingTieredMergePolicy setForceMergeDeletesPctAllowed(double v) {
        if (v < 0.0 || v > 100.0) {
            throw new IllegalArgumentException("forceMergeDeletesPctAllowed must be between 0.0 and 100.0 inclusive (got " + v + ")");
        }
        forceMergeDeletesPctAllowed = v;
        return this;
    }

    /** Returns the current forceMergeDeletesPctAllowed setting.
     * @return the forceMergeDeletesPctAllowed
     *
     * @see #setForceMergeDeletesPctAllowed */
    public double getForceMergeDeletesPctAllowed() {
        return forceMergeDeletesPctAllowed;
    }

    /** Sets the allowed number of segments per tier.  Smaller
     *  values mean more merging but fewer segments.
     *
     *  <p><b>NOTE</b>: this value should be &gt;= the {@link
     *  #setMaxMergeAtOnce} otherwise you'll force too much
     *  merging to occur.</p>
     *
     *  <p>Default is 10.0.</p>
     *
     * @param v segments per tier
     * @return this merge policy instance
     *
     **/
    public CommitMitigatingTieredMergePolicy setSegmentsPerTier(double v) {
        if (v < 2.0) {
            throw new IllegalArgumentException("segmentsPerTier must be >= 2.0 (got " + v + ")");
        }
        segsPerTier = v;
        return this;
    }

    /** Returns the current segmentsPerTier setting.
     * @return segments per tier
     *
     * @see #setSegmentsPerTier */
    public double getSegmentsPerTier() {
        return segsPerTier;
    }

    /**
     * set the max no. of committed MBs for a merge to happen
     * @param maxCommitRateMB max commit rate in MB (mb/sec)
     * @return this merge policy instance
     */
    public CommitMitigatingTieredMergePolicy setMaxCommitRateMB(int maxCommitRateMB) {
        this.maxCommitRateMB = maxCommitRateMB;
        return this;
    }

    private class SegmentByteSizeDescending implements Comparator<SegmentCommitInfo> {
        @Override
        public int compare(SegmentCommitInfo o1, SegmentCommitInfo o2) {
            try {
                final long sz1 = size(o1);
                final long sz2 = size(o2);
                if (sz1 > sz2) {
                    return -1;
                } else if (sz2 > sz1) {
                    return 1;
                } else {
                    return o1.info.name.compareTo(o2.info.name);
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

    /** Holds score and explanation for a single candidate
     *  merge. */
    protected static abstract class MergeScore {
        /** Sole constructor. (For invocation by subclass
         *  constructors, typically implicit.) */
        protected MergeScore() {
        }

        /** Returns the score for this merge candidate; lower
         *  scores are better. */
        abstract double getScore();

        /** Human readable explanation of how the merge got this
         *  score. */
        abstract String getExplanation();
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos) throws IOException {
        int segmentSize = infos.size();
        timeSeriesCount++;

        if (timeSeriesCount % timeSeriesLength == 0) {
            // reset averages
            avgCommitRateDocs = 0d;
            avgCommitRateMB = 0d;
            avgSegs = 0d;
        }
        avgSegs = singleExpSmoothing(segmentSize, avgSegs);

        log.debug("segments: current {}, average {}", segmentSize, avgSegs);

        if (verbose()) {
            message("findMerges: " + segmentSize + " segments, " + avgSegs + " average");
        }
        if (segmentSize == 0) {
            return null;
        }

        // if no. of segments exceeds the maximum, adjust the maximum rates to allow more merges (less commit/rate mitigation)
        if (segmentSize > maxNoOfSegsForMitigation) {
            if (avgCommitRateDocs > maxCommitRateDocs) {
                double v = singleExpSmoothing(avgCommitRateDocs, maxCommitRateDocs);
                log.debug("adjusting maxCommitRateDocs from {} to {}", maxCommitRateDocs, v);
                maxCommitRateDocs = v;
            }
            if (avgCommitRateMB > maxCommitRateMB) {
                double v = singleExpSmoothing(avgCommitRateMB, maxCommitRateMB);
                log.debug("adjusting maxCommitRateMB from {} to {}", maxCommitRateMB, v);
                maxCommitRateMB = v;
            }
        }

        long now = System.currentTimeMillis();
        double timeDelta = (now / 1000d) - (time / 1000d);
        double commitRate = Math.abs(docCount - infos.totalDocCount()) / timeDelta;
        time = now;

        avgCommitRateDocs = singleExpSmoothing(commitRate, avgCommitRateDocs);

        log.debug("commit rate: current {}, average {}, max {} docs/sec", commitRate, avgCommitRateDocs, maxCommitRateDocs);

        docCount = infos.totalDocCount();

        if (verbose()) {
            message(commitRate + "doc/s (max: " + maxCommitRateDocs + ", avg: " + avgCommitRateDocs + " doc/s)");
        }

        // do not mitigate if there're too many segments to avoid affecting performance
        if (commitRate > maxCommitRateDocs && segmentSize < maxNoOfSegsForMitigation) {
            log.debug("mitigation due to {} > {} docs/sec and segments {} < {})", commitRate, maxCommitRateDocs,
                    segmentSize, maxNoOfSegsForMitigation);
            return null;
        }

        final Collection<SegmentCommitInfo> merging = writer.get().getMergingSegments();
        final Collection<SegmentCommitInfo> toBeMerged = new HashSet<SegmentCommitInfo>();

        final List<SegmentCommitInfo> infosSorted = new ArrayList<SegmentCommitInfo>(infos.asList());
        Collections.sort(infosSorted, new SegmentByteSizeDescending());

        // Compute total index bytes & print details about the index
        long totIndexBytes = 0;
        long minSegmentBytes = Long.MAX_VALUE;
        for (SegmentCommitInfo info : infosSorted) {
            final long segBytes = size(info);
            if (verbose()) {
                String extra = merging.contains(info) ? " [merging]" : "";
                if (segBytes >= maxMergedSegmentBytes / 2.0) {
                    extra += " [skip: too large]";
                } else if (segBytes < floorSegmentBytes) {
                    extra += " [floored]";
                }
                message("  seg=" + writer.get().segString(info) + " size=" + String.format(Locale.ROOT, "%.3f", segBytes / 1024 / 1024.) + " MB" + extra);
            }

            minSegmentBytes = Math.min(segBytes, minSegmentBytes);
            // Accum total byte size
            totIndexBytes += segBytes;
        }

        // If we have too-large segments, grace them out
        // of the maxSegmentCount:
        int tooBigCount = 0;
        while (tooBigCount < infosSorted.size() && size(infosSorted.get(tooBigCount)) >= maxMergedSegmentBytes / 2.0) {
            totIndexBytes -= size(infosSorted.get(tooBigCount));
            tooBigCount++;
        }

        minSegmentBytes = floorSize(minSegmentBytes);

        // Compute max allowed segs in the index
        long levelSize = minSegmentBytes;
        long bytesLeft = totIndexBytes;
        double allowedSegCount = 0;
        while (true) {
            final double segCountLevel = bytesLeft / (double) levelSize;
            if (segCountLevel < segsPerTier) {
                allowedSegCount += Math.ceil(segCountLevel);
                break;
            }
            allowedSegCount += segsPerTier;
            bytesLeft -= segsPerTier * levelSize;
            levelSize *= maxMergeAtOnce;
        }
        int allowedSegCountInt = (int) allowedSegCount;

        MergeSpecification spec = null;

        // Cycle to possibly select more than one merge:
        while (true) {

            long mergingBytes = 0;
            double idxBytes = 0;

            // Gather eligible segments for merging, ie segments
            // not already being merged and not already picked (by
            // prior iteration of this loop) for merging:
            final List<SegmentCommitInfo> eligible = new ArrayList<SegmentCommitInfo>();
            for (int idx = tooBigCount; idx < infosSorted.size(); idx++) {
                final SegmentCommitInfo info = infosSorted.get(idx);
                if (merging.contains(info)) {
                    mergingBytes += info.sizeInBytes();
                } else if (!toBeMerged.contains(info)) {
                    eligible.add(info);
                }
                idxBytes += info.sizeInBytes();
            }
            idxBytes /= 1024d * 1024d;

            final boolean maxMergeIsRunning = mergingBytes >= maxMergedSegmentBytes;

            if (verbose()) {
                message("  allowedSegmentCount=" + allowedSegCountInt + " vs count=" + infosSorted.size() + " (eligible count=" + eligible.size() + ") tooBigCount=" + tooBigCount);
            }

            if (eligible.size() == 0) {
                return spec;
            }

            double bytes = idxBytes - this.mb;
            double mbRate = bytes / timeDelta;

            avgCommitRateMB = singleExpSmoothing(mbRate, avgCommitRateMB);

            log.debug("commit rate: current {}, average {}, max {} MB/sec", mbRate, avgCommitRateMB, maxCommitRateMB);

            if (verbose()) {
                message(mbRate + "mb/s (max: " + maxCommitRateMB + ", avg: " + avgCommitRateMB + " MB/s)");
            }

            this.mb = idxBytes;

            // do not mitigate if there're too many segments to avoid affecting performance
            if (mbRate > maxCommitRateMB && segmentSize < maxNoOfSegsForMitigation) {
                log.debug("mitigation due to {} > {} MB/sec and segments {} < {})", mbRate, maxCommitRateMB,
                        segmentSize, maxNoOfSegsForMitigation);
                return null;
            }

            if (eligible.size() >= allowedSegCountInt) {

                // OK we are over budget -- find best merge!
                MergeScore bestScore = null;
                List<SegmentCommitInfo> best = null;
                boolean bestTooLarge = false;
                long bestMergeBytes = 0;

                // Consider all merge starts:
                for (int startIdx = 0; startIdx <= eligible.size() - maxMergeAtOnce; startIdx++) {

                    long totAfterMergeBytes = 0;

                    final List<SegmentCommitInfo> candidate = new ArrayList<SegmentCommitInfo>();
                    boolean hitTooLarge = false;
                    for (int idx = startIdx; idx < eligible.size() && candidate.size() < maxMergeAtOnce; idx++) {
                        final SegmentCommitInfo info = eligible.get(idx);
                        final long segBytes = size(info);

                        if (totAfterMergeBytes + segBytes > maxMergedSegmentBytes) {
                            hitTooLarge = true;
                            // NOTE: we continue, so that we can try
                            // "packing" smaller segments into this merge
                            // to see if we can get closer to the max
                            // size; this in general is not perfect since
                            // this is really "bin packing" and we'd have
                            // to try different permutations.
                            continue;
                        }
                        candidate.add(info);
                        totAfterMergeBytes += segBytes;
                    }

                    final MergeScore score = score(candidate, hitTooLarge, mergingBytes);
                    if (verbose()) {
                        message("  maybe=" + writer.get().segString(candidate) + " score=" + score.getScore() + " " + score.getExplanation() + " tooLarge=" + hitTooLarge + " size=" + String.format(Locale.ROOT, "%.3f MB", totAfterMergeBytes / 1024. / 1024.));
                    }

                    // If we are already running a max sized merge
                    // (maxMergeIsRunning), don't allow another max
                    // sized merge to kick off:
                    if ((bestScore == null || score.getScore() < bestScore.getScore()) && (!hitTooLarge || !maxMergeIsRunning)) {
                        best = candidate;
                        bestScore = score;
                        bestTooLarge = hitTooLarge;
                        bestMergeBytes = totAfterMergeBytes;
                    }
                }

                if (best != null) {
                    if (spec == null) {
                        spec = new MergeSpecification();
                    }
                    final OneMerge merge = new OneMerge(best);
                    spec.add(merge);
                    for (SegmentCommitInfo info : merge.segments) {
                        toBeMerged.add(info);
                    }
                    if (verbose()) {
                        message("  add merge=" + writer.get().segString(merge.segments) + " size=" + String.format(Locale.ROOT, "%.3f MB", bestMergeBytes / 1024. / 1024.) + " score=" + String.format(Locale.ROOT, "%.3f", bestScore.getScore()) + " " + bestScore.getExplanation() + (bestTooLarge ? " [max merge]" : ""));
                    }
                } else {
                    return spec;
                }
            } else {
                return spec;
            }
        }
    }

    /**
     * single exponential smoothing
     * @param input current time series value
     * @param smoothedValue previous smoothed value
     * @return the new smoothed value
     */
    private double singleExpSmoothing(double input, double smoothedValue) {
        return alpha * input + (1 - alpha) * smoothedValue;
    }

    /** Expert: scores one merge; subclasses can override.
     * @param candidate a list of candidate segments
     * @param hitTooLarge hit too large setting
     * @param mergingBytes the bytes to merge
     * @return a merge score
     **/
    protected MergeScore score(List<SegmentCommitInfo> candidate, boolean hitTooLarge, long mergingBytes) throws IOException {
        long totBeforeMergeBytes = 0;
        long totAfterMergeBytes = 0;
        long totAfterMergeBytesFloored = 0;
        for (SegmentCommitInfo info : candidate) {
            final long segBytes = size(info);
            totAfterMergeBytes += segBytes;
            totAfterMergeBytesFloored += floorSize(segBytes);
            totBeforeMergeBytes += info.sizeInBytes();
        }

        // Roughly measure "skew" of the merge, i.e. how
        // "balanced" the merge is (whether the segments are
        // about the same size), which can range from
        // 1.0/numSegsBeingMerged (good) to 1.0 (poor). Heavily
        // lopsided merges (skew near 1.0) is no good; it means
        // O(N^2) merge cost over time:
        final double skew;
        if (hitTooLarge) {
            // Pretend the merge has perfect skew; skew doesn't
            // matter in this case because this merge will not
            // "cascade" and so it cannot lead to N^2 merge cost
            // over time:
            skew = 1.0 / maxMergeAtOnce;
        } else {
            skew = ((double) floorSize(size(candidate.get(0)))) / totAfterMergeBytesFloored;
        }

        // Strongly favor merges with less skew (smaller
        // mergeScore is better):
        double mergeScore = skew;

        // Gently favor smaller merges over bigger ones.  We
        // don't want to make this exponent too large else we
        // can end up doing poor merges of small segments in
        // order to avoid the large merges:
        mergeScore *= Math.pow(totAfterMergeBytes, 0.05);

        // Strongly favor merges that reclaim deletes:
        final double nonDelRatio = ((double) totAfterMergeBytes) / totBeforeMergeBytes;
        mergeScore *= Math.pow(nonDelRatio, reclaimDeletesWeight);

        final double finalMergeScore = mergeScore;

        return new MergeScore() {

            @Override
            public double getScore() {
                return finalMergeScore;
            }

            @Override
            public String getExplanation() {
                return "skew=" + String.format(Locale.ROOT, "%.3f", skew) + " nonDelRatio=" + String.format(Locale.ROOT, "%.3f", nonDelRatio);
            }
        };
    }

    @Override
    public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge) throws IOException {
        if (verbose()) {
            message("findForcedMerges maxSegmentCount=" + maxSegmentCount + " infos=" + writer.get().segString(infos) + " segmentsToMerge=" + segmentsToMerge);
        }

        List<SegmentCommitInfo> eligible = new ArrayList<SegmentCommitInfo>();
        boolean forceMergeRunning = false;
        final Collection<SegmentCommitInfo> merging = writer.get().getMergingSegments();
        boolean segmentIsOriginal = false;
        for (SegmentCommitInfo info : infos) {
            final Boolean isOriginal = segmentsToMerge.get(info);
            if (isOriginal != null) {
                segmentIsOriginal = isOriginal;
                if (!merging.contains(info)) {
                    eligible.add(info);
                } else {
                    forceMergeRunning = true;
                }
            }
        }

        if (eligible.size() == 0) {
            return null;
        }

        if ((maxSegmentCount > 1 && eligible.size() <= maxSegmentCount) ||
                (maxSegmentCount == 1 && eligible.size() == 1 && (!segmentIsOriginal || isMerged(infos, eligible.get(0))))) {
            if (verbose()) {
                message("already merged");
            }
            return null;
        }

        Collections.sort(eligible, new SegmentByteSizeDescending());

        if (verbose()) {
            message("eligible=" + eligible);
            message("forceMergeRunning=" + forceMergeRunning);
        }

        int end = eligible.size();

        MergeSpecification spec = null;

        // Do full merges, first, backwards:
        while (end >= maxMergeAtOnceExplicit + maxSegmentCount - 1) {
            if (spec == null) {
                spec = new MergeSpecification();
            }
            final OneMerge merge = new OneMerge(eligible.subList(end - maxMergeAtOnceExplicit, end));
            if (verbose()) {
                message("add merge=" + writer.get().segString(merge.segments));
            }
            spec.add(merge);
            end -= maxMergeAtOnceExplicit;
        }

        if (spec == null && !forceMergeRunning) {
            // Do final merge
            final int numToMerge = end - maxSegmentCount + 1;
            final OneMerge merge = new OneMerge(eligible.subList(end - numToMerge, end));
            if (verbose()) {
                message("add final merge=" + merge.segString(writer.get().getDirectory()));
            }
            spec = new MergeSpecification();
            spec.add(merge);
        }

        return spec;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos infos) throws IOException {
        if (verbose()) {
            message("findForcedDeletesMerges infos=" + writer.get().segString(infos) + " forceMergeDeletesPctAllowed=" + forceMergeDeletesPctAllowed);
        }
        final List<SegmentCommitInfo> eligible = new ArrayList<SegmentCommitInfo>();
        final Collection<SegmentCommitInfo> merging = writer.get().getMergingSegments();
        for (SegmentCommitInfo info : infos) {
            double pctDeletes = 100. * ((double) writer.get().numDeletedDocs(info)) / info.info.getDocCount();
            if (pctDeletes > forceMergeDeletesPctAllowed && !merging.contains(info)) {
                eligible.add(info);
            }
        }

        if (eligible.size() == 0) {
            return null;
        }

        Collections.sort(eligible, new SegmentByteSizeDescending());

        if (verbose()) {
            message("eligible=" + eligible);
        }

        int start = 0;
        MergeSpecification spec = null;

        while (start < eligible.size()) {
            // Don't enforce max merged size here: app is explicitly
            // calling forceMergeDeletes, and knows this may take a
            // long time / produce big segments (like forceMerge):
            final int end = Math.min(start + maxMergeAtOnceExplicit, eligible.size());
            if (spec == null) {
                spec = new MergeSpecification();
            }

            final OneMerge merge = new OneMerge(eligible.subList(start, end));
            if (verbose()) {
                message("add merge=" + writer.get().segString(merge.segments));
            }
            spec.add(merge);
            start = end;
        }

        return spec;
    }

    @Override
    public void close() {
    }

    private long floorSize(long bytes) {
        return Math.max(floorSegmentBytes, bytes);
    }

    private boolean verbose() {
        final IndexWriter w = writer.get();
        return w != null && w.getConfig().getInfoStream().isEnabled("TMP");
    }

    private void message(String message) {
        writer.get().getConfig().getInfoStream().message("TMP", message);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[" + getClass().getSimpleName() + ": ");
        sb.append("maxMergeAtOnce=").append(maxMergeAtOnce).append(", ");
        sb.append("maxMergeAtOnceExplicit=").append(maxMergeAtOnceExplicit).append(", ");
        sb.append("maxMergedSegmentMB=").append(maxMergedSegmentBytes / 1024 / 1024.).append(", ");
        sb.append("floorSegmentMB=").append(floorSegmentBytes / 1024 / 1024.).append(", ");
        sb.append("forceMergeDeletesPctAllowed=").append(forceMergeDeletesPctAllowed).append(", ");
        sb.append("segmentsPerTier=").append(segsPerTier).append(", ");
        sb.append("maxCFSSegmentSizeMB=").append(getMaxCFSSegmentSizeMB()).append(", ");
        sb.append("noCFSRatio=").append(noCFSRatio);
        return sb.toString();
    }
}
