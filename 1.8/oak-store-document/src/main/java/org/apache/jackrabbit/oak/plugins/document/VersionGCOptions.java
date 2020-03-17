/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import java.util.concurrent.TimeUnit;

public class VersionGCOptions {

    public final int overflowToDiskThreshold;
    public final long collectLimit;
    public final long precisionMs;
    public final int maxIterations;
    public final long maxDurationMs;
    public final double delayFactor;

    public VersionGCOptions() {
        this(100000, 100000, TimeUnit.MINUTES.toMillis(1),
                0, TimeUnit.HOURS.toMillis(0), 0);
    }

    private VersionGCOptions(int overflow, long collectLimit, long precisionMs,
                             int maxIterations, long maxDurationMs, double delayFactor) {
        this.overflowToDiskThreshold = overflow;
        this.collectLimit = collectLimit;
        this.precisionMs = precisionMs;
        this.maxIterations = maxIterations;
        this.maxDurationMs = maxDurationMs;
        this.delayFactor = delayFactor;
    }

    /**
     * Set the limit of number of resource id+_modified strings (not length) held in memory during
     * a collection run. Any more will be stored and sorted in a temporary file.
     * @param overflowToDiskThreshold limit after which to use file based storage for candidate ids
     */
    public VersionGCOptions withOverflowToDiskThreshold(int overflowToDiskThreshold) {
        return new VersionGCOptions(overflowToDiskThreshold, this.collectLimit,
                this.precisionMs, this.maxIterations, this.maxDurationMs, this.delayFactor);
    }

    /**
     * Sets the absolute limit on number of resource ids collected in one run. This does not count
     * nodes which can be deleted immediately. When this limit is exceeded, the run either fails or
     * is attempted with different parameters, depending on other settings. Note that if the inspected
     * time interval is equal or less than {@link #precisionMs}, the collection limit will be ignored.
     *
     * @param limit the absolute limit of resources collected in one run
     */
    public VersionGCOptions withCollectLimit(long limit) {
        return new VersionGCOptions(this.overflowToDiskThreshold, limit,
                this.precisionMs, this.maxIterations, this.maxDurationMs, this.delayFactor);
    }

    /**
     * Set the minimum duration that is used for time based searches. This should at minimum be the
     * precision available on modification dates of documents, but can be set larger to avoid querying
     * the database too often. Note however that {@link #collectLimit} will not take effect for runs
     * that query equal or shorter than precision duration.
     *
     * @param unit time unit used for duration
     * @param t    the number of units in the duration
     */
    public VersionGCOptions withPrecisionMs(TimeUnit unit, long t) {
        return new VersionGCOptions(this.overflowToDiskThreshold, this.collectLimit,
                unit.toMillis(t), this.maxIterations, this.maxDurationMs, this.delayFactor);
    }

    /**
     * Set the maximum duration in elapsed time that the garbage collection shall take. Setting this
     * to 0 means that there is no limit imposed. A positive duration will impose a soft limit, e.g.
     * the collection might take longer, but no next iteration will be attempted afterwards. See
     * {@link #withMaxIterations(int)} on how to control the behaviour.
     *
     * @param unit time unit used for duration
     * @param t    the number of units in the duration
     */
    public VersionGCOptions withMaxDuration(TimeUnit unit, long t) {
        return new VersionGCOptions(this.overflowToDiskThreshold, this.collectLimit,
                this.precisionMs, this.maxIterations, unit.toMillis(t), this.delayFactor);
    }

    /**
     * Set the maximum number of iterations that shall be attempted in a single run. A value
     * of 0 means that there is no limit. Since the garbage collector uses iterations to find
     * suitable time intervals and set sizes for cleanups, limiting the iterations is only
     * recommended for setups where the collector is called often.
     *
     * @param max the maximum number of iterations allowed
     */
    public VersionGCOptions withMaxIterations(int max) {
        return new VersionGCOptions(this.overflowToDiskThreshold, this.collectLimit,
                this.precisionMs, this.maxIterations, max, this.delayFactor);
    }

    /**
     * Set a delay factor between batched database modifications. This rate limits the writes
     * to the database by a garbage collector. 0, e.g. no delay, is the default. This is recommended
     * when garbage collection is done during a maintenance time when other system load is low.
     * <p>
     * For factory &gt; 0, the actual delay is the duration of the last batch modification times
     * the factor. Example: 0.25 would result in a 25% delay, e.g. a batch modification running
     * 10 seconds would be followed by a sleep of 2.5 seconds.
     *
     * @param f the factor used to calculate batch modification delays
     */
    public VersionGCOptions withDelayFactor(double f) {
        return new VersionGCOptions(this.overflowToDiskThreshold, this.collectLimit,
                this.precisionMs, this.maxIterations, this.maxDurationMs, f);
    }

}

