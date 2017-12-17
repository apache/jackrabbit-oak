/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.api.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface IndexStatsMBean {

    String TYPE = "IndexStats";

    String STATUS_INIT = "init";

    String STATUS_RUNNING = "running";

    String STATUS_DONE = "done";

    String STATUS_FAILING = "failing";

    String STATUS_INTERRUPTED = "interrupted";

    /**
     * Name of the async indexing lane
     */
    String getName();

    /**
     * @return The time the indexing job stared at, or {@code ""} if it is
     *         not currently running.
     */
    String getStart();

    /**
     * @return The time the indexing job finished at, or {@code ""} if it
     *         is still running.
     */
    String getDone();

    /**
     * Returns the current status of the indexing job
     * 
     * @return the current status of the indexing job: {@value #STATUS_INIT},
     *         {@value #STATUS_RUNNING} or {@value #STATUS_DONE}
     */
    String getStatus();

    /**
     * @return the last upto which the repository was indexed
     */
    String getLastIndexedTime();

    /**
     * Pauses the background indexing process. Future changes are not indexed
     * until the {@link #resume()} method is called.
     * 
     * The pause call will take effect on the next run cycle and will affect all
     * indexes marked as 'async'.
     * 
     * Note: this is experimental and should only be used for
     * debugging/diagnosis purposes!
     * 
     */
    void pause();

    @Description("Aborts any running indexing cycle and pauses the indexer. Invoke 'resume' once you are read to " +
            "resume indexing again")
    String abortAndPause();

    /**
     * Resumes the indexing process. All changes from the previous indexed state
     * will be indexed.
     * 
     * @see #pause()
     */
    void resume();

    /**
     * Returns the value of the 'paused' flag
     * 
     * @return true if the indexing job is paused
     */
    boolean isPaused();

    /**
     * Returns the number of updates from the current run cycle. This value is
     * kept until the next cycle begins.
     * 
     * @return the number of updates from the current run cycle. This value is
     *         kept until the next cycle begins.
     */
    long getUpdates();

    /**
     * Returns the number of which have been read so far. This value is
     * kept until the next cycle begins.
     *
     * @return the number of node read from the current run cycle. This value is
     *         kept until the next cycle begins.
     */
    long getNodesReadCount();

    /**
     * Returns the current reference checkpoint used by the async indexer
     * 
     * @return the reference checkpoint
     */
    String getReferenceCheckpoint();

    /**
     * Returns the processed checkpoint used by the async indexer. If this index
     * round finishes successfully, the processed checkpoint will become the
     * reference checkpoint, and the old reference checkpoint wil be released.
     * 
     * @return the processed checkpoint
     */
    String getProcessedCheckpoint();

    /**
     * Temporary checkpoints represent old checkpoints that have been processed
     * but the cleanup was not successful of did not happen at all (like in the
     * event the system was forcibly stopped).
     * 
     * @return the already processed checkpoints
     */
    String getTemporaryCheckpoints();

    /**
     * Returns the number of executions.
     *
     * @return the execution count
     */
    long getTotalExecutionCount();

    /**
     * Returns the number of executions as a {@code org.apache.jackrabbit.api.stats.TimeSeries}.
     *
     * @return the execution count time series
     */
    CompositeData getExecutionCount();

    /**
     * Returns the execution time as a {@code org.apache.jackrabbit.api.stats.TimeSeries}.
     *
     * @return the execution times time series
     */
    @Deprecated
    CompositeData getExecutionTime();

    /**
     * Returns the number of indexed nodes as a {@code org.apache.jackrabbit.api.stats.TimeSeries}.
     *
     * @return the indexed nodes time series
     */
    CompositeData getIndexedNodesCount();

    /**
     * Returns the consolidated execution stats since last reset
     * @return consolidated execution stats
     */
    CompositeData getConsolidatedExecutionStats();

    /**
     * Resets the consolidated stats.
     */
    @Deprecated
    void resetConsolidatedExecutionStats();

    /**
     * Splits the current indexing tasks into 2, indexes that are passed in as
     * an input will have their 'async' property updated to
     * {@code newIndexTaskName}.
     * 
     * Note that this call will *not* bootstrap a new indexing task for the
     * given name.
     */
    void splitIndexingTask(
            @Name("paths") @Description("Comma separated list of paths of the index definitions") String paths,
            @Name("newIndexTaskName") @Description("The indexing task name set on the async property") String newIndexTaskName);

    /**
     * Starts a new background indexing task and registers the JMX MBeans for it
     * 
     */
    void registerAsyncIndexer(@Name("name") String name,
            @Name("delayInSeconds") long delayInSeconds);

    /**
     * @return true if the indexing job is failing
     */
    boolean isFailing();

    /**
     * @return The time the indexing job stared failing, or {@code ""} if the
     *         job is not currently failing.
     */
    String getFailingSince();

    /**
     * @return the number of consecutive failed executions or {@code 0} if the
     *         job is not currently failing.
     */
    long getConsecutiveFailedExecutions();

    /**
     * @return the latest indexing error seen, will not be reset once the job
     *         starts working again
     */
    String getLatestError();

    /**
     * @return the time when the latest indexing error has been seen, will not
     *         be reset once the job starts working again
     */
    String getLatestErrorTime();

    TabularData getFailingIndexStats();

}
