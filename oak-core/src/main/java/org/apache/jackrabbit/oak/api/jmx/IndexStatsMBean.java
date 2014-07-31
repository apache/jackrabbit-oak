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

public interface IndexStatsMBean {

    String TYPE = "IndexStats";

    String STATUS_INIT = "init";

    String STATUS_RUNNING = "running";

    String STATUS_DONE = "done";

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

}
