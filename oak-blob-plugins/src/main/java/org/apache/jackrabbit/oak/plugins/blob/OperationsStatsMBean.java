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

package org.apache.jackrabbit.oak.plugins.blob;

/**
 * Interface to give useful statistics for maintenance operations.
 */
public interface OperationsStatsMBean {
    String TYPE = "OperationStats";

    String getName();

    /**
     * Returns the start counts of the operation
     *
     * @return
     */
    long getStartCount();


    /**
     * Returns the finish error count
     *
     * @return
     */
    long getFailureCount();

    /**
     * Returns the duration of the operation
     * @return
     */
    long duration();

    /**
     * Returns the duration of the mark operation
     * @return
     */
    long markDuration();

    /**
     * Returns the number deleted.
     * @return
     */
    long numDeleted();

    /**
     * Returns the size deleted.
     * @return
     */
    long sizeDeleted();
}
