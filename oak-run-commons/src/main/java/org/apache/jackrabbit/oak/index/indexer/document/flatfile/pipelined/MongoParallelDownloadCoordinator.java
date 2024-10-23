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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

/**
 * Coordinates the two parallel download streams used to download from Mongo when parallelDump is enabled. One stream
 * downloads in ascending order the other in descending order. This class keeps track of the top limit of the ascending
 * stream and of the bottom limit of the descending stream, and determines if the streams have crossed. This indicates
 * that the download completed and the two threads should stop.
 */
class MongoParallelDownloadCoordinator {

    private long lowerRangeTop = 0;
    private long upperRangeBottom = Long.MAX_VALUE;

    public long getUpperRangeBottom() {
        return upperRangeBottom;
    }

    public long getLowerRangeTop() {
        return lowerRangeTop;
    }

    public synchronized boolean increaseLowerRange(long modified) {
        if (modified > lowerRangeTop) {
            lowerRangeTop = modified;
        }
        return downloadsCrossed();
    }

    private boolean downloadsCrossed() {
        return lowerRangeTop > upperRangeBottom;
    }

    public synchronized boolean decreaseUpperRange(long modified) {
        if (modified < upperRangeBottom) {
            upperRangeBottom = modified;
        }
        return downloadsCrossed();
    }

    @Override
    public String toString() {
        return "MongoParallelDownloadCoordinator{" +
                "lowerRangeTop=" + lowerRangeTop +
                ", upperRangeBottom=" + upperRangeBottom +
                '}';
    }
}
