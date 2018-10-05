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

package org.apache.jackrabbit.oak.spi.blob.stats;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import com.google.common.io.CountingInputStream;


public final class StatsCollectingStreams {

    public static InputStream wrap(final BlobStatsCollector collector, final String blobId, InputStream in) {
        final CountingInputStream cin = new CountingInputStream(in);
        return new FilterInputStream(cin) {
            final long startTime = System.nanoTime();

            @Override
            public void close() throws IOException {
                super.close();
                //We rely on close to determine how much was downloaded
                //as once an InputStream is exposed its not possible to
                //determine if the stream is actually used

                //Download time might not be accurate as reading code might
                //be processing also as it moved further in stream. So that
                //overhead would add to the download time

                collector.downloaded(blobId, System.nanoTime() - startTime, TimeUnit.NANOSECONDS, cin.getCount());
                collector.downloadCompleted(blobId);
            }
        };
    }
}
