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

package org.apache.jackrabbit.oak.segment.osgi;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SegmentNodeStoreServiceTest {

    @Test
    public void testComponentDescriptor() throws Exception {
        ComponentDescriptor cd = ComponentDescriptor.open(getClass().getResourceAsStream("/OSGI-INF/org.apache.jackrabbit.oak.segment.SegmentNodeStoreService.xml"));
        assertTrue(cd.hasName("org.apache.jackrabbit.oak.segment.SegmentNodeStoreService"));
        assertTrue(cd.hasRequireConfigurationPolicy());
        assertTrue(cd.hasActivateMethod("activate"));
        assertTrue(cd.hasDeactivateMethod("deactivate"));
        assertTrue(cd.hasImplementationClass("org.apache.jackrabbit.oak.segment.SegmentNodeStoreService"));
        assertTrue(cd.hasProperty("repository.home").check());
        assertTrue(cd.hasProperty("tarmk.mode").check());
        assertTrue(cd.hasProperty("repository.backup.dir").check());
        assertTrue(cd.hasProperty("tarmk.size")
            .withIntegerType()
            .withValue("256")
            .check());
        assertTrue(cd.hasProperty("segmentCache.size")
            .withIntegerType()
            .withValue("256")
            .check());
        assertTrue(cd.hasProperty("stringCache.size")
            .withIntegerType()
            .withValue("256")
            .check());
        assertTrue(cd.hasProperty("templateCache.size")
            .withIntegerType()
            .withValue("64")
            .check());
        assertTrue(cd.hasProperty("stringDeduplicationCache.size")
            .withIntegerType()
            .withValue("15000")
            .check());
        assertTrue(cd.hasProperty("templateDeduplicationCache.size")
            .withIntegerType()
            .withValue("3000")
            .check());
        assertTrue(cd.hasProperty("nodeDeduplicationCache.size")
            .withIntegerType()
            .withValue("1048576")
            .check());
        assertTrue(cd.hasProperty("pauseCompaction")
            .withBooleanType()
            .withValue("false")
            .check());
        assertTrue(cd.hasProperty("compaction.retryCount")
            .withIntegerType()
            .withValue("5")
            .check());
        assertTrue(cd.hasProperty("compaction.force.timeout")
            .withIntegerType()
            .withValue("60")
            .check());
        assertTrue(cd.hasProperty("compaction.sizeDeltaEstimation")
            .withLongType()
            .withValue("1073741824")
            .check());
        assertTrue(cd.hasProperty("compaction.disableEstimation")
            .withBooleanType()
            .withValue("false")
            .check());
        assertTrue(cd.hasProperty("compaction.retainedGenerations")
            .withIntegerType()
            .withValue("2")
            .check());
        assertTrue(cd.hasProperty("compaction.memoryThreshold")
            .withIntegerType()
            .withValue("15")
            .check());
        assertTrue(cd.hasProperty("compaction.progressLog")
            .withLongType()
            .withValue("-1")
            .check());
        assertTrue(cd.hasProperty("standby")
            .withBooleanType()
            .withValue("false")
            .check());
        assertTrue(cd.hasProperty("customBlobStore")
            .withBooleanType()
            .withValue("false")
            .check());
        assertTrue(cd.hasProperty("customSegmentStore")
            .withBooleanType()
            .withValue("false")
            .check());
        assertTrue(cd.hasProperty("blobGcMaxAgeInSecs")
            .withLongType()
            .withValue("86400")
            .check());
        assertTrue(cd.hasProperty("blobTrackSnapshotIntervalInSecs")
            .withLongType()
            .withValue("43200")
            .check());
        assertTrue(cd.hasReference("blobStore")
            .withInterface("org.apache.jackrabbit.oak.spi.blob.BlobStore")
            .withOptionalUnaryCardinality()
            .withStaticPolicy()
            .withGreedyPolicyOption()
            .withTarget("(&(!(split.blobstore=old))(!(split.blobstore=new)))")
            .withBind("bindBlobStore")
            .withUnbind("unbindBlobStore")
            .check());
        assertTrue(cd.hasReference("segmentStore")
            .withInterface("org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence")
            .withOptionalUnaryCardinality()
            .withStaticPolicy()
            .withGreedyPolicyOption()
            .withBind("bindSegmentStore")
            .withUnbind("unbindSegmentStore")
            .check());
        assertTrue(cd.hasReference("statisticsProvider")
            .withInterface("org.apache.jackrabbit.oak.stats.StatisticsProvider")
            .withMandatoryUnaryCardinality()
            .withStaticPolicy()
            .withBind("bindStatisticsProvider")
            .withUnbind("unbindStatisticsProvider")
            .check());
    }
}
