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

import org.apache.jackrabbit.oak.segment.osgi.MetatypeInformation.ObjectClassDefinition;
import org.junit.Test;

public class SegmentNodeStoreMonitorServiceTest {

    @Test
    public void testComponentDescriptor() throws Exception {
        ComponentDescriptor cd = ComponentDescriptor.open(getClass().getResourceAsStream("/OSGI-INF/org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService.xml"));
        assertTrue(cd.hasName("org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService"));
        assertTrue(cd.hasRequireConfigurationPolicy());
        assertTrue(cd.hasActivateMethod("activate"));
        assertTrue(cd.hasImplementationClass("org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService"));
        assertTrue(cd.hasReference("snsStatsMBean")
            .withInterface("org.apache.jackrabbit.oak.segment.SegmentNodeStoreStatsMBean")
            .withMandatoryUnaryCardinality()
            .withStaticPolicy()
            .withField("snsStatsMBean")
            .check());
    }

    @Test
    public void testMetatypeInformation() throws Exception {
        MetatypeInformation mi = MetatypeInformation.open(getClass().getResourceAsStream("/OSGI-INF/metatype/org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService$Configuration.xml"));
        assertTrue(mi.hasDesignate()
            .withPid("org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService")
            .withReference("org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService$Configuration")
            .check());

        ObjectClassDefinition ocd = mi.getObjectClassDefinition("org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService$Configuration");
        assertTrue(ocd.hasAttributeDefinition("commitsTrackerWriterGroups")
            .withStringType()
            .withCardinality("2147483647")
            .check());

    }
}
