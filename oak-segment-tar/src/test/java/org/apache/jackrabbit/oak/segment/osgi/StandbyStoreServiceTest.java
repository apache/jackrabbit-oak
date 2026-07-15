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
import org.junit.Ignore;
import org.junit.Test;

public class StandbyStoreServiceTest {

    @Test
    public void testComponentDescriptor() throws Exception {
        ComponentDescriptor cd = ComponentDescriptor.open(getClass().getResourceAsStream("/OSGI-INF/org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService.xml"));
        assertTrue(cd.hasName("org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService"));
        assertTrue(cd.hasRequireConfigurationPolicy());
        assertTrue(cd.hasActivateMethod("activate"));
        assertTrue(cd.hasDeactivateMethod("deactivate"));
        assertTrue(cd.hasImplementationClass("org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService"));
        assertTrue(cd.hasProperty("org.apache.sling.installer.configuration.persist")
            .withBooleanType()
            .withValue("false")
            .check());
        assertTrue(cd.hasProperty("mode")
            .withValue("primary")
            .check());
        assertTrue(cd.hasProperty("port")
            .withIntegerType()
            .withValue("8023")
            .check());
        assertTrue(cd.hasProperty("primary.host")
            .withValue("127.0.0.1")
            .check());
        assertTrue(cd.hasProperty("interval")
            .withLongType()
            .withValue("5")
            .check());
        assertTrue(cd.hasProperty("secure")
            .withBooleanType()
            .withValue("false")
            .check());
        assertTrue(cd.hasProperty("standby.readtimeout")
            .withIntegerType()
            .withValue("60000")
            .check());
        assertTrue(cd.hasProperty("standby.autoclean")
            .withBooleanType()
            .withValue("true")
            .check());
        assertTrue(cd.hasReference("storeProvider")
            .withInterface("org.apache.jackrabbit.oak.segment.SegmentStoreProvider")
            .withMandatoryUnaryCardinality()
            .withStaticPolicy()
            .withGreedyPolicyOption()
            .withField("storeProvider")
            .check());
    }

    @Test
    public void testMetatypeInformation() throws Exception {
        MetatypeInformation mi = MetatypeInformation.open(getClass().getResourceAsStream("/OSGI-INF/metatype/org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService$Configuration.xml"));
        assertTrue(mi.hasDesignate()
            .withPid("org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService")
            .withReference("org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService$Configuration")
            .check());

        ObjectClassDefinition ocd = mi.getObjectClassDefinition("org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService$Configuration");
        assertTrue(ocd.hasAttributeDefinition("org.apache.sling.installer.configuration.persist")
            .withBooleanType()
            .withDefaultValue("false")
            .check());
        assertTrue(ocd.hasAttributeDefinition("mode")
            .withStringType()
            .withDefaultValue("primary")
            .withOptions("primary", "standby")
            .check());
        assertTrue(ocd.hasAttributeDefinition("port")
            .withIntegerType()
            .withDefaultValue("8023")
            .check());
        assertTrue(ocd.hasAttributeDefinition("primary.host")
            .withStringType()
            .withDefaultValue("127.0.0.1")
            .check());
        assertTrue(ocd.hasAttributeDefinition("interval")
            .withLongType()
            .withDefaultValue("5")
            .check());
        assertTrue(ocd.hasAttributeDefinition("secure")
            .withBooleanType()
            .withDefaultValue("false")
            .check());
        assertTrue(ocd.hasAttributeDefinition("standby.readtimeout")
            .withIntegerType()
            .withDefaultValue("60000")
            .check());
        assertTrue(ocd.hasAttributeDefinition("standby.autoclean")
            .withBooleanType()
            .withDefaultValue("true")
            .check());
        assertTrue(ocd.hasAttributeDefinition("primary.allowed-client-ip-ranges")
            .withStringType()
            .withCardinality("2147483647")
            .check());
    }

}
