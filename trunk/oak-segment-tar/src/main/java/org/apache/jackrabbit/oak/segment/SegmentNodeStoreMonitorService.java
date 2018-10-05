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

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreMonitorService.Configuration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/**
 * An OSGi wrapper for segment node store monitoring configurations.
 */
@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = Configuration.class)
public class SegmentNodeStoreMonitorService {

    @ObjectClassDefinition(
        name = "Oak Segment Tar Monitoring service",
        description = "This service is responsible for different configurations related to " +
            "Oak Segment Tar read/write monitoring."
    )
    @interface Configuration {

        @AttributeDefinition(
            name = "Writer groups",
            description = "Writer groups for which commits are tracked individually"
        )
        String[] commitsTrackerWriterGroups() default {};

    }

    @Reference
    private SegmentNodeStoreStatsMBean snsStatsMBean;

    @Activate
    public void activate(Configuration config) {
        snsStatsMBean.setWriterGroupsForLastMinuteCounts(PropertiesUtil.toStringArray(config.commitsTrackerWriterGroups(), null));
    }

}
