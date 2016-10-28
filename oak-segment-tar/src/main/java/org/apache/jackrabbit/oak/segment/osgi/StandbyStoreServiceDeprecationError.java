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
package org.apache.jackrabbit.oak.segment.osgi;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This component is activated when a configuration for the deprecated {@code
 * StandbyStoreService} from {@code oak-segment} is detected. When this
 * component is activated, it prints a detailed error message describing the
 * detected problem and hinting at a possible solution.
 */
@Component(
        policy = ConfigurationPolicy.REQUIRE,
        configurationPid = "org.apache.jackrabbit.oak.plugins.segment.standby.store.StandbyStoreService"
)
public class StandbyStoreServiceDeprecationError {

    private static final Logger logger = LoggerFactory.getLogger(StandbyStoreServiceDeprecationError.class);

    private static final String OLD_PID = "org.apache.jackrabbit.oak.plugins.segment.standby.store.StandbyStoreService";

    private static final String NEW_PID = "org.apache.jackrabbit.oak.segment.standby.store.StandbyStoreService";

    @Activate
    public void activate() {
        logger.warn(DeprecationMessage.movedPid(OLD_PID, NEW_PID));
    }

}
