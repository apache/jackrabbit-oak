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
package org.apache.jackrabbit.oak.jcr.observation;

import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;

import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Temporary class to be removed before 1.6 which allows to turn on/off
 * the prefiltering test mode via an osgi-config.
 * <p>
 * Note: the system property (oak.observation.prefilteringTestMode) 
 * has thus been removed.
 * TODO: remove me before 1.6
 * @deprecated to be removed before 1.6
 */
@Component(
        policy = ConfigurationPolicy.OPTIONAL,
        immediate = true,
        metatype = true,
        label = "Apache Jackrabbit Oak Change Processor Prefilter TestConfig",
        description = "Temporary config used for testing ChangeProcessor prefiltering."
)
public class ChangeProcessorPrefilterTestConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeProcessorPrefilterTestConfig.class);

    private static final boolean DEFAULT_TESTMODE = false;
    @Property(boolValue = DEFAULT_TESTMODE, label = "turns on or off the prefiltering test mode", 
            description = "When set to true it puts prefiltering in a test mode. In the test mode the"
                    + " prefilter is still evaluated but not applied - instead the result (whether or not"
                    + " any events were delivered to the listener) is compared with the outcome of prefiltering."
                    + " If prefiltering and normal filtering mismatch this is reported in as a log.warn."
                    + " When set to false it leaves prefiltering in normal mode. Default is "
            + DEFAULT_TESTMODE)
    private static final String PROP_TESTMODE = "prefiltering.testmode";
    
    private boolean prefilteringTestmode = DEFAULT_TESTMODE;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) {
        reconfig(config);
        LOG.info("activate: prefilteringTestmode=" + prefilteringTestmode);
        setTestMode();
    }

    @Modified
    protected void modified(final Map<String, Object> config) {
        reconfig(config);
        LOG.info("modified: prefilteringTestmode=" + prefilteringTestmode);
        setTestMode();
    }
    
    private void reconfig(Map<String, ?> config) {
        prefilteringTestmode = toBoolean(config.get(PROP_TESTMODE), DEFAULT_TESTMODE);
    }

    @Deactivate
    protected void deactivate() {
        prefilteringTestmode = DEFAULT_TESTMODE;
        LOG.info("deactivate: prefilteringTestmode=" + prefilteringTestmode + " (default)");
        setTestMode();
    }

    private void setTestMode() {
        ChangeProcessor.setPrefilteringTestMode(prefilteringTestmode);
    }

}
