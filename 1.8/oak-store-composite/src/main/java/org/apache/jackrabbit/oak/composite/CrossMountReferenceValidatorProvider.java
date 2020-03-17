/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.framework.BundleContext;

import java.util.Map;

/**
 * {@link Validator} which detects references crossing the mount boundaries
 */
@Component(label = "Apache Jackrabbit Oak CrossMountReferenceValidatorProvider", policy = ConfigurationPolicy.REQUIRE)
@Property(name = "type", value = "crossMountRefValidator", propertyPrivate = true)
@Service({ValidatorProvider.class, EditorProvider.class})
public class CrossMountReferenceValidatorProvider extends ValidatorProvider {

    @Property(
            boolValue = true,
            label = "Fail when detecting commits cross-mount references",
            description = "Commits will fail if set to true when detecting cross-mount references. If set to false the commit information is only logged."
    )
    private static final String PROP_FAIL_ON_DETECTION = "failOnDetection";
    private boolean failOnDetection;

    @Reference
    private MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config) {
        failOnDetection = PropertiesUtil.toBoolean(config.get(PROP_FAIL_ON_DETECTION), false);
    }

    @Override
    protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        return new CrossMountReferenceValidator(after, mountInfoProvider, failOnDetection);
    }

    CrossMountReferenceValidatorProvider with(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
        return this;
    }

    CrossMountReferenceValidatorProvider withFailOnDetection(boolean failOnDetection) {
        this.failOnDetection = failOnDetection;
        return this;
    }
}
