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

import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ComponentPropertyType;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@link Validator} which detects references crossing the mount boundaries
 */
@Component(configurationPolicy = ConfigurationPolicy.REQUIRE, service = {ValidatorProvider.class, EditorProvider.class})
public class CrossMountReferenceValidatorProvider extends ValidatorProvider {

    @ComponentPropertyType
    @interface Config {
        @AttributeDefinition
        String type() default "crossMountRefValidator";

        @AttributeDefinition(
                name = "Fail when detecting commits cross-mount references",
                description = "Commits will fail if set to true when detecting cross-mount references. If set to false the commit information is only logged."
        )
        boolean failOnDetection() default true;
    }

    private boolean failOnDetection;

    private MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();

    public CrossMountReferenceValidatorProvider() {
    }

    public CrossMountReferenceValidatorProvider(MountInfoProvider mountInfoProvider, boolean failOnDetection) {
        this.failOnDetection = failOnDetection;
        this.mountInfoProvider = mountInfoProvider;
    }

    @Activate
    private void activate(Config config) {
        failOnDetection = config.failOnDetection();
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

    @SuppressWarnings("unused")
    @Reference(name = "mountInfoProvider")
    protected void bindMountInfoProvider(MountInfoProvider mip) {
        this.mountInfoProvider = mip;
    }

    @SuppressWarnings("unused")
    protected void unbindMountInfoProvider(MountInfoProvider mip) {
        if (this.mountInfoProvider == mip) {
            this.mountInfoProvider = null;
        }

    }
}
