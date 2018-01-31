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
package org.apache.jackrabbit.oak.security.internal;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

import static com.google.common.base.Preconditions.checkNotNull;

public class SecurityProviderBuilder {

    private ConfigurationParameters configuration = null;

    private SecurityConfiguration sc;
    private Class<? extends SecurityConfiguration> cls;

    public SecurityProviderBuilder with(@Nonnull ConfigurationParameters configuration) {
        this.configuration = checkNotNull(configuration);
        return this;
    }

    public SecurityProviderBuilder with(@Nonnull SecurityConfiguration sc, @Nonnull Class<? extends SecurityConfiguration> cls) {
        this.sc = sc;
        this.cls = cls;
        return this;
    }

    public SecurityProvider build() {
        SecurityProvider sp;
        if (configuration != null) {
            sp = new SecurityProviderImpl(configuration);
        } else {
            sp = new SecurityProviderImpl();
        }

        if (sc != null && cls != null) {
            Object cc = sp.getConfiguration(cls);
            if (!(cc instanceof CompositeConfiguration)) {
                throw new IllegalStateException();
            } else {
                CompositeConfiguration composite = (CompositeConfiguration) cc;
                SecurityConfiguration defConfig = composite.getDefaultConfig();

                if (sc instanceof ConfigurationBase) {
                    ConfigurationBase cb = (ConfigurationBase) sc;
                    cb.setSecurityProvider(sp);
                    cb.setRootProvider(((ConfigurationBase) defConfig).getRootProvider());
                    cb.setTreeProvider(((ConfigurationBase) defConfig).getTreeProvider());
                }

                composite.addConfiguration(sc);
                composite.addConfiguration(defConfig);
            }
        }

        return sp;
    }
}
