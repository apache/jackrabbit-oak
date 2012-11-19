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
package org.apache.jackrabbit.mk.osgi;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.osgi.service.component.ComponentContext;

@Component(policy = ConfigurationPolicy.REQUIRE)
@Service(MicroKernel.class)
public class MicroKernelService extends MicroKernelImpl {

    @Property(description="The unique name of this instance")
    public static final String NAME = "name";

    @Property(description="The home directory (in-memory if not set)")
    public static final String HOME_DIR = "homeDir";

    private String name;

    @Override
    public String toString() {
        return name;
    }

    @Activate
    public void activate(ComponentContext context) {
        Object homeDir = context.getProperties().get(HOME_DIR);
        name = "" + context.getProperties().get(NAME);
        if (homeDir != null) {
            init(homeDir.toString());
        }
    }

    @Deactivate
    public void deactivate() {
        dispose();
    }

}
