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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ProtectionConfig;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Component(service = ProtectionConfig.class, immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = ProtectionConfigImpl.Configuration.class)
public class ProtectionConfigImpl implements ProtectionConfig {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak External Identity Protection Exclusion List",
            description = "Implementation of ProtectionConfig that marks all properties and trees as protected except for those specified matching the names in the 2 allow lists.")
    @interface Configuration {
        @AttributeDefinition(
                name = "Property Allow List",
                description = "Names of properties that are excluded from the protection status.",
                cardinality = Integer.MAX_VALUE)
        String[] propertyNames() default {};

        @AttributeDefinition(
                name = "Node Allow List",
                description = "Names of nodes that are excluded from the protection status. Note that the exception is applied to the whole subtree.",
                cardinality = Integer.MAX_VALUE)
        String[] nodeNames() default {};
    }
    
    private Set<String> propertyNames = Collections.emptySet();
    private Set<String> nodeNames = Collections.emptySet();

    @Activate
    protected void activate(Map<String, Object> properties) {
        propertyNames = CollectionUtils.toSet(PropertiesUtil.toStringArray(properties.get("propertyNames"), new String[0]));
        nodeNames = CollectionUtils.toSet(PropertiesUtil.toStringArray(properties.get("nodeNames"), new String[0]));
    }
    
    @Override
    public boolean isProtectedProperty(@NotNull Tree parent, @NotNull PropertyState property) {
        return !propertyNames.contains(property.getName());
    }

    @Override
    public boolean isProtectedTree(@NotNull Tree tree) {
        return !nodeNames.contains(tree.getName());
    }

}