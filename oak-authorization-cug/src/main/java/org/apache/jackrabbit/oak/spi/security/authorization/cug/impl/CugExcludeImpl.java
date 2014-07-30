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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;

/**
 * CugExcludeImpl... TODO
 */
@Component()
@Service({CugExclude.class})
@Properties({
        @Property(name = "principalNames",
                label = "Principal Names",
                description = "Name of principals that are always excluded from CUG evaluation.",
                cardinality = Integer.MAX_VALUE),
        @Property(name = "principalPaths",
                label = "Principal Paths",
                description = "Path pattern for principals that are always excluded from CUG evaluation",
                cardinality = Integer.MAX_VALUE)
})
public class CugExcludeImpl extends CugExclude.Default {

    private String[] principalNames = new String[0];
    private String[] principalPaths = new String[0];

    @Override
    public boolean isExcluded(@Nonnull Set<Principal> principals) {
        if (super.isExcluded(principals)) {
            return true;
        }
        for (String principalName : principalNames) {
            if (principals.contains(new PrincipalImpl(principalName))) {
                return true;
            }
        }
        if (principalPaths.length > 0) {
            for (String principalPath : getPrincipalPaths(principals)) {
                for (String path : principalPaths) {
                    if (principalPath.startsWith(path)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Activate
    protected void activate(Map<String, Object> properties) {
        principalNames = PropertiesUtil.toStringArray(properties.get("principalNames"), new String[0]);
        principalPaths = PropertiesUtil.toStringArray(properties.get("principalPaths"), new String[0]);
    }

    private static Iterable<String> getPrincipalPaths(@Nonnull final Iterable<Principal> principals) {
        return Iterables.filter(Iterables.transform(principals, new Function<Principal, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Principal input) {
                if (input instanceof ItemBasedPrincipal) {
                    try {
                        return ((ItemBasedPrincipal) input).getPath();
                    } catch (RepositoryException e) {
                        return null;
                    }
                } else {
                    return null;
                }
            }
        }), Predicates.notNull());
    }
}