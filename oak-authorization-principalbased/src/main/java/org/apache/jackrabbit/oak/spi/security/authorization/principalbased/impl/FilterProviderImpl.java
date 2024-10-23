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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of the {@link org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter} interface that
 * consists of the following two filtering conditions:
 *
 * <ol>
 *     <li>All principals in the set must be of type {@link org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal}</li>
 *     <li>All principals in the set must be located in the repository below the configured path.</li>
 * </ol>
 */
@Component(service = {FilterProvider.class}, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = FilterProviderImpl.Configuration.class)
public class FilterProviderImpl implements FilterProvider {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak Filter for Principal Based Authorization")
    @interface Configuration {
        @AttributeDefinition(
                name = "Path",
                description = "Required path underneath which all filtered system-user-principals must be located in the repository.")
        String path();
    }

    private static final Logger log = LoggerFactory.getLogger(FilterProviderImpl.class);

    private String oakPath;

    private final Map<String, String> validatedPrincipalNamesPathMap = new ConcurrentHashMap<>();
    private final Map<String, String> unsupportedPrincipalNames = new ConcurrentHashMap<>();

    /**
     * Constructor to use outside OSGi containers
     * @param oakPath the repository path where the principals are located
     * @since 1.54
     */
    public FilterProviderImpl(@NotNull String oakPath) {
        setPath(oakPath);
    }

    //-----------------------------------------------------< FilterProvider >---

    @Override
    public boolean handlesPath(@NotNull String oakPath) {
        return Text.isDescendantOrEqual(this.oakPath, oakPath);
    }

    @NotNull
    @Override
    public String getFilterRoot() {
        return oakPath;
    }

    @NotNull
    @Override
    public Filter getFilter(@NotNull SecurityProvider securityProvider, @NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        PrincipalProvider principalProvider = securityProvider.getConfiguration(PrincipalConfiguration.class).getPrincipalProvider(root, namePathMapper);
        return new FilterImpl(root, principalProvider, namePathMapper);
    }

    //----------------------------------------------------< SCR Integration >---

    public FilterProviderImpl() {
        // constructor to use from SCR (not yet possible to use constructor injection, see https://issues.apache.org/jira/browse/OAK-9837)
    }

    @Activate
    protected void activate(Configuration configuration, Map<String, Object> properties) {
        setPath(configuration.path());
    }

    @Modified
    protected void modified(Configuration configuration, Map<String, Object> properties) {
        setPath(configuration.path());
    }

    private void setPath(@NotNull String path) {
        Validate.checkState(isValidPath(path), "Configured path must be a valid absolute path.");
        oakPath = path;
    }

    private static boolean isValidPath(@Nullable String path) {
        return !Strings.isNullOrEmpty(path) && PathUtils.isAbsolute(path);
    }
    //-------------------------------------------------------------< Filter >---

    private final class FilterImpl implements Filter {

        private final Root root;
        private final PrincipalProvider principalProvider;
        private final NamePathMapper namePathMapper;

        private FilterImpl(@NotNull Root root, @NotNull PrincipalProvider principalProvider, @NotNull NamePathMapper namePathMapper) {
            this.root = root;
            this.principalProvider = principalProvider;
            this.namePathMapper = namePathMapper;
        }

        @Override
        public boolean canHandle(@NotNull Set<Principal> principals) {
            if (principals.isEmpty()) {
                return false;
            }
            for (Principal p : principals) {
                if (!isValidPrincipal(p)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        @NotNull
        public String getOakPath(@NotNull Principal validPrincipal) {
            String principalPath = validatedPrincipalNamesPathMap.get(validPrincipal.getName());
            if (principalPath == null) {
                throw new IllegalArgumentException("Invalid principal " + validPrincipal.getName());
            }
            return principalPath;
        }

        @Override
        @Nullable
        public Principal getValidPrincipal(@NotNull String oakPath) {
            ItemBasedPrincipal principal = principalProvider.getItemBasedPrincipal(oakPath);
            if (principal != null && isValidPrincipal(principal)) {
                return principal;
            } else {
                return null;
            }
        }

        private boolean isValidPrincipal(@NotNull Principal principal) {
            if (!(principal instanceof SystemUserPrincipal)) {
                return false;
            }

            String principalName = principal.getName();
            String path = validatedPrincipalNamesPathMap.get(principalName);
            if (path != null && isValidMapEntry(principal, path)) {
                return true;
            }
            path = unsupportedPrincipalNames.get(principalName);
            if (path != null && isValidMapEntry(principal, path)) {
                return false;
            }

            String principalPath = getPrincipalPath(principal);
            if (principalPath != null && handlesPath(principalPath)) {
                unsupportedPrincipalNames.remove(principalName);
                validatedPrincipalNamesPathMap.put(principalName, principalPath);
                return true;
            } else {
                validatedPrincipalNamesPathMap.remove(principalName);
                unsupportedPrincipalNames.put(principalName, Objects.toString(principalPath, ""));
                return false;
            }
        }

        /**
         * Besteffort validation if the given entry in 'validatedPrincipalNamesPathMap' is points to the correct path.
         * Note, that this will just be performed for instances of {@code ItemBasedPrincipal}, where obtaining the path
         * doesn't require looking up the principal again.
         *
         * @param principal The target principal to be validated
         * @param oakPath The Oak path stored in 'validatedPrincipalNamesPathMap' for the given principal.
         * @return {@code true}, if the principal is an instance of {@code ItemBasedPrincipal}, whose Oak path is equal
         * to the given {@code oakPath} and {@code false} if the paths are not equal. For any other types of principal
         * this method will return {@code true} in order to avoid excessive principal lookup.
         */
        private boolean isValidMapEntry(@NotNull Principal principal, @NotNull String oakPath) {
            if (principal instanceof ItemBasedPrincipal) {
                return oakPath.equals(getOakPath((ItemBasedPrincipal) principal));
            } else {
                return true;
            }
        }

        @Nullable
        private String getPrincipalPath(@NotNull Principal principal) {
            String prinicpalOakPath = null;
            if (principal instanceof ItemBasedPrincipal) {
                prinicpalOakPath = getOakPath((ItemBasedPrincipal) principal);
            }
            if (prinicpalOakPath == null || !root.getTree(prinicpalOakPath).exists()) {
                // the given principal is not ItemBasedPrincipal or it has been obtained with a different name-path-mapper
                // (making the conversion to oak-path return null) or it has been moved and the path no longer points to
                // an existing tree -> try looking up principal by name
                Principal p = principalProvider.getPrincipal(principal.getName());
                if (p instanceof ItemBasedPrincipal) {
                    prinicpalOakPath = getOakPath((ItemBasedPrincipal) p);
                } else {
                    prinicpalOakPath = null;
                }
            }
            return prinicpalOakPath;
        }

        @Nullable
        private String getOakPath(@NotNull ItemBasedPrincipal principal) {
            try {
                return namePathMapper.getOakPath(principal.getPath());
            } catch (RepositoryException e) {
                log.error("Error while retrieving path from ItemBasedPrincipal {}, {}", principal.getName(), e.getMessage());
                return null;
            }
        }
    }
}
