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
package org.apache.jackrabbit.oak.exercise.security.authorization.restriction;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.util.Map;
import java.util.Set;

/**
 * EXERCISE: complete the implementation
 */
@Component(service = RestrictionProvider.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = CustomRestrictionProvider.Configuration.class)
public class CustomRestrictionProvider implements RestrictionProvider {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak CustomRestrictionProvider (Oak Exercises)")
    @interface Configuration {
        // EXERCISE
    }

    @NotNull
    @Override
    public Set<RestrictionDefinition> getSupportedRestrictions(@Nullable String oakPath) {
        // EXERCISE
        return null;
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value value) throws RepositoryException {
        // EXERCISE
        return null;
    }

    @NotNull
    @Override
    public Restriction createRestriction(@Nullable String oakPath, @NotNull String oakName, @NotNull Value... values) throws RepositoryException {
        // EXERCISE
        return null;
    }

    @NotNull
    @Override
    public Set<Restriction> readRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) {
        // EXERCISE
        return null;
    }

    @Override
    public void writeRestrictions(@Nullable String oakPath, @NotNull Tree aceTree, @NotNull Set<Restriction> restrictions) throws RepositoryException {
        // EXERCISE

    }

    @Override
    public void validateRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) throws RepositoryException {
        // EXERCISE

    }

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
        // EXERCISE
        return null;
    }

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
        // EXERCISE
        return null;
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(Map<String, Object> properties) {
        // EXERCISE
    }

    @SuppressWarnings("UnusedDeclaration")
    @Modified
    private void modified(Map<String, Object> properties) {
        // EXERCISE
    }

    @SuppressWarnings("UnusedDeclaration")
    @Deactivate
    private void deactivate(Map<String, Object> properties) {
        // EXERCISE
    }
}
