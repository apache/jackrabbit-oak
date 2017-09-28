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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositePattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default restriction provider implementation that supports the following
 * restrictions:
 *
 * <ul>
 *     <li>{@link #REP_GLOB}: A simple paths matching pattern. See {@link GlobPattern}
 *     for details.</li>
 *     <li>{@link #REP_NT_NAMES}: A restriction that allows to limit the effect
 *     of a given access control entries to JCR nodes of any of the specified
 *     primary node type. In case of a JCR property the primary type of the
 *     parent node is taken into consideration when evaluating the permissions.</li>
 *     <li>{@link #REP_PREFIXES}: A multivalued access control restriction
 *     which matches by name space prefix. The corresponding restriction type
 *     is {@link org.apache.jackrabbit.oak.api.Type#STRINGS}.</li>
 * </ul>
 */
@Component(
        service = RestrictionProvider.class,
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl")
public class RestrictionProviderImpl extends AbstractRestrictionProvider {

    private static final Logger log = LoggerFactory.getLogger(RestrictionProviderImpl.class);

    private static final int NUMBER_OF_DEFINITIONS = 3;

    public RestrictionProviderImpl() {
        super(supportedRestrictions());
    }

    private static Map<String, RestrictionDefinition> supportedRestrictions() {
        RestrictionDefinition glob = new RestrictionDefinitionImpl(REP_GLOB, Type.STRING, false);
        RestrictionDefinition nts = new RestrictionDefinitionImpl(REP_NT_NAMES, Type.NAMES, false);
        RestrictionDefinition pfxs = new RestrictionDefinitionImpl(REP_PREFIXES, Type.STRINGS, false);
        RestrictionDefinition names = new RestrictionDefinitionImpl(REP_ITEM_NAMES, Type.NAMES, false);
        return ImmutableMap.of(glob.getName(), glob, nts.getName(), nts, pfxs.getName(), pfxs, names.getName(), names);
    }

    //------------------------------------------------< RestrictionProvider >---

    @Nonnull
    @Override
    public RestrictionPattern getPattern(String oakPath, @Nonnull Tree tree) {
        if (oakPath == null) {
            return RestrictionPattern.EMPTY;
        } else {
            List<RestrictionPattern> patterns = new ArrayList<RestrictionPattern>(NUMBER_OF_DEFINITIONS);
            PropertyState glob = tree.getProperty(REP_GLOB);
            if (glob != null) {
                patterns.add(GlobPattern.create(oakPath, glob.getValue(Type.STRING)));
            }
            PropertyState ntNames = tree.getProperty(REP_NT_NAMES);
            if (ntNames != null) {
                patterns.add(new NodeTypePattern(ntNames.getValue(Type.NAMES)));
            }
            PropertyState prefixes = tree.getProperty(REP_PREFIXES);
            if (prefixes != null) {
                patterns.add(new PrefixPattern(prefixes.getValue(Type.STRINGS)));
            }
            PropertyState itemNames = tree.getProperty(REP_ITEM_NAMES);
            if (itemNames != null) {
                patterns.add(new ItemNamePattern(itemNames.getValue(Type.NAMES)));
            }

            return CompositePattern.create(patterns);
        }
    }

    @Nonnull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
        if (oakPath == null || restrictions.isEmpty()) {
            return RestrictionPattern.EMPTY;
        } else {
            List<RestrictionPattern> patterns = new ArrayList<RestrictionPattern>(NUMBER_OF_DEFINITIONS);
            for (Restriction r : restrictions) {
                String name = r.getDefinition().getName();
                if (REP_GLOB.equals(name)) {
                    patterns.add(GlobPattern.create(oakPath, r.getProperty().getValue(Type.STRING)));
                } else if (REP_NT_NAMES.equals(name)) {
                    patterns.add(new NodeTypePattern(r.getProperty().getValue(Type.NAMES)));
                } else if (REP_PREFIXES.equals(name)) {
                    patterns.add(new PrefixPattern(r.getProperty().getValue(Type.STRINGS)));
                } else if (REP_ITEM_NAMES.equals(name)) {
                    patterns.add(new ItemNamePattern(r.getProperty().getValue(Type.NAMES)));
                } else {
                    log.debug("Ignoring unsupported restriction " + name);
                }
            }
            return CompositePattern.create(patterns);
        }
    }

    @Override
    public void validateRestrictions(String oakPath, @Nonnull Tree aceTree) throws AccessControlException {
        super.validateRestrictions(oakPath, aceTree);

        Tree restrictionsTree = getRestrictionsTree(aceTree);
        PropertyState glob = restrictionsTree.getProperty(REP_GLOB);
        if (glob != null) {
            GlobPattern.validate(glob.getValue(Type.STRING));
        }
    }
}
