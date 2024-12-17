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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.security.AccessControlException;

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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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

    private static final Map<String, RestrictionDefinition> DEFINITIONS;

    static {
        Map<String, RestrictionDefinition> tmp = new LinkedHashMap<>();
        tmp.put(REP_GLOB, new RestrictionDefinitionImpl(REP_GLOB, Type.STRING, false));
        tmp.put(REP_NT_NAMES, new RestrictionDefinitionImpl(REP_NT_NAMES, Type.NAMES, false));
        tmp.put(REP_PREFIXES, new RestrictionDefinitionImpl(REP_PREFIXES, Type.STRINGS, false));
        tmp.put(REP_ITEM_NAMES, new RestrictionDefinitionImpl(REP_ITEM_NAMES, Type.NAMES, false));
        tmp.put(REP_CURRENT, new RestrictionDefinitionImpl(REP_CURRENT, Type.STRINGS, false));
        tmp.put(REP_GLOBS, new RestrictionDefinitionImpl(REP_GLOBS, Type.STRINGS, false));
        tmp.put(REP_SUBTREES, new RestrictionDefinitionImpl(REP_SUBTREES, Type.STRINGS, false));
        DEFINITIONS = Collections.unmodifiableMap(tmp);
    }

    public RestrictionProviderImpl() {
        super(DEFINITIONS);
    }

    //------------------------------------------------< RestrictionProvider >---

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
        if (oakPath == null) {
            return RestrictionPattern.EMPTY;
        } else {
            List<RestrictionPattern> patterns = new ArrayList<>(DEFINITIONS.size());
            PropertyState glob = tree.getProperty(REP_GLOB);
            if (glob != null) {
                patterns.add(GlobPattern.create(oakPath, glob.getValue(Type.STRING)));
            }
            for (String name : new String[] {REP_NT_NAMES, REP_ITEM_NAMES}) {
                PropertyState ps = tree.getProperty(name);
                if (ps != null) {
                    patterns.add(createPattern(oakPath, name, ps.getValue(Type.NAMES)));
                }
            }
            for (String name : new String[] {REP_PREFIXES, REP_CURRENT, REP_GLOBS, REP_SUBTREES}) {
                PropertyState ps = tree.getProperty(name);
                if (ps != null) {
                    patterns.add(createPattern(oakPath, name, ps.getValue(Type.STRINGS)));
                }
            }
            return CompositePattern.create(patterns);
        }
    }
    
    private static RestrictionPattern createPattern(@NotNull String path, @NotNull String name, @NotNull Iterable<String> values) {
        switch (name) {
            case REP_ITEM_NAMES: return new ItemNamePattern(values);
            case REP_NT_NAMES: return new NodeTypePattern(values);
            case REP_PREFIXES: return new PrefixPattern(values);
            case REP_CURRENT:  return new CurrentPattern(path, values);
            case REP_GLOBS:    return new GlobsPattern(path, values);
            case REP_SUBTREES: return new SubtreePattern(path, values);
            default: {
                log.debug("Ignoring unsupported restriction {} at {}", name, path);
                return RestrictionPattern.EMPTY;
            }
        }
    }

    @NotNull
    @Override
    public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
        if (oakPath == null || restrictions.isEmpty()) {
            return RestrictionPattern.EMPTY;
        } else {
            List<RestrictionPattern> patterns = new ArrayList<>(DEFINITIONS.size());
            for (Restriction r : restrictions) {
                String name = r.getDefinition().getName();
                if (REP_GLOB.equals(name)) {
                    patterns.add(GlobPattern.create(oakPath, r.getProperty().getValue(Type.STRING)));
                } else if (REP_NT_NAMES.equals(name) || REP_ITEM_NAMES.equals(name)) {
                    patterns.add(createPattern(oakPath, name, r.getProperty().getValue(Type.NAMES)));
                } else {
                    patterns.add(createPattern(oakPath, name, r.getProperty().getValue(Type.STRINGS)));
                }
            }
            return CompositePattern.create(patterns);
        }
    }

    @Override
    public void validateRestrictions(@Nullable String oakPath, @NotNull Tree aceTree) throws AccessControlException {
        super.validateRestrictions(oakPath, aceTree);

        Tree restrictionsTree = getRestrictionsTree(aceTree);
        PropertyState glob = restrictionsTree.getProperty(REP_GLOB);
        if (glob != null) {
            GlobPattern.validate(glob.getValue(Type.STRING));
        }
        PropertyState globs = restrictionsTree.getProperty(REP_GLOBS);
        if (globs != null) {
            for (String v : globs.getValue(Type.STRINGS)) {
                GlobPattern.validate(v);
            }
        }
    }
}
