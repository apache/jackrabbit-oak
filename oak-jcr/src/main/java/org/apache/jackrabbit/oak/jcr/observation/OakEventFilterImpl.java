/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.observation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static javax.jcr.observation.Event.NODE_REMOVED;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.observation.filter.OakEventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder;
import org.apache.jackrabbit.oak.plugins.observation.filter.PermissionProviderFactory;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder.Condition;

/**
 * Implements OakEventFilter which is an extension to the JackrabbitEventFilter
 * with features only supported by Oak.
 */
public class OakEventFilterImpl extends OakEventFilter {

    private final JackrabbitEventFilter delegate;
    
    /** whether or not applyNodeTypeOnSelf feature is enabled */
    private boolean applyNodeTypeOnSelf;

    /** whether or not includeAncestorsRemove feature is enabled */
    private boolean includeAncestorRemove;

    public OakEventFilterImpl(@Nonnull JackrabbitEventFilter delegate) {
        checkNotNull(delegate);
        this.delegate = delegate;
    }

    @Override
    public String getAbsPath() {
        return delegate.getAbsPath();
    }
    
    @Override
    public JackrabbitEventFilter setAbsPath(String absPath) {
        delegate.setAbsPath(absPath);
        return this;
    }
    
    @Override
    public String[] getAdditionalPaths() {
        return delegate.getAdditionalPaths();
    }

    @Override
    public JackrabbitEventFilter setAdditionalPaths(String... absPaths) {
        delegate.setAdditionalPaths(absPaths);
        return this;
    }

    @Override
    public int getEventTypes() {
        return delegate.getEventTypes();
    }
    
    @Override
    public JackrabbitEventFilter setEventTypes(int eventTypes) {
        delegate.setEventTypes(eventTypes);
        return this;
    }

    @Override
    public String[] getExcludedPaths() {
        return delegate.getExcludedPaths();
    }
    
    @Override
    public JackrabbitEventFilter setExcludedPaths(String... excludedPaths) {
        delegate.setExcludedPaths(excludedPaths);
        return this;
    }
    
    @Override
    public String[] getIdentifiers() {
        return delegate.getIdentifiers();
    }

    @Override
    public JackrabbitEventFilter setIdentifiers(String[] identifiers) {
        delegate.setIdentifiers(identifiers);
        return this;
    }

    @Override
    public boolean getIsDeep() {
        return delegate.getIsDeep();
    }
    
    @Override
    public JackrabbitEventFilter setIsDeep(boolean isDeep) {
        delegate.setIsDeep(isDeep);
        return this;
    }
    
    @Override
    public String[] getNodeTypes() {
        return delegate.getNodeTypes();
    }

    @Override
    public JackrabbitEventFilter setNodeTypes(String[] nodeTypeNames) {
        delegate.setNodeTypes(nodeTypeNames);
        return this;
    }
    
    @Override
    public boolean getNoExternal() {
        return delegate.getNoExternal();
    }

    @Override
    public JackrabbitEventFilter setNoExternal(boolean noExternal) {
        delegate.setNoExternal(noExternal);
        return this;
    }

    @Override
    public boolean getNoInternal() {
        return delegate.getNoInternal();
    }
    
    @Override
    public JackrabbitEventFilter setNoInternal(boolean noInternal) {
        delegate.setNoInternal(noInternal);
        return this;
    }

    @Override
    public boolean getNoLocal() {
        return delegate.getNoLocal();
    }
    
    @Override
    public JackrabbitEventFilter setNoLocal(boolean noLocal) {
        delegate.setNoLocal(noLocal);
        return this;
    }

    @Override
    public OakEventFilter withApplyNodeTypeOnSelf() {
        this.applyNodeTypeOnSelf = true;
        return this;
    }
    
    boolean getApplyNodeTypeOnSelf() {
        return applyNodeTypeOnSelf;
    }

    @Override
    public OakEventFilter withIncludeAncestorsRemove() {
        this.includeAncestorRemove = true;
        return this;
    }

    boolean getIncludeAncestorsRemove() {
        return includeAncestorRemove;
    }

    private void addAncestorsRemoveCondition(Set<String> parentPaths, String globPath) {
        if (globPath == null || !globPath.contains("/")) {
            return;
        }
        // from /a/b/c         => add /a and /a/b
        // from /a/b/**        => add /a
        // from /a             => add nothing
        // from /              => add nothing
        // from /a/b/**/*.html => add /a
        // from /a/b/*/*.html  => add /a

        Iterator<String> it = PathUtils.elements(globPath).iterator();
        StringBuffer sb = new StringBuffer();
        while(it.hasNext()) {
            String element = it.next();
            if (element.contains("*")) {
                if (parentPaths.size() > 0) {
                    parentPaths.remove(parentPaths.size()-1);
                }
                break;
            } else if (!it.hasNext()) {
                break;
            }
            sb.append("/");
            sb.append(element);
            parentPaths.add(sb.toString() + "/*");
        }
    }

    public Condition wrapMainCondition(Condition mainCondition, FilterBuilder filterBuilder, PermissionProviderFactory permissionProviderFactory) {
        if (!includeAncestorRemove || (getEventTypes() & NODE_REMOVED) != NODE_REMOVED) {
            return mainCondition;
        }
        Set<String> parentPaths = new HashSet<String>();
        addAncestorsRemoveCondition(parentPaths, getAbsPath());
        if (getAdditionalPaths() != null) {
            for (String absPath : getAdditionalPaths()) {
                addAncestorsRemoveCondition(parentPaths, absPath);
            }
        }
//        if (globPaths != null) {
//            for (String globPath : globPaths) {
//                addAncestorsRemoveCondition(parentPaths, globPath);
//            }
//        }
        if (parentPaths.size() == 0) {
            return mainCondition;
        }
        List<Condition> ancestorsRemoveConditions = new LinkedList<Condition>();
        for (String aParentPath : parentPaths) {
            ancestorsRemoveConditions.add(filterBuilder.path(aParentPath));
        }
        return filterBuilder.any(
                        mainCondition,
                        filterBuilder.all(
                                filterBuilder.eventType(NODE_REMOVED),
                                filterBuilder.any(ancestorsRemoveConditions),
                                filterBuilder.deleteSubtree(),
                                filterBuilder.accessControl(permissionProviderFactory)
                                )
                        );
    }

}
