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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;
import org.apache.jackrabbit.oak.jcr.observation.filter.OakEventFilter;

/**
 * Implements OakEventFilter which is an extension to the JackrabbitEventFilter
 * with features only supported by Oak.
 */
public class OakEventFilterImpl extends OakEventFilter {

    private final JackrabbitEventFilter delegate;
    
    /** whether or not applyNodeTypeOnSelf feature is enabled */
    private boolean applyNodeTypeOnSelf;

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

}
