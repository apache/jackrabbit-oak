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
import static com.google.common.collect.Lists.newArrayList;
import static javax.jcr.observation.Event.NODE_REMOVED;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.observation.filter.OakEventFilter;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.observation.filter.ConstantFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventAggregator;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder.Condition;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.PermissionProviderFactory;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Implements OakEventFilter which is an extension to the JackrabbitEventFilter
 * with features only supported by Oak.
 */
public class OakEventFilterImpl extends OakEventFilter {

}
