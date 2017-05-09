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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR;
import static org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter.STAR_STAR;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;

import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;
import org.apache.jackrabbit.api.observation.JackrabbitObservationManager;
import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.observation.ExcludeExternal;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder.Condition;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.plugins.observation.filter.PermissionProviderFactory;
import org.apache.jackrabbit.oak.plugins.observation.filter.ChangeSetFilterImpl;
import org.apache.jackrabbit.oak.plugins.observation.filter.Selectors;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class ObservationManagerImpl implements JackrabbitObservationManager {
    private static final Logger LOG = LoggerFactory.getLogger(ObservationManagerImpl.class);
    private static final int STOP_TIME_OUT = 1000;

    public static final Marker OBSERVATION =
            MarkerFactory.getMarker("observation");

    private static final Marker DEPRECATED =
            MarkerFactory.getMarker("deprecated");

    private final Map<EventListener, ChangeProcessor> processors =
            new HashMap<EventListener, ChangeProcessor>();

    private final SessionDelegate sessionDelegate;
    private final ReadOnlyNodeTypeManager ntMgr;
    private final AuthorizationConfiguration authorizationConfig;
    private final NamePathMapper namePathMapper;
    private final Whiteboard whiteboard;
    private final StatisticManager statisticManager;
    private final int queueLength;
    private final CommitRateLimiter commitRateLimiter;
    private final PermissionProviderFactory permissionProviderFactory;

    /**
     * Create a new instance based on a {@link ContentSession} that needs to implement
     * {@link Observable}.
     *
     * @param sessionContext   session delegate of the session in whose context this observation manager
     *                         operates.
     * @param nodeTypeManager  node type manager for the content session
     * @param whiteboard
     * @throws IllegalArgumentException if {@code contentSession} doesn't implement {@code Observable}.
     */
    public ObservationManagerImpl(
            SessionContext sessionContext, ReadOnlyNodeTypeManager nodeTypeManager,
            Whiteboard whiteboard, int queueLength, CommitRateLimiter commitRateLimiter) {

        this.sessionDelegate = sessionContext.getSessionDelegate();
        this.authorizationConfig = sessionContext.getSecurityProvider().getConfiguration(AuthorizationConfiguration.class);
        this.ntMgr = nodeTypeManager;
        this.namePathMapper = sessionContext;
        this.whiteboard = whiteboard;
        this.statisticManager = sessionContext.getStatisticManager();
        this.queueLength = queueLength;
        this.commitRateLimiter = commitRateLimiter;
        this.permissionProviderFactory = new PermissionProviderFactory() {
            Set<Principal> principals = sessionDelegate.getAuthInfo().getPrincipals();
            @Nonnull
            @Override
            public PermissionProvider create(Root root) {
                return authorizationConfig.getPermissionProvider(root,
                        sessionDelegate.getWorkspaceName(), principals);
            }
        };
    }

    public void dispose() {
        List<ChangeProcessor> toBeStopped;

        synchronized (this) {
            toBeStopped = newArrayList(processors.values());
            processors.clear();
        }

        for (ChangeProcessor processor : toBeStopped) {
            stop(processor);
        }
    }
    
    /** for testing only, hence package protected **/
    synchronized ChangeProcessor getChangeProcessor(EventListener listener) {
        return processors.get(listener);        
    }

    private synchronized void addEventListener(EventListener listener, ListenerTracker tracker,
            FilterProvider filterProvider) {

        ChangeProcessor processor = processors.get(listener);
        if (processor == null) {
            LOG.debug(OBSERVATION,
                    "Registering event listener {} with filter {}", listener, filterProvider);
            // TODO sharing the namePathMapper across different thread might lead to lock contention.
            // If this turns out to be problematic we might create a dedicated snapshot for each
            // session. See OAK-1368.
            processor = new ChangeProcessor(sessionDelegate.getContentSession(), namePathMapper,
                    tracker, filterProvider, statisticManager, queueLength,
                    commitRateLimiter);
            processors.put(listener, processor);
            processor.start(whiteboard);
        } else {
            LOG.debug(OBSERVATION,
                    "Changing event listener {} to filter {}", listener, filterProvider);
            processor.setFilterProvider(filterProvider);
        }
    }

    /**
     * Adds an event listener that listens for the events specified
     * by the {@code filterProvider} passed to this method.
     * <p>
     * The set of events will be further filtered by the access rights
     * of the current {@code Session}.
     * <p>
     * The filters of an already-registered {@code EventListener} can be
     * changed at runtime by re-registering the same {@code EventListener}
     * object (i.e. the same actual Java object) with a new filter provider.
     * The implementation must ensure that no events are lost during the
     * changeover.
     *
     * @param listener        an {@link EventListener} object.
     * @param filterProvider  filter provider specifying the filter for this listener
     */
    public void addEventListener(EventListener listener, FilterProvider filterProvider) {
        // FIXME Add support for FilterProvider in ListenerTracker
        ListenerTracker tracker = new WarningListenerTracker(
                true, listener, 0, null, true, null, null, false);
        addEventListener(listener, tracker, filterProvider);
    }

    @Override
    public void addEventListener(EventListener listener, int eventTypes, String absPath,
            boolean isDeep, String[] uuids, String[] nodeTypeName, boolean noLocal)
            throws RepositoryException {

        JackrabbitEventFilter filter = new JackrabbitEventFilter();
        filter.setEventTypes(eventTypes);
        if (absPath != null) {
            filter.setAbsPath(absPath);
        }
        filter.setIsDeep(isDeep);
        if (uuids != null) {
            filter.setIdentifiers(uuids);
        }
        if (nodeTypeName != null) {
            filter.setNodeTypes(nodeTypeName);
        }
        filter.setNoLocal(noLocal);
        filter.setNoExternal(listener instanceof ExcludeExternal);
        addEventListener(listener, filter);
    }

    @Override
    public void addEventListener(EventListener listener, JackrabbitEventFilter filter)
            throws RepositoryException {
        OakEventFilterImpl oakEventFilter = null;
        if (filter instanceof OakEventFilterImpl) {
            oakEventFilter = (OakEventFilterImpl) filter;
        }

        int eventTypes = filter.getEventTypes();
        boolean isDeep = filter.getIsDeep();
        String[] uuids = filter.getIdentifiers();
        String[] nodeTypeName = filter.getNodeTypes();
        boolean noLocal = filter.getNoLocal();
        boolean noExternal = filter.getNoExternal() || listener instanceof ExcludeExternal;
        boolean noInternal = filter.getNoInternal();
        Set<String> includePaths = getOakPaths(namePathMapper, filter.getAdditionalPaths());
        String absPath = filter.getAbsPath();
        if (absPath != null) {
            includePaths.add(namePathMapper.getOakPath(absPath));
        }
        Set<String> excludedPaths = getOakPaths(namePathMapper, filter.getExcludedPaths());
        PathUtils.unifyInExcludes(includePaths, excludedPaths);
        if (oakEventFilter != null) {
            String[] includeGlobPaths = oakEventFilter.getIncludeGlobPaths();
            if (includeGlobPaths != null) {
                includePaths.addAll(Arrays.asList(includeGlobPaths));
            }
        }
        if (includePaths.isEmpty()) {
            LOG.warn("The passed filter excludes all events. No event listener registered");
            return;
        }

        FilterBuilder filterBuilder = new FilterBuilder();
        String depthPattern = isDeep ? STAR + '/' + STAR_STAR : STAR;
        List<Condition> includeConditions = newArrayList();
        filterBuilder.addPathsForMBean(includePaths);
        for (String path : includePaths) {
            final String deepenedPath;
            if (path.endsWith(STAR)) {
                // that's the case for a glob ending with * already, so
                // no need to add another * or **
                deepenedPath = path;
            } else if (path.contains(STAR)) {
                // for any other glob path that doesn't end with *
                // we only add a single *, not a **
                deepenedPath = concat (path, STAR);
            } else {
                // for any non-glob path we do it the traditional way
                deepenedPath = concat(path, depthPattern);
            }
            includeConditions.add(filterBuilder.path(deepenedPath));
            if (oakEventFilter != null && oakEventFilter.getIncludeAncestorsRemove()) {
                // with the 'includeAncestorsRemove' extension we need
                // to register '/' as the base path - done in wrapMainCondition
                // - in order to catch any node removal. So we have to skip adding 
                // the subtree here as a result.
                continue;
            }
            // only register the part leading to the first STAR:
            filterBuilder.addSubTree(pathWithoutGlob(path));
        }

        List<Condition> excludeConditions = createExclusions(filterBuilder, excludedPaths);

        final String[] validatedNodeTypeNames = validateNodeTypeNames(nodeTypeName);
        Selector nodeTypeSelector = Selectors.PARENT;
        boolean deleteSubtree = true;
        if (oakEventFilter != null) {
            Condition additionalIncludes = oakEventFilter.getAdditionalIncludeConditions(includePaths);
            if (additionalIncludes != null) {
                includeConditions.add(additionalIncludes);
            }
            filterBuilder.aggregator(oakEventFilter.getAggregator());
            if (oakEventFilter.getApplyNodeTypeOnSelf()) {
                nodeTypeSelector = Selectors.THIS;
            }
            if (oakEventFilter.getIncludeSubtreeOnRemove()) {
                deleteSubtree = false;
            }
        }
        if (deleteSubtree) {
            excludeConditions.add(filterBuilder.deleteSubtree());
        }

        Condition condition = filterBuilder.all(
                    filterBuilder.all(excludeConditions),
                    filterBuilder.any(includeConditions),
//                    filterBuilder.deleteSubtree(),     // moved depending on deleteSubtree on excludeConditions
                    filterBuilder.moveSubtree(),
                    filterBuilder.eventType(eventTypes),
                    filterBuilder.uuid(Selectors.PARENT, uuids),
                    filterBuilder.nodeType(nodeTypeSelector, validatedNodeTypeNames),
                    filterBuilder.accessControl(permissionProviderFactory));
        if (oakEventFilter != null) {
            condition = oakEventFilter.wrapMainCondition(condition, filterBuilder, permissionProviderFactory);
        }
        filterBuilder
            .includeSessionLocal(!noLocal)
            .includeClusterExternal(!noExternal)
            .includeClusterLocal(!noInternal)
            .condition(condition);

        // FIXME support multiple path in ListenerTracker
        ListenerTracker tracker = new WarningListenerTracker(
                !noExternal, listener, eventTypes, absPath, isDeep, uuids, nodeTypeName, noLocal);

        Set<String> additionalIncludePaths = null;
        if (oakEventFilter != null) {
            additionalIncludePaths = oakEventFilter.calcPrefilterIncludePaths(includePaths);
        }
        
        // OAK-5082 : node type filtering should not only be direct but include derived types
        // one easy way to solve this is to 'explode' the node types at start by including
        // all subtypes of every registered node type
        HashSet<String> explodedNodeTypes = null;
        if (validatedNodeTypeNames != null) {
            explodedNodeTypes = newHashSet();
            for (String nt : validatedNodeTypeNames) {
                explodeSubtypes(nt, explodedNodeTypes);
            }
        }
        
        // OAK-4908 : prefiltering support. here we have explicit yes/no/maybe filtering
        // for things like propertyNames/nodeTypes/nodeNames/paths which cannot be 
        // applied on the full-fledged filterBuilder above but requires an explicit 'prefilter' for that.
        filterBuilder.setChangeSetFilter(new ChangeSetFilterImpl(includePaths, isDeep, additionalIncludePaths, excludedPaths, null,
                explodedNodeTypes, null));
        
        addEventListener(listener, tracker, filterBuilder.build());
    }
    
    private void explodeSubtypes(String nodeType, Set<String> set) throws RepositoryException {
        set.add(nodeType);
        NodeTypeIterator it = ntMgr.getNodeType(nodeType).getSubtypes();
        while(it.hasNext()) {
            String subnt = String.valueOf(it.next());
            if (!set.contains(subnt)) {
                set.add(subnt);
                explodeSubtypes(subnt, set);
            }
        }
    }

    private String pathWithoutGlob(String path) {
        if (!path.contains("*")) {
            return path;
        }
        Iterator<String> it = elements(path).iterator();
        String result = "/";
        while(it.hasNext()) {
            String next = it.next();
            if (next.contains("*")) {
                // then stop here
                break;
            }
            result = concat(result, next);
        }
        return result;
    }

    private static List<Condition> createExclusions(FilterBuilder filterBuilder, Iterable<String> excludedPaths) {
        List<Condition> conditions = newArrayList();
        for (String path : excludedPaths) {
            conditions.add(filterBuilder.not(filterBuilder.path(path + '/' + STAR_STAR)));
        }
        return conditions;
    }

    private static Set<String> getOakPaths(NamePathMapper mapper, String[] paths) {
        Set<String> oakPaths = newHashSet();
        for (String path : paths) {
            oakPaths.add(mapper.getOakPath(path));
        }
        return oakPaths;
    }

    @Override
    public void removeEventListener(EventListener listener) {
        ChangeProcessor processor;
        synchronized (this) {
            processor = processors.remove(listener);
        }
        if (processor != null) {
            stop(processor); // needs to happen outside synchronization
        }
    }

    @Override
    public EventListenerIterator getRegisteredEventListeners() {
        return new EventListenerIteratorAdapter(processors.keySet());
    }

    @Override
    public void setUserData(@Nullable String userData) {
        sessionDelegate.setUserData(userData);
    }

    @Override
    public EventJournal getEventJournal() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public EventJournal getEventJournal(int eventTypes, String absPath, boolean isDeep, String[] uuid, String[]
            nodeTypeName) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    //------------------------------------------------------------< private >---

    /**
     * Validates the given node type names.
     *
     * @param nodeTypeNames the node type names.
     * @return the node type names as oak names.
     * @throws javax.jcr.nodetype.NoSuchNodeTypeException if one of the node type names refers to
     *                                 an non-existing node type.
     * @throws javax.jcr.RepositoryException     if an error occurs while reading from the
     *                                 node type manager.
     */
    @CheckForNull
    private String[] validateNodeTypeNames(@Nullable String[] nodeTypeNames)
            throws NoSuchNodeTypeException, RepositoryException {
        if (nodeTypeNames == null) {
            return null;
        }
        String[] oakNames = new String[nodeTypeNames.length];
        for (int i = 0; i < nodeTypeNames.length; i++) {
            ntMgr.getNodeType(nodeTypeNames[i]);
            oakNames[i] = namePathMapper.getOakName(nodeTypeNames[i]);
        }
        return oakNames;
    }

    private static void stop(ChangeProcessor processor) {
        if (!processor.stopAndWait(STOP_TIME_OUT, MILLISECONDS)) {
            LOG.warn(
                    OBSERVATION,
                    "Timed out waiting for change processor to stop after "
                            + STOP_TIME_OUT
                            + " milliseconds. Falling back to asynchronous stop on "
                            + processor
                            + " (listener details: '"
                            + processor.getListenerToString()
                            + "')");
            processor.stop();
        }
    }

    private class WarningListenerTracker extends ListenerTracker {
        private final boolean enableWarning;

        public WarningListenerTracker(
                boolean enableWarning, EventListener listener, int eventTypes, String absPath,
                boolean isDeep, String[] uuids, String[] nodeTypeName, boolean noLocal) {
            super(listener, eventTypes, absPath, isDeep, uuids, nodeTypeName, noLocal);
            this.enableWarning = enableWarning;
        }

        @Override
        protected void warn(String message) {
            if (enableWarning) {
                LOG.warn(DEPRECATED, message, initStackTrace);
            }
        }

        @Override
        protected void beforeEventDelivery() {
            sessionDelegate.refreshAtNextAccess();
        }
    }

}
