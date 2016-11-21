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
package org.apache.jackrabbit.oak.plugins.observation;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toInteger;
import static org.apache.jackrabbit.oak.commons.PropertiesUtil.toBoolean;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * A ChangeCollectorProvider can be hooked into Oak thus enabling the collection
 * of ChangeSets of changed items of a commit, which downstream Observers can
 * then use at their convenience.
 * <p>
 * 
 * @see ChangeSet for details on what is tracked and how that data should be
 *      interpreted
 */
@Component(
        immediate = true,
        metatype = true,
        label = "Apache Jackrabbit Oak Change Collector Service",
        description = "It hooks into the commit and collects a ChangeSet of changed items of a commit which " +
                "is then used to speed up observation processing"
)
@Property(name = "type", value = ChangeCollectorProvider.TYPE, propertyPrivate = true)
@Service(ValidatorProvider.class)
public class ChangeCollectorProvider extends ValidatorProvider {
    public static final String TYPE = "changeCollectorProvider";

    private static final Logger LOG = LoggerFactory.getLogger(ChangeCollectorProvider.class);

    public static final String COMMIT_CONTEXT_OBSERVATION_CHANGESET = "oak.observation.changeSet";

    private static final int DEFAULT_MAX_ITEMS = 50;
    @Property(longValue = DEFAULT_MAX_ITEMS, label = "Maximum Number of Collected Items (per type)", description = "Integer value indicating maximum number of individual items of changes - "
            + "such as property, nodeType, node name, path - to be collected. If there are "
            + "more changes, the collection is considered failed and marked as such. " + "Default is "
            + DEFAULT_MAX_ITEMS)
    private static final String PROP_MAX_ITEMS = "maxItems";

    private static final int DEFAULT_MAX_PATH_DEPTH = 9;
    @Property(longValue = DEFAULT_MAX_PATH_DEPTH, label = "Maximum depth of paths to collect", description = "Integer value indicating maximum depth of paths to collect. "
            + "Paths deeper than this will not be individually reported, and instead "
            + "a path at this max depth will be added. Note that this doesn't affect "
            + "any other collected item such as property, nodeType - ie those will "
            + "all be collected irrespective of this config param." + "Default is " + DEFAULT_MAX_PATH_DEPTH)
    private static final String PROP_MAX_PATH_DEPTH = "maxPathDepth";

    private static final boolean DEFAULT_ENABLED = true;
    @Property(boolValue = DEFAULT_ENABLED, label = "enable/disable this validator", 
            description = "Whether this validator is enabled. If disabled no ChangeSet will be generated. Default is "
            + DEFAULT_ENABLED)
    private static final String PROP_ENABLED = "enabled";

    /**
     * There is one CollectorSupport per validation process - it is shared
     * between multiple instances of ChangeCollector (Validator) - however it
     * can remain unsynchronized as validators are executed single-threaded.
     */
    private static class CollectorSupport {
        private final CommitInfo info;
        private final int maxPathDepth;
        private final ChangeSetBuilder changeSetBuilder;

        private CollectorSupport(@Nonnull CommitInfo info, @Nonnull ChangeSetBuilder changeSetBuilder,
                int maxPathDepth) {
            this.info = info;
            this.changeSetBuilder = changeSetBuilder;
            this.maxPathDepth = maxPathDepth;
        }

        @Override
        public String toString() {
            return "CollectorSupport with " + changeSetBuilder;
        }

        private CommitInfo getInfo() {
            return info;
        }

        private int getMaxPathDepth() {
            return maxPathDepth;
        }

        private ChangeSetBuilder getChangeSetBuilder() {
            return changeSetBuilder;
        }

        private Set<String> getParentPaths() {
            return changeSetBuilder.getParentPaths();
        }

        private Set<String> getParentNodeNames() {
            return changeSetBuilder.getParentNodeNames();
        }

        private Set<String> getParentNodeTypes() {
            return changeSetBuilder.getParentNodeTypes();
        }

        private Set<String> getPropertyNames() {
            return changeSetBuilder.getPropertyNames();
        }

        private Set<String> getAllNodeTypes() {
            return changeSetBuilder.getAllNodeTypes();
        }

    }

    /**
     * ChangeCollectors are the actual working-horse Validators that are created
     * for each level thus as a whole propage through the entire change.
     * <p>
     * The actual data is collected via a per-commit CollectorSupport and its
     * underlying ChangeSet (the latter is where the actual changes end up in).
     * <p>
     * When finished - ie in the last==root leave() - the resulting ChangeSet is
     * marked immutable and set in the CommitContext.
     */
    private static class ChangeCollector implements Validator {

        private final CollectorSupport support;

        private final boolean isRoot;
        private final NodeState beforeParentNodeOrNull;
        private final NodeState afterParentNodeOrNull;
        private final String path;
        private final String childName;
        private final int level;

        private boolean changed;

        private static ChangeCollector newRootCollector(@Nonnull CommitInfo info, int maxItems, int maxPathDepth) {
            ChangeSetBuilder changeSetBuilder = new ChangeSetBuilder(maxItems, maxPathDepth);
            CollectorSupport support = new CollectorSupport(info, changeSetBuilder, maxPathDepth);
            return new ChangeCollector(support, true, null, null, "/", null, 0);
        }

        private ChangeCollector newChildCollector(@Nullable NodeState beforeParentNodeOrNull, @Nullable NodeState afterParentNodeOrNull, @Nonnull String childName) {
            return new ChangeCollector(support, false, beforeParentNodeOrNull, afterParentNodeOrNull, concat(path, childName), childName, level + 1);
        }

        private ChangeCollector(@Nonnull CollectorSupport support, boolean isRoot, @Nullable NodeState beforeParentNodeOrNull,
                @Nullable NodeState afterParentNodeOrNull, @Nonnull String path, @Nullable String childNameOrNull, int level) {
            this.support = support;
            this.isRoot = isRoot;
            this.beforeParentNodeOrNull = beforeParentNodeOrNull;
            this.afterParentNodeOrNull = afterParentNodeOrNull;
            this.path = path;
            this.childName = childNameOrNull;
            this.level = level;
        }

        @Override
        public String toString() {
            return "ChangeCollector[path=" + path + "]";
        }

        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
            // nothing to be done here
        }

        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            // first check if we have to add anything to paths and/or nodeNames
            if (changed && level <= support.getMaxPathDepth()) {
                support.getParentPaths().add(path);
            }
            if (changed && childName != null) {
                support.getParentNodeNames().add(childName);
            }
            if (changed && beforeParentNodeOrNull != null) {
                String primaryType = beforeParentNodeOrNull.getName(JcrConstants.JCR_PRIMARYTYPE);
                if (primaryType != null) {
                    support.getParentNodeTypes().add(primaryType);
                }
                Iterables.addAll(support.getParentNodeTypes(), beforeParentNodeOrNull.getNames(JcrConstants.JCR_MIXINTYPES));
            }
            if (changed && afterParentNodeOrNull != null) {
                String primaryType = afterParentNodeOrNull.getName(JcrConstants.JCR_PRIMARYTYPE);
                if (primaryType != null) {
                    support.getParentNodeTypes().add(primaryType);
                }
                Iterables.addAll(support.getParentNodeTypes(), afterParentNodeOrNull.getNames(JcrConstants.JCR_MIXINTYPES));
            }

            // then if we're not at the root, we're done
            if (!isRoot) {
                return;
            }

            // but if we're at the root, then we add the ChangeSet to the
            // CommitContext of the CommitInfo
            CommitContext commitContext = (CommitContext) support.getInfo().getInfo().get(CommitContext.NAME);
            ChangeSet changeSet = support.getChangeSetBuilder().build();
            commitContext.set(COMMIT_CONTEXT_OBSERVATION_CHANGESET, changeSet);
            LOG.debug("Collected changeSet for commit {} is {}", support.getInfo(), changeSet);
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            changed = true;
            support.getPropertyNames().add(after.getName());
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            changed = true;
            support.getPropertyNames().add(before.getName());
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            changed = true;
            support.getPropertyNames().add(before.getName());
        }

        @Override
        public Validator childNodeAdded(String childName, NodeState after) throws CommitFailedException {
            changed = true;
            addToAllNodeType(after);
            return newChildCollector(null, after, childName);
        }

        @Override
        public Validator childNodeChanged(String childName, NodeState before, NodeState after)
                throws CommitFailedException {
            if (level == support.getMaxPathDepth()) {
                // then we'll cut off further paths below.
                // to compensate, add the current path at this level
                support.getParentPaths().add(path);

                // however, continue normally to handle names/types/properties
                // below
            }
            
            // in theory the node type could be changed, so collecting both before and after
            addToAllNodeType(before);
            addToAllNodeType(after);

            return newChildCollector(before, after, childName);
        }

        @Override
        public Validator childNodeDeleted(String childName, NodeState before) throws CommitFailedException {
            changed = true;
            addToAllNodeType(before);
            return newChildCollector(before, null, childName);
        }
        
        private void addToAllNodeType(NodeState state) {
            String primaryType = state.getName(JcrConstants.JCR_PRIMARYTYPE);
            if (primaryType != null) {
                support.getAllNodeTypes().add(primaryType);
            }
            Iterables.addAll(support.getAllNodeTypes(), state.getNames(JcrConstants.JCR_MIXINTYPES));
        }

    }

    private int maxItems = DEFAULT_MAX_ITEMS;

    private int maxPathDepth = DEFAULT_MAX_PATH_DEPTH;
    
    private boolean enabled = DEFAULT_ENABLED;

    @Activate
    protected void activate(ComponentContext context, Map<String, ?> config) {
        reconfig(config);
        LOG.info("activate: maxItems=" + maxItems + ", maxPathDepth=" + maxPathDepth + ", enabled=" + enabled);
    }

    @Modified
    protected void modified(final Map<String, Object> config) {
        reconfig(config);
        LOG.info("modified: maxItems=" + maxItems + ", maxPathDepth=" + maxPathDepth + ", enabled=" + enabled);
    }

    private void reconfig(Map<String, ?> config) {
        maxItems = toInteger(config.get(PROP_MAX_ITEMS), DEFAULT_MAX_ITEMS);
        maxPathDepth = toInteger(config.get(PROP_MAX_PATH_DEPTH), DEFAULT_MAX_PATH_DEPTH);
        enabled = toBoolean(config.get(PROP_ENABLED), DEFAULT_ENABLED);
    }

    /** FOR TESTING-ONLY **/
    protected void setMaxPathDepth(int maxPathDepth) {
        this.maxPathDepth = maxPathDepth;
    }

    /** FOR TESTING-ONLY **/
    protected int getMaxPathDepth() {
        return this.maxPathDepth;
    }

    /** FOR TESTING-ONLY **/
    protected void setMaxItems(int maxItems) {
        this.maxItems = maxItems;
    }

    /** FOR TESTING-ONLY **/
    protected int getMaxItems() {
        return this.maxItems;
    }

    @Override
    protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        if (!enabled) {
            return null;
        }
        if (info == null || !info.getInfo().containsKey(CommitContext.NAME)) {
            // then we cannot do change-collecting, as we can't store
            // it in the info
            return null;
        }

        return ChangeCollector.newRootCollector(info, maxItems, maxPathDepth);
    }

}
