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
package org.apache.jackrabbit.oak.composite.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.composite.impl.NonDefaultMountWriteReportingObserver.Config;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports writes to non-default mounts
 * 
 * <p>This is a <em>diagnostic observer</em> and is expected to be used in scenarios where the
 * <code>CompositeNodeStore</code> is configured in a 'seed' mode, where the non-default
 * mounts are write-enabled.</p>
 * 
 * <p>In such scenarios it is useful to report writes to non-default mounts from components
 * that are unexpected. For instance, it can report all writes that do not originate
 * from the FileVault package installer.</p>
 * 
 * <p>Performance note: the overhead of this observer has not been measured, but as it is
 * designed to be used only for initial setups the performance impact should not
 * matter.</p>
 *
 */
@Component(service =  Observer.class, configurationPolicy =  ConfigurationPolicy.REQUIRE)
@Designate(ocd = Config.class)
public class NonDefaultMountWriteReportingObserver implements Observer {

    @ObjectClassDefinition
    public @interface Config {
        @AttributeDefinition(description = "Class name fragments that, when found in the stack trace, cause the writes to not be reported. Examples: org.apache.jackrabbit.vault.packaging.impl.JcrPackageImpl, org.apache.jackrabbit.vault.packaging.impl.JcrPackageImpl,org.apache.sling.jcr.repoinit")
        String[] ignoredClassNameFragments();
    }
    
    @Reference
    private MountInfoProvider mountInfoProvider;
    private Config cfg;
    
    private ChangeReporter reporter = new ChangeReporter();
    private NodeState oldState = null;

    protected void activate(Config cfg) {
        this.cfg = cfg;
    }

    // method copied from DiffObserver since we need to report at the end, when all the changes
    // have been accumulated
    @Override
    public final synchronized void contentChanged(NodeState root, CommitInfo info) {
        checkNotNull(root);
        if (oldState != null) {
            CountingDiff diff = new CountingDiff("/", new LinkedHashMap<>());
            root.compareAgainstBaseState(oldState, diff);
            diff.report();
        }
        oldState = root;
    }    
    
    class CountingDiff extends DefaultNodeStateDiff {
        private final String path;
        private final Map<String, String> changes;
        
        private CountingDiff(String path, Map<String, String> changes) {
            this.path = path;
            this.changes = changes;
        }
        
        public void report() {
            if ( changes.isEmpty() )
                return;
            
            RuntimeException location = new RuntimeException();
            for ( StackTraceElement element : location.getStackTrace() )
                for ( String acceptedClassName : cfg.ignoredClassNameFragments() )
                    if ( element.getClassName().contains(acceptedClassName ))
                        return;
            
            reporter.reportChanges(changes, location);
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            return onChange(name, before, EmptyNodeState.MISSING_NODE, "Deleted");
        }
        
        private boolean onChange(String name, NodeState before, NodeState after, String changeType) {
            String childPath = PathUtils.concat(path, name);

            boolean isCovered = mountInfoProvider.getNonDefaultMounts().stream()
                    .anyMatch( m -> m.isMounted(childPath)) ;
            
            if ( isCovered )
                changes.put(childPath, changeType);
            
            return after.compareAgainstBaseState(before, new CountingDiff(childPath, changes));            
        }
        
        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            
            return onChange(name, before, after, "Changed");
        }
        
        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            
            return onChange(name, EmptyNodeState.MISSING_NODE, after, "Added");
        }
    }
    
    // visible for testing
    void setReporter(ChangeReporter reporter) {
        this.reporter = reporter;
    }
    
    static class ChangeReporter {
        
        private static final int LOG_OUTPUT_MAX_ITEMS = 50;
        
        private final Logger logger = LoggerFactory.getLogger(getClass());
        
        void reportChanges(Map<String, String> changes, RuntimeException location) {
            
            if ( !logger.isWarnEnabled() )
                return;
            
            StringBuilder out = new StringBuilder();
            out.append("Unexpected changes (")
                .append(changes.size())
                .append(") performed on a non-default mount. Printing at most ")
                .append(LOG_OUTPUT_MAX_ITEMS);
            
            changes.entrySet().stream()
                .limit(LOG_OUTPUT_MAX_ITEMS)
                .forEach( e -> 
                    out.append("\n- ").append(e.getKey()).append(" : ").append(e.getValue())
            );

            logger.warn(out.toString(), location);
        }
    }
}
