/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite.checks;

import java.util.Set;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.plugins.name.ReadOnlyNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Sets;

@Component
@Service(MountedNodeStoreChecker.class)
public class NamespacePrefixNodestoreChecker implements MountedNodeStoreChecker<NamespacePrefixNodestoreChecker.Context> {

    @Override
    public Context createContext(NodeStore globalStore, MountInfoProvider mip) {

        Root root = RootFactory.createReadOnlyRoot(globalStore.getRoot());
        
        ReadOnlyNamespaceRegistry registry = new ReadOnlyNamespaceRegistry(root);
        
        return new Context(registry.getPrefixes());
    }

    @Override
    public boolean check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Context context) {
        
        String name = tree.getName();
        String path = tree.getPath();
        
        validate(mountedStore, errorHolder, context, name, path);
        
        for ( PropertyState prop : tree.getProperties() ) {
            String propName = prop.getName();
            validate(mountedStore, errorHolder, context, propName, PathUtils.concat(tree.getPath(), propName));
        }
        
        return true;
    }

    private void validate(MountedNodeStore mountedStore, ErrorHolder errorHolder, Context context, String name,
            String path) {
        
        String prefix = getPrefix(name);
        if ( prefix != null && !context.validPrefixes.contains(prefix) ) {
            errorHolder.report(mountedStore, path, "invalid namespace prefix " + prefix + " , expected one of " + context.validPrefixes);
        }
    }
    
    private static String getPrefix(String name) {
        int idx = name.indexOf(':');
        if ( idx < 0 ) {
            return null;
        }
        
        // name will not start with a colon as it's an invalid JCR name
        // and we assume the repositories to be well-formed
        return name.substring(0, idx);
    }
    
    static class Context {

        private final Set<String> validPrefixes = Sets.newHashSet();
        
        public Context(String[] prefixes) {
            for (String prefix : prefixes ) {
                validPrefixes.add(prefix);
            }
        }
    }

}
