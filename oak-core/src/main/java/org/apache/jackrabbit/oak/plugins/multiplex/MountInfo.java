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

package org.apache.jackrabbit.oak.plugins.multiplex;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.spi.mount.Mount;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

final class MountInfo implements Mount {

    private static final Function<String, String> SANITIZE_PATH =  new Function<String,String>() {
        @Override
        public String apply(String input) {
            if ( input.endsWith("/") && input.length() > 1) {
                return input.substring(0, input.length() - 1); 
            }
            return input;
        }
    };

    private final String name;
    private final boolean readOnly;
    private final boolean defaultMount;
    private final String pathFragmentName;
    private final List<String> includedPaths;

    public MountInfo(String name, boolean readOnly, boolean defaultMount,
            List<String> includedPaths) {
        this.name = checkNotNull(name, "Mount name must not be null");
        this.readOnly = readOnly;
        this.defaultMount = defaultMount;
        this.pathFragmentName = "oak:mount-" + name;
        this.includedPaths = cleanCopy(includedPaths);
    }

    @Override
    public boolean isUnder(String path) {
        path = SANITIZE_PATH.apply(path);
        for (String includedPath : includedPaths) {
            if (isAncestor(path, includedPath)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isDirectlyUnder(String path) {
        path = SANITIZE_PATH.apply(path);
        for (String includedPath : includedPaths) {
            if (getParentPath(includedPath).equals(path)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isMounted(String path) {
        if (path.contains(pathFragmentName)){
            return true;
        }

        path = SANITIZE_PATH.apply(path);

        //TODO may be optimized via trie
        
        for (String includedPath : includedPaths){
            if ( includedPath.equals(path) || isAncestor(includedPath, path)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public boolean isDefault() {
        return defaultMount;
    }

    @Override
    public String getPathFragmentName() {
        return pathFragmentName;
    }

    private static ImmutableList<String> cleanCopy(List<String> includedPaths) {
        // ensure that paths don't have trailing slashes - this triggers an assertion in PahtUtils isAncestor
        return ImmutableList.copyOf(Iterables.transform(includedPaths, SANITIZE_PATH));
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String readAttr = readOnly ? "r" : "rw";
        String displayName = defaultMount ? "default" : name;
        pw.print(displayName + "(" + readAttr + ")");
        for (String path : includedPaths) {
            pw.printf("\t%s%n", path);
        }
        return sw.toString();
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MountInfo other = (MountInfo) obj;
        return name.equals(other.name);
    }
}
