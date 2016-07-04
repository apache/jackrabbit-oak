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

package org.apache.jackrabbit.oak.spi.mount;

import static com.google.common.base.Preconditions.checkNotNull;

public final class Mount {
    /**
     * Default Mount info which indicates that no explicit mount
     * is created for given path
     */
    public static final Mount DEFAULT = new Mount("", false, true);

    private final String name;
    private final boolean readOnly;
    private final boolean defaultMount;
    private final String pathFragmentName;

    public Mount(String name){
        this(name, false);
    }

    public Mount(String name, boolean readOnly) {
       this(name, readOnly, false);
    }

    private Mount(String name, boolean readOnly, boolean defaultMount){
        this.name = checkNotNull(name, "Mount name must not be null");
        this.readOnly = readOnly;
        this.defaultMount = defaultMount;
        this.pathFragmentName = "oak:" + name;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isDefault(){
        return defaultMount;
    }

    /**
     * Decorated mount name which is meant to be used for constructing path
     * which should become part of given mount
     */
    public String getPathFragmentName() {
        return pathFragmentName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Mount mount = (Mount) o;

        return name.equals(mount.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        String readAttr = readOnly ? "r" : "rw";
        String displayName =  defaultMount ? "default" : name;
        return displayName + "(" + readAttr + ")";
    }
}
