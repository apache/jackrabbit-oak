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

import java.util.Collection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Holds information related to the {@link Mount}s configured in a {@code ContentRepository}.
 * 
 * <p>The configuration may either be trivial - only a default mount is configured, or defining at least one non-default mount.</p>
 */
@ProviderType
public interface MountInfoProvider {

    /**
     * Maps a given path to logical store name.
     *
     * @param path node path for which backing store location is to be determined
     * @return mountInfo for the given path. If no explicit mount configured then
     * default mount would be returned
     */
    @NotNull
    Mount getMountByPath(String path);

    /**
     * Set of non default mount points configured for the setup
     * 
     * @return a collection of mounts, possibly empty
     */
    @NotNull
    Collection<Mount> getNonDefaultMounts();

    /**
     * Returns the mount instance for given mount name
     *
     * @param name name of the mount
     * @return mount instance for given mount name. If no mount exists for given name
     * {@code null} would be returned
     */
    @Nullable
    Mount getMountByName(String name);

    /**
     * Return true if there are explicit mounts configured
     * 
     * @return true if at least one non-default mount is configured, false otherwise
     */
    boolean hasNonDefaultMounts();

    /**
     * Returns all mounts placed under the specified path
     *
     * @param path the path under which mounts are to be found
     * @return a collection of mounts, possibly empty
     * 
     * @see Mount#isUnder(String)
     */
    @NotNull
    Collection<Mount> getMountsPlacedUnder(String path);

    /**
     * Returns all mounts placed directly under the specified path
     * 
     * @param path the path under which mounts are to be foud
     * @return a collection of mounts, possibly empty
     * 
     * @see Mount#isDirectlyUnder(String)
     */
    @NotNull
    Collection<Mount> getMountsPlacedDirectlyUnder(String path);

    /**
     * Returns the default mount
     * 
     * @return the default mount
     */
    @NotNull
    Mount getDefaultMount();
}
