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
package org.apache.jackrabbit.oak.util;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.util.Text;

/**
 * LocationUtil... FIXME: workaround for OAK-426
 */
public final class LocationUtil {

    private LocationUtil() {
    }

    @Nonnull
    public static TreeLocation getTreeLocation(TreeLocation parentLocation, String relativePath) {
        TreeLocation targetLocation = parentLocation;
        String[] segments = Text.explode(relativePath, '/', false);
        for (int i = 0; i < segments.length && targetLocation != TreeLocation.NULL; i++) {
            String segment = segments[i];
            if (PathUtils.denotesCurrent(segment)) {
                continue;
            } else if (PathUtils.denotesParent(segment)) {
                targetLocation = targetLocation.getParent();
            } else {
                targetLocation = targetLocation.getChild(segment);
            }
        }
        return targetLocation;
    }
}
