/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.authorization.permission;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.util.Text;

/**
 * PermissionUtil... TODO
 */
public final class PermissionUtil {

    private PermissionUtil() {}

    @CheckForNull
    public static String getParentPathOrNull(@Nonnull String path) {
        return (PathUtils.denotesRoot(path) || path.isEmpty()) ? null : Text.getRelativeParent(path, 1);
    }

    @Nonnull
    public static String getEntryName(@Nullable String accessControlledPath) {
        String path = Strings.nullToEmpty(accessControlledPath);
        return String.valueOf(path.hashCode());
    }
}