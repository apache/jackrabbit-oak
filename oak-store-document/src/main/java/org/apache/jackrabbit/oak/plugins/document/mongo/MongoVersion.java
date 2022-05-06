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
package org.apache.jackrabbit.oak.plugins.document.mongo;


import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Util Class to represent mongo version
 *
 * @author daim
 */
public final class MongoVersion implements Comparable<MongoVersion>{
    final int majorVersion;
    final int minorVersion;
    final int patchVersion;


    private MongoVersion(@NotNull final String version) {

        final ImmutableTriple<Integer, Integer, Integer> versions = extractVersions(version);

        this.majorVersion = versions.left;
        this.minorVersion = versions.middle;
        this.patchVersion = versions.right;
    }

    private ImmutableTriple<Integer, Integer, Integer> extractVersions(String version) {

        final String[] verArr = version.split("\\.");
        if (verArr.length >= 3)
            return ImmutableTriple.of(Integer.valueOf(verArr[0]), Integer.valueOf(verArr[1]), Integer.valueOf(verArr[2]));
        else
            return ImmutableTriple.of(Integer.valueOf(verArr[0]), Integer.valueOf(verArr[1]), 0);

    }

    static final MongoVersion MONGO_4_0_0 = new MongoVersion("4.0.0");

    /**
     * static factory to create mongo version
     * @param version curent version of mongo db
     * @return if version is null/malformed then MONGO_4_0 (default) else version as per given param
     */
    public static MongoVersion of(String version) {
        return isVersionMalformed(version) ? MONGO_4_0_0 : new MongoVersion(version);
    }

    private static boolean isVersionMalformed(String version) {
        return version == null || !version.contains(".");
    }

    @Override
    public int compareTo(@NotNull MongoVersion o) {
         final int x = Integer.compare(this.majorVersion, o.majorVersion);
         final int y = Integer.compare(this.minorVersion, o.minorVersion);
         return x == 0 ? y == 0 ? Integer.compare(this.patchVersion, o.patchVersion) : y : x;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MongoVersion version = (MongoVersion) o;
        return majorVersion == version.majorVersion && minorVersion == version.minorVersion && patchVersion == version.patchVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(majorVersion, minorVersion, patchVersion);
    }
}
