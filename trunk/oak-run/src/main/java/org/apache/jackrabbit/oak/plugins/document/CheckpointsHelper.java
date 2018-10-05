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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.google.common.collect.Maps;

import static org.apache.jackrabbit.oak.plugins.document.Checkpoints.Info;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

/**
 * Helper class to access package private functionality.
 */
public abstract class CheckpointsHelper {

    public static SortedMap<Revision, Long> getCheckpoints(
            DocumentNodeStore store) {
        SortedMap<Revision, Info> checkpoints = store.getCheckpoints().getCheckpoints();
        SortedMap<Revision, Long> map = Maps.newTreeMap(checkpoints.comparator());
        for (Map.Entry<Revision, Info> entry : checkpoints.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getExpiryTime());
        }
        return map;
    }

    public static long removeAll(DocumentNodeStore store) {
        long cnt = 0;
        for (Revision r : getCheckpoints(store).keySet()) {
            store.getCheckpoints().release(r.toString());
            cnt++;
        }
        return cnt;
    }

    public static long removeOlderThan(DocumentNodeStore store, Revision r) {
        long cnt = 0;
        for (Revision cp : getCheckpoints(store).keySet()) {
            if (cp.getTimestamp() < r.getTimestamp()) {
                store.getCheckpoints().release(cp.toString());
                cnt++;
            }
        }
        return cnt;
    }

    public static int remove(DocumentNodeStore store, Revision r) {
        if (getCheckpoints(store).containsKey(r)) {
            store.getCheckpoints().release(r.toString());
            return 1;
        } else {
            return 0;
        }
    }

    public static Revision min(Set<String> revs) {
        if (revs == null || revs.isEmpty()) {
            return null;
        }
        Revision r = null;
        for (String cp : revs) {
            r = Utils.min(r, Revision.fromString(cp));
        }
        return r;
    }

    public static int setInfoProperty(DocumentNodeStore store, String rev, String key, String value) {
        try {
            store.getCheckpoints().setInfoProperty(rev, key, value);
        } catch (IllegalArgumentException e) {
            return 0;
        }
        return 1;
    }
}
