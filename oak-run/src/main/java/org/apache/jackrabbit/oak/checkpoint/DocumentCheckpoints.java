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

package org.apache.jackrabbit.oak.checkpoint;

import static org.apache.jackrabbit.oak.plugins.document.CheckpointsHelper.getCheckpoints;
import static org.apache.jackrabbit.oak.plugins.document.CheckpointsHelper.min;
import static org.apache.jackrabbit.oak.plugins.document.CheckpointsHelper.removeOlderThan;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.document.CheckpointsHelper;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Revision;

class DocumentCheckpoints extends Checkpoints {

    private final DocumentNodeStore store;

    DocumentCheckpoints(DocumentNodeStore store) {
        this.store = store;
    }

    @Override
    public List<CP> list() {
        List<CP> list = Lists.newArrayList();
        for (Map.Entry<Revision, Long> entry : getCheckpoints(store).entrySet()) {
            list.add(new CP(entry.getKey().toString(),
                    entry.getKey().getTimestamp(),
                    entry.getValue()));
        }
        return list;
    }

    @Override
    public long removeAll() {
        return CheckpointsHelper.removeAll(store);
    }

    @Override
    public long removeUnreferenced() {
        Revision ref = min(getReferencedCheckpoints(store.getRoot()));
        if (ref == null) {
            return -1;
        }
        return removeOlderThan(store, ref);
    }

    @Override
    public int remove(String cp) {
        Revision r;
        try {
            r = Revision.fromString(cp);
        } catch (IllegalArgumentException e) {
            return 0;
        }
        return CheckpointsHelper.remove(store, r);
    }

    @Override
    public Map<String, String> getInfo(String cp) {
        return store.checkpointInfo(cp);
    }

    @Override
    public int setInfoProperty(String cp, String name, String value) {
        return CheckpointsHelper.setInfoProperty(store, cp, name, value);
    }

}
