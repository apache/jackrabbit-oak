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
package org.apache.jackrabbit.oak.plugins.segment.file;

import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryJournal;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class FileJournal extends MemoryJournal {

    private final FileStore store;

    FileJournal(FileStore store, NodeState root) {
        super(store, root);
        this.store = store;
    }

    FileJournal(FileStore store, String parent) {
        super(store, parent);
        this.store = store;
    }

    @Override
    public synchronized boolean setHead(RecordId base, RecordId head) {
        if (super.setHead(base, head)) {
            store.writeJournals();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void merge() {
        super.merge();
        store.writeJournals();
    }
}