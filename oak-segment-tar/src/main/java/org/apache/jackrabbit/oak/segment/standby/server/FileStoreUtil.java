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

package org.apache.jackrabbit.oak.segment.standby.server;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileStoreUtil {

    private static final Logger log = LoggerFactory.getLogger(FileStoreUtil.class);
    private static final long DEFAULT_SLEEP_TIME = 125L;

    private FileStoreUtil() {
        // Prevent instantiation
    }

    public static int roundDiv(long x, int y) {
        return (int) Math.ceil((double) x / (double) y);
    }
    
    static RecordId readPersistedHeadWithRetry(FileStore store, long timeout) {
        Supplier<RecordId> headSupplier = () -> {
            return store.getRevisions().getPersistedHead();
        };

        if (timeout > DEFAULT_SLEEP_TIME) {
            return readWithRetry(headSupplier, "persisted head", timeout);
        } else {
            return headSupplier.get();
        }
    }
    
    private static <T> T readWithRetry(Supplier<T> supplier, String supplied, long timeout) {
        for (int i = 0; i < timeout / DEFAULT_SLEEP_TIME; i++) {
            if (supplier.get() != null) {
                return supplier.get();
            }
            
            try {
                log.trace("Unable to read {}, waiting...", supplied);
                TimeUnit.MILLISECONDS.sleep(DEFAULT_SLEEP_TIME);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }
}
