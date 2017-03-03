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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;

public class FileStoreHelper {

    public static File isValidFileStoreOrFail(File store) {
        checkArgument(isValidFileStore(store), "Invalid FileStore directory "
                + store);
        return store;
    }
    
    /**
     * Checks if the provided directory is a valid FileStore
     *
     * @return true if the provided directory is a valid FileStore
     */
    public static boolean isValidFileStore(File store) {
        if (!store.exists()) {
            return false;
        }
        if (!store.isDirectory()) {
            return false;
        }
        // for now the only check is the existence of the journal file
        for (String f : store.list()) {
            if ("journal.log".equals(f)) {
                return true;
            }
        }
        return false;
    }
}