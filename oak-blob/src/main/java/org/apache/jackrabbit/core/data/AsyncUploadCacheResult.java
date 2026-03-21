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
package org.apache.jackrabbit.core.data;

import java.io.File;

/**
 * This class holds result of asynchronous upload from {@link AsyncUploadCache}
 */
public class AsyncUploadCacheResult {

    /**
     * flag to indicate that asynchronous upload can be started on file.
     */
    private boolean asyncUpload;

    /**
     * flag to indicate that cached file requires to be deleted. It is
     * applicable in case where file marked for delete before asynchronous
     * upload completes.
     */
    private boolean requiresDelete;

    private File file;

    /**
     * Flag to denote that asynchronous upload can be started on file.
     */
    public boolean canAsyncUpload() {
        return asyncUpload;
    }

    public void setAsyncUpload(boolean asyncUpload) {
        this.asyncUpload = asyncUpload;
    }

    /**
     * Flag to indicate that record to be deleted from {@link DataStore}.
     */
    public boolean doRequiresDelete() {
        return requiresDelete;
    }

    public void setRequiresDelete(boolean requiresDelete) {
        this.requiresDelete = requiresDelete;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

}
