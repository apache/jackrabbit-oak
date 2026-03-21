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
 * 
 * The class holds the result of asynchronous upload to {@link Backend}
 */
public class AsyncUploadResult {
    /**
     * {@link DataIdentifier} on which asynchronous upload is initiated.
     */
    private final DataIdentifier identifier;
    
    /**
     * {@link File} which is asynchronously uploaded.
     */
    private final File file;
    
    /**
     * Any {@link Exception} which is raised in asynchronously upload.
     */
    private Exception exception;
    
    public AsyncUploadResult(DataIdentifier identifier, File file) {
        super();
        this.identifier = identifier;
        this.file = file;
    }

    public DataIdentifier getIdentifier() {
        return identifier;
    }

    public File getFile() {
        return file;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}
