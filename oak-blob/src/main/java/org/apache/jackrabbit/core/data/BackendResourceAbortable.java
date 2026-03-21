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

import java.io.InputStream;

/**
 * {@link Backend} resource abstraction, such as {@link InputStream}, which can be aborted without consuming it
 * fully for efficiency.
 * <p>
 * Some {@link Backend} implementations such as <code>S3Backend</code> may return an abortable <code>InputStream</code>
 * for a more optimal resource use. <code>S3Backend</code> internally uses Apache HttpClient library which tries
 * to reuse HTTP connections by reading data fully to the end of an attached <code>InputStream</code> on {@link InputStream#close()}
 * by default. It can be efficient from a socket pool management perspective, but possibly a significant overhead
 * while bytes are read from S3 just to be discarded. So, a {@link Backend} implementation that retrieves an abortable
 * resource may decide to wrap the underlying resource (e.g, <code>InputStream</code>) by this interface (e.g,
 * <code>S3BackendResourceAbortableInputStream</code>) in order to abort the underlying resources (e.g, http request
 * object) without having to read data fully.
 */
public interface BackendResourceAbortable {

    /**
     * Abort the underlying backend resource(s).
     */
    public void abort();

}
