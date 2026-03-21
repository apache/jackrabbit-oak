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
package org.apache.jackrabbit.core.fs;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Extends the regular <code>java.io.OutputStream</code> with a random
 * access facility. Multiple <code>write()</code> operations can be
 * positioned off sequence with the {@link #seek} method.
 *
 * @deprecated this class should no longer be used
 */
@Deprecated
public abstract class RandomAccessOutputStream extends OutputStream {

    /**
     * Sets the current position in the resource where the next write
     * will occur.
     *
     * @param position the new position in the resource.
     * @throws IOException if an error occurs while seeking to the position.
     */
    public abstract void seek(long position) throws IOException;
}
