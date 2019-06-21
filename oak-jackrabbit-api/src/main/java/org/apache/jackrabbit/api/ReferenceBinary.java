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
package org.apache.jackrabbit.api;

import javax.jcr.Binary;

/**
 * Referenceable binary. In addition to the normal JCR {@link Binary}
 * functionality, implementations of this class contain a <em>secure
 * reference</em> to the storage location of the binary stream. This
 * reference can be used to efficiently copy binaries across servers as
 * long as both the source and target servers use the same underlying
 * storage for binaries.
 */
public interface ReferenceBinary extends Binary {

    /**
     * Returns a secure reference to this binary, or {@code null} if such
     * a reference is not available.
     *
     * @return binary reference, or {@code null}
     */
    String getReference();

}
