/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.api.blob;

import java.net.URL;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface URLWritableBlob extends Blob {

    @Nullable
    URL getWriteURL();

    /**
     * Called when the tree was committed and the blob is referenced at least once in a property.
     * Implementations must generated the write URL inside this method and start any expiry
     * time at this point.
     */
    void commit();
}
