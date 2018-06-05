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

package org.apache.jackrabbit.oak.plugins.value.jcr;

import java.net.URL;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.blob.URLWritableBlob;
import org.apache.jackrabbit.oak.api.binary.URLWritableBinary;

public class URLWritableBinaryImpl extends BinaryImpl implements URLWritableBinary {

    private final URLWritableBlob urlWritableBlob;

    URLWritableBinaryImpl(ValueImpl value, URLWritableBlob urlWritableBlob) {
        super(value);
        this.urlWritableBlob = urlWritableBlob;
    }

    @Override
    public URL getWriteURL() throws RepositoryException {
        return urlWritableBlob.getWriteURL();
    }

}
