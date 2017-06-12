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

package org.apache.jackrabbit.oak.run.cli;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

class ReadOnlyBlobStoreWrapper {

    public static BlobStore wrap(BlobStore delegate){
        Class[] interfaces = delegate.getClass().getInterfaces();
        return (BlobStore) Proxy.newProxyInstance(ReadOnlyBlobStoreWrapper.class.getClassLoader(),
                interfaces,
                new ReadOnlyHandler(delegate));
    }

    private static class ReadOnlyHandler implements InvocationHandler {
        private final Set<String> writableMethods = ImmutableSet.of(
                "writeBlob",        //BlobStore
                "deleteChunks",     //GarbageCollectableBlobStore
                "countDeleteChunks" //GarbageCollectableBlobStore
        );
        private final BlobStore delegate;

        ReadOnlyHandler(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            if (writableMethods.contains(methodName)) {
                throw new UnsupportedOperationException("Readonly BlobStore - Cannot invoked " + method);
            }
            return method.invoke(delegate, args);
        }
    }
}
