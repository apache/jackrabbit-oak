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
package org.apache.jackrabbit.oak.plugins.document.util;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Set;

public class ReadOnlyDocumentStoreWrapperFactory {
    private static final Set<String> unsupportedMethods = Sets.newHashSet(
            "remove", "create", "update", "createOrUpdate", "findAndUpdate");
    public static DocumentStore getInstance(@Nonnull final DocumentStore delegate) {
        return (DocumentStore)Proxy.newProxyInstance(DocumentStore.class.getClassLoader(),
                new Class[]{DocumentStore.class},
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if (unsupportedMethods.contains(method.getName())) {
                            throw new UnsupportedOperationException(String.format("Method - %s. Params: %s",
                                    method.getName(), Arrays.toString(args)));
                        }
                        return method.invoke(delegate, args);
                    }
                });
    }
}
