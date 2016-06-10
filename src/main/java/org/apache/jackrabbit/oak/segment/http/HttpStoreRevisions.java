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

package org.apache.jackrabbit.oak.segment.http;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URLConnection;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;

/**
 * This {@code Revisions} implementation delegates via HTTP
 * to its remote counterpart. It does not support setting
 * the id of the head state.
 */
public class HttpStoreRevisions implements Revisions {

    @Nonnull
    private final HttpStore store;

    public HttpStoreRevisions(@Nonnull HttpStore store) {
        this.store = store;
    }

    @Nonnull
    @Override
    public RecordId getHead() {
        try {
            URLConnection connection = store.get(null);
            try (
                InputStream stream = connection.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
            ) {
                return RecordId.fromString(store, reader.readLine());
            }
        } catch (IllegalArgumentException | MalformedURLException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Not supported: throws {@code UnsupportedOperationException}
     * @throws UnsupportedOperationException, always
     */
    @Override
    public boolean setHead(
            @Nonnull RecordId expected, @Nonnull RecordId head,
            @Nonnull Option... options) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not supported: throws {@code UnsupportedOperationException}
     * @throws UnsupportedOperationException, always
     */
    @Override
    public boolean setHead(
            @Nonnull Function<RecordId, RecordId> newHead,
            @Nonnull Option... options) throws InterruptedException {
        throw new UnsupportedOperationException();
    }
}
