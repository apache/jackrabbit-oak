/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.http.handler;

/**
 * A collection of handlers used to respond to some requests handled by the
 * remote servlet.
 */
public class Handlers {

    private Handlers() {
    }

    /**
     * Create an handler that will return the last revision available to the
     * server.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createGetLastRevisionHandler() {
        return withAuthentication(new GetLastRevisionHandler());
    }

    /**
     * Create an handler that will return a repository sub-tree at a specific
     * revision.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createGetRevisionTreeHandler() {
        return withAuthentication(new GetRevisionTreeHandler());
    }

    /**
     * Create an handler that will return a repository sub-tree at the latest
     * known state.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createGetLastTreeHandler() {
        return withAuthentication(new GetLastTreeHandler());
    }

    /**
     * Create an handler that will return a 404 response to the client.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createNotFoundHandler() {
        return new NotFoundHandler();
    }

    /**
     * Create a handler that will read a binary object from the repository.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createGetBinaryHandler() {
        return withAuthentication(new GetBinaryHandler());
    }

    /**
     * Create a handler that will check if a binary exists
     *
     * @return An instance of {@code Handler}
     */
    public static Handler createHeadBinaryHandler() {
        return withAuthentication(new HeadBinaryHandler());
    }

    /**
     * Create a handler that will perform the creation of new binary object.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createPostBinaryHandler() {
        return withAuthentication(new PostBinaryHandler());
    }

    /**
     * Create a handler that will patch the content at a specific revision.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createPatchSpecificRevisionHandler() {
        return withAuthentication(new PatchSpecificRevisionHandler());
    }

    /**
     * Create a handler that will patch the content at the last revision.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createPatchLastRevisionHandler() {
        return withAuthentication(new PatchLastRevisionHandler());
    }

    /**
     * Create a handler that checks if a tree exists at the last revision.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createHeadLastTreeHandler() {
        return withAuthentication(new HeadLastTreeHandler());
    }

    /**
     * Create a handler that checks if a tree exists at a given revision.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createHeadRevisionTreeHandler() {
        return withAuthentication(new HeadRevisionTreeHandler());
    }

    /**
     * Create a handler that searches for content at the last revision.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createSearchLastRevisionHandler() {
        return withAuthentication(new SearchLastRevisionHandler());
    }

    /**
     * Create a handler that searches for content at the provided revision.
     *
     * @return An instance of {@code Handler}.
     */
    public static Handler createSearchSpecificRevisionHandler() {
        return withAuthentication(new SearchSpecificRevisionHandler());
    }

    private static Handler withAuthentication(Handler authenticated) {
        return new AuthenticationWrapperHandler(authenticated, createForbiddenHandler());
    }

    private static Handler createForbiddenHandler() {
        return new UnauthorizedHandler();
    }

}
