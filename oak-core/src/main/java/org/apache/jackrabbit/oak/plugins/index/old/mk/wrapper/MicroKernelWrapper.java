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
package org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;

/**
 * This interface allows a MicroKernel client to use a JsopReader instead of
 * having to use strings.
 */
public interface MicroKernelWrapper extends MicroKernel {

    JsopReader getRevisionsStream(long since, int maxEntries, String path) throws MicroKernelException;

    JsopReader getJournalStream(String fromRevisionId, String toRevisionId, String path) throws MicroKernelException;

    JsopReader getNodesStream(String path, String revisionId, int depth, long offset, int count, String filter) throws MicroKernelException;

    String commitStream(String path, JsopReader jsonDiff, String revisionId, String message) throws MicroKernelException;

    JsopReader diffStream(String fromRevisionId, String toRevisionId, String path, int depth);

}
