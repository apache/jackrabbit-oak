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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import org.apache.jackrabbit.commons.json.JsonUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

final class ResultMessages {
    
    private final List<String> messages = new ArrayList<>();

    ResultMessages() {}

    String[] getMessages() {
        return messages.toArray(new String[0]);
    }

    void append(@NotNull List<SyncResult> results) {
        for (SyncResult result : results) {
            append(result);
        }
    }

    void append(@NotNull List<SyncResult> results, @NotNull Exception e) {
        for (SyncResult result : results) {
            if (result instanceof ErrorSyncResult) {
                append(result.getIdentity(), ((ErrorSyncResult) result).getException());
            } else {
                SyncResult.Status st = result.getStatus();
                switch (st) {
                    case ADD:
                    case DELETE:
                    case UPDATE:
                    case ENABLE:
                    case DISABLE:
                        append(result.getIdentity(), e);
                        break;
                    default:
                        append(result);
                }
            }
        }
    }

    private void append(@NotNull SyncResult r) {
        if (r instanceof ErrorSyncResult) {
            append(r.getIdentity(), ((ErrorSyncResult) r).getException());
        } else {
            append(r.getIdentity(), getOperationFromStatus(r.getStatus()), null);
        }
    }

    private void append(@Nullable SyncedIdentity syncedIdentity, @NotNull Exception e) {
        append(syncedIdentity, "ERR", e.toString());
    }

    private void append(@Nullable SyncedIdentity syncedIdentity, @NotNull String op, @Nullable String msg) {
        String uid = JsonUtil.getJsonString((syncedIdentity == null ? null : syncedIdentity.getId()));
        ExternalIdentityRef externalIdentityRef = (syncedIdentity == null) ? null : syncedIdentity.getExternalIdRef();
        String eid = (externalIdentityRef == null) ? "\"\"" : JsonUtil.getJsonString(externalIdentityRef.getString());

        if (msg == null) {
            messages.add(String.format("{op:\"%s\",uid:%s,eid:%s}", op, uid, eid));
        } else {
            messages.add(String.format("{op:\"%s\",uid:%s,eid:%s,msg:%s}", op, uid, eid, JsonUtil.getJsonString(msg)));
        }
    }

    private static String getOperationFromStatus(@NotNull SyncResult.Status syncStatus) {
        String op;
        switch (syncStatus) {
            case ADD:
                op = "add";
                break;
            case UPDATE:
                op = "upd";
                break;
            case DELETE:
                op = "del";
                break;
            case ENABLE:
                op = "ena";
                break;
            case DISABLE:
                op = "dis";
                break;
            case NO_SUCH_AUTHORIZABLE:
                op = "nsa";
                break;
            case NO_SUCH_IDENTITY:
                op = "nsi";
                break;
            case MISSING:
                op = "mis";
                break;
            case FOREIGN:
                op = "for";
                break;
            default:
                // Status.NOP
                op = "nop";
        }
        return op;
    }
}