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

package org.apache.jackrabbit.oak.segment.file.cancel;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

class ConditionCanceller extends Canceller {

    private final Canceller parent;

    private final String reason;

    private final BooleanSupplier condition;

    ConditionCanceller(Canceller parent, String reason, BooleanSupplier condition) {
        this.parent = parent;
        this.reason = reason;
        this.condition = condition;
    }

    @Override
    public Cancellation check() {
        if (condition.getAsBoolean()) {
            return new Cancellation(true, reason);
        }
        return parent.check();
    }

}
