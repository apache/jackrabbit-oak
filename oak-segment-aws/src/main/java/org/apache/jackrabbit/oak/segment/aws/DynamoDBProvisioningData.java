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
package org.apache.jackrabbit.oak.segment.aws;

import com.amazonaws.services.dynamodbv2.model.BillingMode;

public class DynamoDBProvisioningData {

    public static final DynamoDBProvisioningData DEFAULT = new DynamoDBProvisioningData(BillingMode.PAY_PER_REQUEST);

    private BillingMode billingMode;
    private Long journalTableProvisionedWcu;
    private Long journalTableProvisionedRcu;
    private Long lockTableProvisionedWcu;
    private Long lockTableProvisionedRcu;

    private DynamoDBProvisioningData(BillingMode billingMode) {
        this.billingMode = billingMode;
    }

    public DynamoDBProvisioningData(BillingMode billingMode, Long journalTableProvisionedWcu, Long journalTableProvisionedRcu, Long lockTableProvisionedWcu, Long lockTableProvisionedRcu) {
        this(billingMode);
        this.journalTableProvisionedWcu = journalTableProvisionedWcu;
        this.journalTableProvisionedRcu = journalTableProvisionedRcu;
        this.lockTableProvisionedWcu = lockTableProvisionedWcu;
        this.lockTableProvisionedRcu = lockTableProvisionedRcu;
    }

    public BillingMode getBillingMode() {
        return billingMode;
    }

    public Long getJournalTableProvisionedWcu() {
        return journalTableProvisionedWcu;
    }

    public Long getJournalTableProvisionedRcu() {
        return journalTableProvisionedRcu;
    }

    public Long getLockTableProvisionedWcu() {
        return lockTableProvisionedWcu;
    }

    public Long getLockTableProvisionedRcu() {
        return lockTableProvisionedRcu;
    }
}
