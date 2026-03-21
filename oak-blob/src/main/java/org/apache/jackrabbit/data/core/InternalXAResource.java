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
package org.apache.jackrabbit.data.core;



/**
 * Interface implemented by resources that provide XA functionality.
 */
public interface InternalXAResource {

    /**
     * Associate this resource with a transaction. All further operations on
     * the object should be interpreted as part of this transaction and changes
     * recorded in some attribute of the transaction context.
     * @param tx transaction context, if <code>null</code> disassociate
     */
    void associate(TransactionContext tx);

    /**
     * Invoked before one of the {@link #prepare}, {@link #commit} or
     * {@link #rollback} method is called.
     * @param tx transaction context
     */
    void beforeOperation(TransactionContext tx);

    /**
     * Prepare transaction. The transaction is identified by a transaction
     * context.
     * @param tx transaction context
     * @throws TransactionException if an error occurs
     */
    void prepare(TransactionContext tx) throws TransactionException;

    /**
     * Commit transaction. The transaction is identified by a transaction
     * context. If the method throws, other resources get their changes
     * rolled back.
     * @param tx transaction context
     * @throws TransactionException if an error occurs
     */
    void commit(TransactionContext tx) throws TransactionException;

    /**
     * Rollback transaction. The transaction is identified by a transaction
     * context.
     * @param tx transaction context.
     */
    void rollback(TransactionContext tx) throws TransactionException;

    /**
     * Invoked after one of the {@link #prepare}, {@link #commit} or
     * {@link #rollback} method has been called.
     * @param tx transaction context
     */
    void afterOperation(TransactionContext tx);

}
