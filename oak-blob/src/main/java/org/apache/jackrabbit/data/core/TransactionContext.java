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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the transaction on behalf of the component that wants to
 * explicitly demarcate transaction boundaries. After having been prepared,
 * schedules a task that rolls back the transaction if some time passes without
 * any further action. This will guarantee that global objects locked by one
 * of the resources' {@link InternalXAResource#prepare} method, are eventually
 * unlocked.
 */
public class TransactionContext {

    /**
     * Logger instance.
     */
    private static final Logger log = LoggerFactory.getLogger(TransactionContext.class);

    private static final int STATUS_PREPARING = 1;
    private static final int STATUS_PREPARED = 2;
    private static final int STATUS_COMMITTING = 3;
    private static final int STATUS_COMMITTED = 4;
    private static final int STATUS_ROLLING_BACK = 5;
    private static final int STATUS_ROLLED_BACK = 6;

    /**
     * The per thread associated Xid
     */
    private static final ThreadLocal<Xid> CURRENT_XID = new ThreadLocal<Xid>();

    /**
     * Transactional resources.
     */
    private final InternalXAResource[] resources;

    /**
    * The Xid
    */
   private final Xid xid;

    /**
     * Transaction attributes.
     */
    private final Map<String, Object> attributes = new HashMap<String, Object>();

    /**
     * Status.
     */
    private int status;

    /**
     * Flag indicating whether the association is currently suspended.
     */
    private boolean suspended;

    /**
     * Create a new instance of this class.
     *
     * @param xid associated xid
     * @param resources transactional resources
     */
    public TransactionContext(Xid xid, InternalXAResource[] resources) {
        this.xid = xid;
        this.resources = resources;
    }

    /**
     * Set an attribute on this transaction. If the value specified is
     * <code>null</code>, it is semantically equivalent to
     * {@link #removeAttribute}.
     *
     * @param name  attribute name
     * @param value attribute value
     */
    public void setAttribute(String name, Object value) {
        if (value == null) {
            removeAttribute(name);
        }
        attributes.put(name, value);
    }

    /**
     * Return an attribute value on this transaction.
     *
     * @param name attribute name
     * @return attribute value, <code>null</code> if no attribute with that
     *         name exists
     */
    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    /**
     * Remove an attribute on this transaction.
     *
     * @param name attribute name
     */
    public void removeAttribute(String name) {
        attributes.remove(name);
    }

    /**
     * Prepare the transaction identified by this context. Prepares changes on
     * all resources. If some resource reports an error on prepare,
     * automatically rollback changes on all other resources. Throw exception
     * at the end if errors were found.
     *
     * @throws XAException if an error occurs
     */
    public synchronized void prepare() throws XAException {
        bindCurrentXid();
        status = STATUS_PREPARING;
        beforeOperation();

        TransactionException txe = null;
        for (int i = 0; i < resources.length; i++) {
            try {
                resources[i].prepare(this);
            } catch (TransactionException e) {
                txe = e;
                break;
            } catch (Exception e) {
                txe = new TransactionException("Error while preparing resource " + resources, e);
                break;
            }
        }

        afterOperation();
        status = STATUS_PREPARED;

        if (txe != null) {
            // force immediate rollback on error.
            try {
                rollback();
            } catch (XAException e) {
                /* ignore */
            }
            XAException e = new XAException(XAException.XA_RBOTHER);
            e.initCause(txe);
            throw e;
        }
    }

    /**
     * Commit the transaction identified by this context. Commits changes on
     * all resources. If some resource reports an error on commit,
     * automatically rollback changes on all other resources. Throw
     * exception at the end if some commit failed.
     *
     * @throws XAException if an error occurs
     */
    public synchronized void commit() throws XAException {
        if (status == STATUS_ROLLED_BACK) {
            throw new XAException(XAException.XA_HEURRB);
        }

        boolean heuristicCommit = false;
        bindCurrentXid();
        status = STATUS_COMMITTING;
        beforeOperation();

        TransactionException txe = null;
        for (int i = 0; i < resources.length; i++) {
            InternalXAResource resource = resources[i];
            if (txe != null) {
                try {
                    resource.rollback(this);
                } catch (Exception e) {
                    log.warn("Unable to rollback changes on " + resource, e);
                }
            } else {
                try {
                    resource.commit(this);
                    heuristicCommit = true;
                } catch (TransactionException e) {
                    txe = e;
                } catch (Exception e) {
                    txe = new TransactionException("Error while committing resource " + resource, e);
                }
            }
        }
        afterOperation();
        status = STATUS_COMMITTED;

        cleanCurrentXid();

        if (txe != null) {
            XAException e = null;
            if (heuristicCommit) {
                e = new XAException(XAException.XA_HEURMIX);
            } else {
                e = new XAException(XAException.XA_HEURRB);
            }
            e.initCause(txe);
            throw e;
        }
    }

    /**
     * Rollback the transaction identified by this context. Rolls back changes
     * on all resources. Throws exception at the end if errors were found.
     * @throws XAException if an error occurs
     */
    public synchronized void rollback() throws XAException {
        if (status == STATUS_ROLLED_BACK) {
            throw new XAException(XAException.XA_RBOTHER);
        }
        bindCurrentXid();
        status = STATUS_ROLLING_BACK;
        beforeOperation();

        int errors = 0;
        for (int i = 0; i < resources.length; i++) {
            InternalXAResource resource = resources[i];
            try {
                resource.rollback(this);
            } catch (Exception e) {
                log.warn("Unable to rollback changes on " + resource, e);
                errors++;
            }
        }
        afterOperation();
        status = STATUS_ROLLED_BACK;

        cleanCurrentXid();

        if (errors != 0) {
            throw new XAException(XAException.XA_RBOTHER);
        }
    }

    /**
     * Invoke all of the registered resources' {@link InternalXAResource#beforeOperation}
     * methods.
     */
    private void beforeOperation() {
        for (int i = 0; i < resources.length; i++) {
            resources[i].beforeOperation(this);
        }
    }

    /**
     * Invoke all of the registered resources' {@link InternalXAResource#afterOperation}
     * methods.
     */
    private void afterOperation() {
        for (int i = 0; i < resources.length; i++) {
            resources[i].afterOperation(this);
        }
    }

    /**
     * Return a flag indicating whether the association is suspended.
     *
     * @return <code>true</code> if the association is suspended;
     *         <code>false</code> otherwise
     */
    public boolean isSuspended() {
        return suspended;
    }

    /**
     * Set a flag indicating whether the association is suspended.
     *
     * @param suspended flag whether that the association is suspended.
     */
    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    /**
     * Helper Method to bind the {@link Xid} associated with this {@link TransactionContext}
     * to the {@link #CURRENT_XID} ThreadLocal.
     */
    private void bindCurrentXid() {
        CURRENT_XID.set(xid);
    }

    /**
     * Helper Method to clean the {@link Xid} associated with this {@link TransactionContext}
     * from the {@link #CURRENT_XID} ThreadLocal.
     */
    private void cleanCurrentXid() {
        CURRENT_XID.set(null);
    }

    /**
     * Returns the {@link Xid} bind to the {@link #CURRENT_XID} ThreadLocal
     * @return current Xid or null
     */
    private static Xid getCurrentXid() {
        return CURRENT_XID.get();
    }

    /**
     * Returns the current thread identifier. The identifier is either the
     * current thread instance or the global transaction identifier wrapped 
     * in a {@link XidWrapper}, when running under a transaction.
     *
     * @return current thread identifier
     */
    public static Object getCurrentThreadId() {
        Xid xid = TransactionContext.getCurrentXid();
        if (xid != null) {
            return new XidWrapper(xid.getGlobalTransactionId());
        } else {
            return Thread.currentThread();
        }
    }

    /**
     * Compares the given thread identifiers for equality.
     *
     * @see #getCurrentThreadId()
     */
    public static boolean isSameThreadId(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a != null) {
        	return a.equals(b);
        } else {
            return false;
        }
    }
    
    /**
     * Wrapper around a global transaction id (byte[]) 
     * that handles hashCode and equals in a proper way.
     */
    private static class XidWrapper {
        
    	private static final char[] HEX = "0123456789abcdef".toCharArray();
        
    	private byte[] gtid;
    	
    	public XidWrapper(byte[] gtid) {
    		this.gtid = gtid;
    	}

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof XidWrapper)) {
                return false;
            }
            return Arrays.equals((byte[]) gtid, ((XidWrapper)other).gtid);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(gtid);
        }

        @Override
        public String toString() {
            return encodeHexString(gtid);
        }

        /**
         * Returns the hex encoding of the given bytes.
         *
         * @param value value to be encoded
         * @return encoded value
         */
        private static String encodeHexString(byte[] value) {
            char[] buffer = new char[value.length * 2];
            for (int i = 0; i < value.length; i++) {
                buffer[2 * i] = HEX[(value[i] >> 4) & 0x0f];
                buffer[2 * i + 1] = HEX[value[i] & 0x0f];
            }
            return new String(buffer);
        }

    }

}
