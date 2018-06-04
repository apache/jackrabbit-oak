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
package org.apache.jackrabbit.oak.jcr.lock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionImpl;
import org.apache.jackrabbit.oak.jcr.xml.ImporterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support deprecation of JCR locking as per OAK-6421.
 */
public class LockDeprecation {

    private static final Logger LOG = LoggerFactory.getLogger(LockDeprecation.class);

    private static final String LOCKSUPPORT = System.getProperty("oak.locksupport", "deprecated");
    private static final boolean ISLOCKINGSUPPORTED = !("disabled".equals(LOCKSUPPORT));

    private static final String MAYBEOK = "(invocation might be ok in order to detect locking support)";

    private LockDeprecation() {
    }

    private static final int LOGDEPTH = 5;

    private static AtomicBoolean NOTWARNEDYET = new AtomicBoolean(true);

    // classes that we don't want to include in the stack trace elements in the
    // log message
    private static final Set<String> IGNOREDCLASSES = new HashSet<String>(Arrays.asList(
            new String[] { Thread.class.getName(), java.lang.reflect.Method.class.getName(), LockDeprecation.class.getName(),
                    LockManagerImpl.class.getName(), NodeImpl.class.getName(), SessionImpl.class.getName() }));

    // classes for which we tolerate locking related calls for now (only TRACE
    // level, no WARNING)
    private static final Set<String> ACCEPTEDCLASSES = new HashSet<String>(
            Arrays.asList(new String[] { ImporterImpl.class.getName() }));

    public static void handleCall(String operation) throws UnsupportedRepositoryOperationException {

        if (!ISLOCKINGSUPPORTED) {
            throw new UnsupportedRepositoryOperationException(
                    "Support for JCR Locking is disabled (see OAK-6421 for further information)");
        } else {
            boolean firstInvocation = NOTWARNEDYET.getAndSet(false);

            String message = createLogMessage(operation);
            if (firstInvocation && !message.endsWith(MAYBEOK)) {
                String explanation = "Support for JCR Locking is deprecated and will be disabled in a future version of Jackrabbit Oak (see OAK-6421 for further information)";
                LOG.warn(explanation + " - " + message);
            } else if (LOG.isTraceEnabled()) {
                LOG.trace(message);
            }
        }
    }

    public final static boolean isLockingSupported() {
        return ISLOCKINGSUPPORTED;
    }

    private static String createLogMessage(String operation) {
        boolean whitelistThisCall = false;
        StackTraceElement elements[] = Thread.currentThread().getStackTrace();
        StringBuilder b = new StringBuilder();
        b.append("operation '" + operation + "' called from:");
        int depth = 0;
        for (StackTraceElement e : elements) {
            if (depth < LOGDEPTH) {
                String cn = e.getClassName();
                whitelistThisCall |= ACCEPTEDCLASSES.contains(cn);
                if (!IGNOREDCLASSES.contains(cn) && !cn.startsWith("sun.reflect")) {
                    b.append(" ").append(e.toString());
                    depth += 1;
                }
            }
        }
        if (whitelistThisCall) {
            b.append(' ').append(MAYBEOK);
        }
        return b.toString();
    }
}
