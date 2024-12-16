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
package org.apache.jackrabbit.oak.commons.jdkcompat;

import javax.security.auth.Subject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

/**
 * This class contains methods replacing the deprecated
 * {@link Subject#getSubject(AccessControlContext)} 
 * and associated methods, which changed their behavior
 * with Java 23 (@see https://inside.java/2024/07/08/quality-heads-up).
 */
public class Java23Subject {

    static Method current, callAs;

    static {
        try {
            current = Subject.class.getMethod("current");
            callAs = Subject.class.getMethod("callAs", Subject.class, Callable.class);
        } catch (NoSuchMethodException ignored) {}
    }

    public static Subject getSubject() {
        Subject result;
        if (current != null) {
            try {
                result = (Subject) current.invoke(null);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new SecurityException(e);
            }
        } else {
            result = Subject.getSubject(AccessController.getContext());
        }
        return result;
    }

    public static <T> T doAs(Subject subject, PrivilegedAction<T> action) {
        T result;
        if (callAs != null) {
            try {
                result = (T) callAs.invoke(null, subject, (Callable<T>) () -> action.run());
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new SecurityException(e);
            }
        } else {
            result = Subject.doAs(subject, action);
        }
        return result;
    }

    public static <T> T doAsPrivileged(Subject subject, PrivilegedAction<T> action, AccessControlContext acc) {
        T result;
        if (callAs != null) {
            try {
                result = (T) callAs.invoke(null, subject, (Callable<T>) () -> action.run());
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new SecurityException(e);
            }
        } else {
            result = Subject.doAsPrivileged(subject, action, acc);
        }
        return result;
    }

    public static <T> T doAs(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        T result;
        if (callAs != null) {
            try {
                result = (T) callAs.invoke(null, subject, (Callable<T>) () -> action.run());
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new SecurityException(e);
            }
        } else {
            result = Subject.doAs(subject, action);
        }
        return result;
    }

    public static <T> T doAsPrivileged(Subject subject, PrivilegedExceptionAction<T> action, AccessControlContext acc) throws PrivilegedActionException {
        T result;
        if (callAs != null) {
            try {
                result = (T) callAs.invoke(null, subject, (Callable<T>) () -> action.run());
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new SecurityException(e);
            }
        } else {
            result = Subject.doAsPrivileged(subject, action, acc);
        }
        return result;
    }
}
