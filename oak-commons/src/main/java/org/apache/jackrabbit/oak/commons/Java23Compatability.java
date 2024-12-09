package org.apache.jackrabbit.oak.commons;

import javax.security.auth.Subject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

public class Java23Compatability {

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
                result = (T) callAs.invoke(null, subject, action);
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
                result = (T) callAs.invoke(null, subject, action);
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
                result = (T) callAs.invoke(null, subject, action);
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
                result = (T) callAs.invoke(null, subject, action);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new SecurityException(e);
            }
        } else {
            result = Subject.doAsPrivileged(subject, action, acc);
        }
        return result;
    }
}
