package org.apache.jackrabbit.oak.util;

/**
 * Utility class containing type safe counterparts for some of the predicates of
 * commons-collections.
 */
public class Predicates {
    private static final Predicate<?> NOT_NULL = new Predicate<Object>() {
        @Override
        public boolean evaluate(Object arg) {
            return arg != null;
        }
    };

    private Predicates() {
    }

    @SuppressWarnings("unchecked")
    public static <T> Predicate<T> nonNull() {
        return (Predicate<T>) NOT_NULL;
    }
}
