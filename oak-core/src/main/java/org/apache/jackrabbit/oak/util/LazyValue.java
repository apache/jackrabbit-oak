package org.apache.jackrabbit.oak.util;

/**
 * An instances of this class represents a lazy value of type {@code T}.
 * {@code LazyValue} implements an evaluate by need semantics:
 * {@link #createValue()} is called exactly once when {@link #get()}
 * is called for the first time.
 * <p>
 * {@code LazyValue} instances are thread safe.
 */
public abstract class LazyValue<T> {
    private volatile T value;

    /**
     * Factory method called to create the value on an as need basis.
     * @return a new instance for {@code T}.
     */
    protected abstract T createValue();

    /**
     * Get value. Calls {@link #createValue()} if called for the first time.
     * @return  the value
     */
    public T get () {
        // Double checked locking is fine since Java 5 as long as value is volatile.
        // See http://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html
        if (value == null) {
            synchronized (this) {
                if (value == null) {
                    value = createValue();
                }
            }
        }
        return value;
    }

}
