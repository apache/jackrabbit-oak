package org.apache.jackrabbit.oak.spi.blob;

public interface CompositeDataStoreAware {
    boolean isDelegate();
    void setIsDelegate(boolean isDelegate);
}
