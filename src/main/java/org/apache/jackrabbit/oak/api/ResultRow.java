package org.apache.jackrabbit.oak.api;

/**
 * A query result row.
 */
public interface ResultRow {

    String getPath();

    String getPath(String selectorName);

    CoreValue getValue(String columnName);

    CoreValue[] getValues();

}
