package org.apache.jackrabbit.oak.api;

import org.apache.jackrabbit.oak.query.CoreValue;

/**
 * A query result row.
 */
public interface ResultRow {

    String getPath();

    String getPath(String selectorName);

    CoreValue getValue(String columnName);

    CoreValue[] getValues();

}
