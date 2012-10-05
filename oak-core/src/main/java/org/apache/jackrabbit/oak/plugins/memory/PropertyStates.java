package org.apache.jackrabbit.oak.plugins.memory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;

public final class PropertyStates {
    private PropertyStates() {}

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> PropertyState createProperty(String name, T value, Type<T> type) {
        switch (type.tag()) {
            case PropertyType.STRING: return type.isArray()
                ? stringProperty(name, (Iterable<String>) value)
                : stringProperty(name, (String) value);
            case PropertyType.BINARY: return type.isArray()
                ? binaryPropertyFromBlob(name, (Iterable<Blob>) value)
                : binaryProperty(name, (Blob) value);
            case PropertyType.LONG: return type.isArray()
                ? longProperty(name, (Iterable<Long>) value)
                : longProperty(name, (Long) value);
            case PropertyType.DOUBLE: return type.isArray()
                ? doubleProperty(name, (Iterable<Double>) value)
                : doubleProperty(name, (Double) value);
            case PropertyType.DATE: return type.isArray()
                ? dateProperty(name, (Iterable<String>) value)
                : dateProperty(name, (String) value);
            case PropertyType.BOOLEAN: return type.isArray()
                ? booleanProperty(name, (Iterable<Boolean>) value)
                : booleanProperty(name, (Boolean) value);
            case PropertyType.NAME: return type.isArray()
                ? nameProperty(name, (Iterable<String>) value)
                : nameProperty(name, (String) value);
            case PropertyType.PATH: return type.isArray()
                ? pathProperty(name, (Iterable<String>) value)
                : pathProperty(name, (String) value);
            case PropertyType.REFERENCE: return type.isArray()
                ? referenceProperty(name, (Iterable<String>) value)
                : referenceProperty(name, (String) value);
            case PropertyType.WEAKREFERENCE: return type.isArray()
                ? weakreferenceProperty(name, (Iterable<String>) value)
                : weakreferenceProperty(name, (String) value);
            case PropertyType.URI: return type.isArray()
                ? uriProperty(name, (Iterable<String>) value)
                : uriProperty(name, (String) value);
            case PropertyType.DECIMAL: return type.isArray()
                ? decimalProperty(name, (Iterable<BigDecimal>) value)
                : decimalProperty(name, (BigDecimal) value);
            default: throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    @Nonnull
    public static <T> PropertyState createProperty(String name, T value) {
        if (value instanceof String) {
            return stringProperty(name, (String) value);
        }
        else if (value instanceof Blob) {
            return binaryProperty(name, (Blob) value);
        }
        else if (value instanceof byte[]) {
            return binaryProperty(name, (byte[]) value);
        }
        else if (value instanceof Long) {
            return longProperty(name, (Long) value);
        }
        else if (value instanceof Integer) {
            return longProperty(name, (Integer) value);
        }
        else if (value instanceof Double) {
            return doubleProperty(name, (Double) value);
        }
        else if (value instanceof Boolean) {
            return booleanProperty(name, (Boolean) value);
        }
        else if (value instanceof BigDecimal) {
            return decimalProperty(name, (BigDecimal) value);
        }
        else {
            throw new IllegalArgumentException("Can't infer type of value of class '" + value.getClass() + '\'');
        }
    }

    public static PropertyState emptyProperty(String name, Type<?> type) {
        return new EmptyPropertyState(name, type);
    }

    public static PropertyState stringProperty(String name, String value) {
        return new SinglePropertyState(name, new StringValue(value));
    }

    public static PropertyState binaryProperty(String name, byte[] value) {
        return new SinglePropertyState(name, new BinaryValue(value));
    }

    public static PropertyState longProperty(String name, long value) {
        return new SinglePropertyState(name, new LongValue(value));
    }

    public static PropertyState doubleProperty(String name, double value) {
        return new SinglePropertyState(name, new DoubleValue(value));
    }

    public static PropertyState dateProperty(String name, String value) {
        return new SinglePropertyState(name, new GenericValue(PropertyType.DATE, value));
    }

    public static PropertyState booleanProperty(String name, boolean value) {
        return new SinglePropertyState(name, value ? BooleanValue.TRUE : BooleanValue.FALSE);
    }

    public static PropertyState nameProperty(String name, String value) {
        return new SinglePropertyState(name, new GenericValue(PropertyType.NAME, value));
    }

    public static PropertyState pathProperty(String name, String value) {
        return new SinglePropertyState(name, new GenericValue(PropertyType.PATH, value));
    }

    public static PropertyState referenceProperty(String name, String value) {
        return new SinglePropertyState(name, new GenericValue(PropertyType.REFERENCE, value));
    }

    public static PropertyState weakreferenceProperty(String name, String value) {
        return new SinglePropertyState(name, new GenericValue(PropertyType.WEAKREFERENCE, value));
    }

    public static PropertyState uriProperty(String name, String value) {
        return new SinglePropertyState(name, new GenericValue(PropertyType.URI, value));
    }

    public static PropertyState decimalProperty(String name, BigDecimal value) {
        return new SinglePropertyState(name, new DecimalValue(value));
    }

    public static PropertyState binaryProperty(String name, Blob value) {
        return new SinglePropertyState(name, new BinaryValue(toBytes(value)));
    }

    public static PropertyState stringProperty(String name, Iterable<String> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (String value : values) {
            cvs.add(new StringValue(value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState binaryPropertyFromBlob(String name, Iterable<Blob> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (Blob value : values) {
            cvs.add(new BinaryValue(toBytes(value)));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState longProperty(String name, Iterable<Long> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (long value : values) {
            cvs.add(new LongValue(value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState doubleProperty(String name, Iterable<Double> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (double value : values) {
            cvs.add(new DoubleValue(value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState dateProperty(String name, Iterable<String> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (String value : values) {
            cvs.add(new GenericValue(PropertyType.DATE, value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState booleanProperty(String name, Iterable<Boolean> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (boolean value : values) {
            cvs.add(value ? BooleanValue.TRUE : BooleanValue.FALSE);
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState nameProperty(String name, Iterable<String> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (String value : values) {
            cvs.add(new GenericValue(PropertyType.NAME, value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState pathProperty(String name, Iterable<String> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (String value : values) {
            cvs.add(new GenericValue(PropertyType.PATH, value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState referenceProperty(String name, Iterable<String> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (String value : values) {
            cvs.add(new GenericValue(PropertyType.REFERENCE, value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState weakreferenceProperty(String name, Iterable<String> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (String value : values) {
            cvs.add(new GenericValue(PropertyType.WEAKREFERENCE, value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState uriProperty(String name, Iterable<String> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (String value : values) {
            cvs.add(new GenericValue(PropertyType.URI, value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState decimalProperty(String name, Iterable<BigDecimal> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (BigDecimal value : values) {
            cvs.add(new DecimalValue(value));
        }
        return new MultiPropertyState(name, cvs);
    }

    public static PropertyState binaryPropertyFromArray(String name, Iterable<byte[]> values) {
        List<CoreValue> cvs = Lists.newArrayList();
        for (byte[] value : values) {
            cvs.add(new BinaryValue(value));
        }
        return new MultiPropertyState(name, cvs);
    }

    private static byte[] toBytes(Blob blob) {
        try {
            InputStream is = blob.getNewStream();
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] b = new byte[4096];
                int n = is.read(b);
                while (n != -1) {
                    buffer.write(b, 0, n);
                    n = is.read(b);
                }
                return buffer.toByteArray();
            }
            finally {
                is.close();
            }
        }
        catch (IOException e) {
            // TODO
            return null;
        }
    }
}
