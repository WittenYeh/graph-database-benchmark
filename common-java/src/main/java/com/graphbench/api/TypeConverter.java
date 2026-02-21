package com.graphbench.api;

/**
 * Utility class for converting values between different types.
 * Used for converting CSV strings and JSON values to the correct Java types.
 */
public class TypeConverter {

    /**
     * Convert a String value to the appropriate Java type based on target class.
     * Used when loading data from CSV files.
     *
     * @param value String value from CSV
     * @param targetType Target Java class (Integer, Long, Float, Double, Boolean, String)
     * @return Converted value, or null if value is null/empty
     */
    public static Object convertFromString(String value, Class<?> targetType) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            if (targetType == Integer.class) {
                return Integer.parseInt(value);
            } else if (targetType == Long.class) {
                return Long.parseLong(value);
            } else if (targetType == Float.class) {
                return Float.parseFloat(value);
            } else if (targetType == Double.class) {
                return Double.parseDouble(value);
            } else if (targetType == Boolean.class) {
                return Boolean.parseBoolean(value);
            } else {
                return value; // String or unknown type
            }
        } catch (NumberFormatException e) {
            // If conversion fails, fall back to String
            return value;
        }
    }

    /**
     * Convert a Number to the appropriate Java numeric type.
     * Used when converting JSON numbers (typically Double) to the correct type.
     *
     * @param num Number value (typically Double from JSON)
     * @param targetType Target Java class (Integer, Long, Float, Double)
     * @return Converted number, or original value if target type is not numeric
     */
    public static Object convertFromNumber(Number num, Class<?> targetType) {
        if (num == null) {
            return null;
        }
        if (targetType == Integer.class) {
            return num.intValue();
        } else if (targetType == Long.class) {
            return num.longValue();
        } else if (targetType == Float.class) {
            return num.floatValue();
        } else if (targetType == Double.class) {
            return num.doubleValue();
        }
        return num;
    }

    /**
     * Convert a query value (from JSON) to the correct type based on metadata.
     * Gson deserializes JSON numbers as Double, so we need to convert them to the correct type.
     *
     * @param value Value from JSON (typically String, Double, Boolean, or null)
     * @param targetType Target Java class from metadata
     * @return Converted value
     */
    public static Object convertQueryValue(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }

        // If value is already the correct type, return as-is
        if (targetType.isInstance(value)) {
            return value;
        }

        // Convert from Number (typically Double from JSON) to target type
        if (value instanceof Number) {
            return convertFromNumber((Number) value, targetType);
        }

        // Convert from String if needed
        if (value instanceof String) {
            return convertFromString((String) value, targetType);
        }

        // For Boolean or other types, return as-is
        return value;
    }
}
