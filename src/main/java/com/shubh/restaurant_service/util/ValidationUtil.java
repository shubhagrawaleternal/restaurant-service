
package com.shubh.restaurant_service.util;

public class ValidationUtil {

    public static boolean isNullOrEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
    
    public static void requireNonEmpty(String value, String fieldName) {
        if (isNullOrEmpty(value)) {
            throw new IllegalArgumentException(fieldName + " is required and cannot be empty");
        }
    }
}
