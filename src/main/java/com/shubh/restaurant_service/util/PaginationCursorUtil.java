
package com.shubh.restaurant_service.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PaginationCursorUtil {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static String encodeCursor(Map<String, AttributeValue> lastEvaluatedKey) {
        if (lastEvaluatedKey == null || lastEvaluatedKey.isEmpty()) {
            return null;
        }
        
        try {
            Map<String, String> simpleMap = new HashMap<>();
            lastEvaluatedKey.forEach((key, value) -> {
                if (value.s() != null) {
                    simpleMap.put(key, value.s());
                } else if (value.n() != null) {
                    simpleMap.put(key, value.n());
                }
            });
            
            String json = objectMapper.writeValueAsString(simpleMap);
            return Base64.getUrlEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
            
        } catch (Exception e) {
            log.error("Failed to encode pagination cursor", e);
            return null;
        }
    }
    
    public static Map<String, AttributeValue> decodeCursor(String cursor) {
        if (cursor == null || cursor.trim().isEmpty()) {
            return null;
        }
        
        try {
            byte[] decodedBytes = Base64.getUrlDecoder().decode(cursor);
            String json = new String(decodedBytes, StandardCharsets.UTF_8);
            
            Map<String, String> simpleMap = objectMapper.readValue(
                json,
                new TypeReference<Map<String, String>>() {}
            );
            
            Map<String, AttributeValue> attributeMap = new HashMap<>();
            simpleMap.forEach((key, value) -> {
                if (isNumeric(value)) {
                    attributeMap.put(key, AttributeValue.builder().n(value).build());
                } else {
                    attributeMap.put(key, AttributeValue.builder().s(value).build());
                }
            });
            
            return attributeMap;
            
        } catch (Exception e) {
            log.error("Failed to decode pagination cursor: {}", cursor, e);
            return null;
        }
    }
    
    private static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
