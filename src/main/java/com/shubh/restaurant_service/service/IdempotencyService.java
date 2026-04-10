
package com.shubh.restaurant_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {
    
    private final StringRedisTemplate redisTemplate;
    
    private static final String KEY_PREFIX = "idempotency:";
    private static final Duration TTL = Duration.ofMinutes(2);
    
    public boolean isProcessed(String eventType, String entityId) {
        String key = buildKey(eventType, entityId);
        Boolean exists = redisTemplate.hasKey(key);
        
        if (Boolean.TRUE.equals(exists)) {
            log.debug("Event already processed: {}:{}", eventType, entityId);
            return true;
        }
        
        log.debug("New event: {}:{}", eventType, entityId);
        return false;
    }
    
    public void markAsProcessed(String eventType, String entityId) {
        String key = buildKey(eventType, entityId);
        redisTemplate.opsForValue().set(key, "1", TTL);
        log.debug("Marked as processed: {}:{}", eventType, entityId);
    }
    
    public Long getVersion(String restaurantId) {
        String key = buildVersionKey(restaurantId);
        String value = redisTemplate.opsForValue().get(key);
        
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                log.error("Invalid version value in Redis for restaurant {}: {}", restaurantId, value);
                return null;
            }
        }
        
        return null;
    }
    
    public void setVersion(String restaurantId, Long version) {
        String key = buildVersionKey(restaurantId);
        redisTemplate.opsForValue().set(key, version.toString(), Duration.ofMinutes(30));
        log.debug("Stored version in Redis: restaurant={}, version={}", restaurantId, version);
    }
    
    public boolean isStaleVersion(String restaurantId, Long eventVersion) {
        Long currentVersion = getVersion(restaurantId);
        
        if (currentVersion == null) {
            log.debug("No version found in Redis for restaurant {}, accepting event version {}",
                    restaurantId, eventVersion);
            return false;
        }
        
        if (eventVersion <= currentVersion) {
            log.warn("Stale event version detected - restaurant={}, eventVersion={}, currentVersion={}",
                    restaurantId, eventVersion, currentVersion);
            return true;
        }
        
        log.debug("Event version is newer - restaurant={}, eventVersion={}, currentVersion={}",
                restaurantId, eventVersion, currentVersion);
        return false;
    }
    
    private String buildKey(String eventType, String entityId) {
        return KEY_PREFIX + eventType + ":" + entityId;
    }
    
    private String buildVersionKey(String restaurantId) {
        return "restaurant:version:" + restaurantId;
    }
}
