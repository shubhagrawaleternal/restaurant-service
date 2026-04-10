
package com.shubh.restaurant_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheInvalidationService {
    
    private final CacheManager cacheManager;
    private final RedisTemplate<String, Object> redisTemplate;
    
    private static final String RESTAURANT_CACHE = "restaurants";
    private static final String RESTAURANT_LIST_CACHE = "restaurant-list";
    
    public void invalidateRestaurantCache(String restaurantId, String city, String cuisineType) {
        try {

            Cache restaurantCache = cacheManager.getCache(RESTAURANT_CACHE);
            if (restaurantCache != null) {
                restaurantCache.evict(restaurantId);
                log.info("Invalidated cache for restaurant: {}", restaurantId);
            }
            
            invalidateListCachesByPattern(city, cuisineType);
            
        } catch (Exception e) {
            log.error("Error invalidating cache for restaurant: {}", restaurantId, e);
        }
    }
    

    private void invalidateListCachesByPattern(String city, String cuisineType) {
        try {
            //Invalidate all caches for this city with specific cuisine
            // Example: Delhi-Italian-*
            if (cuisineType != null && !cuisineType.trim().isEmpty()) {
                String pattern = RESTAURANT_LIST_CACHE + "::" + city + "-" + cuisineType + "-*";
                deleteKeysByPattern(pattern);
                log.info("Invalidated list cache for city={}, cuisine={}", city, cuisineType);
            }
            
        } catch (Exception e) {
            log.error("Error invalidating list cache patterns for city={}, cuisine={}", city, cuisineType, e);
        }
    }
    
    private void deleteKeysByPattern(String pattern) {
        try {
            Set<String> keys = redisTemplate.keys(pattern);
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
                log.debug("Deleted {} cache keys matching pattern: {}", keys.size(), pattern);
            }
        } catch (Exception e) {
            log.error("Error deleting keys by pattern: {}", pattern, e);
        }
    }
}
