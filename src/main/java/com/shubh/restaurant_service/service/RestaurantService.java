
package com.shubh.restaurant_service.service;

import com.shubh.restaurant_service.dto.PaginationResult;
import com.shubh.restaurant_service.dto.RestaurantDTO;
import com.shubh.restaurant_service.entity.Restaurant;
import com.shubh.restaurant_service.event.RestaurantEvent;
import com.shubh.restaurant_service.model.DynamoRestaurant;
import com.shubh.restaurant_service.repository.DynamoDbRepository;
import com.shubh.restaurant_service.repository.RestaurantRepository;
import com.shubh.restaurant_service.util.PaginationCursorUtil;
import com.shubh.restaurant_service.util.RestaurantMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class RestaurantService {
    
    private final RestaurantRepository restaurantRepository;
    private final DynamoDbRepository dynamoDbRepository;
    private final KafkaProducerService kafkaProducerService;

    @Value("${kafka.topic.restaurant-created}")
    private String restaurantCreatedTopic;
    
    @Value("${kafka.topic.restaurant-create-async}")
    private String restaurantCreateAsyncTopic;
    
    @Value("${kafka.topic.restaurant-updated}")
    private String restaurantUpdatedTopic;
    
    @Value("${kafka.topic.restaurant-deleted}")
    private String restaurantDeletedTopic;
    
    @Transactional
    @RateLimiter(name = "restaurant-create", fallbackMethod = "createFallback")
    public RestaurantDTO createRestaurantSync(RestaurantDTO dto) {
        log.info("Creating restaurant synchronously: {}", dto.getName());
        
        String email = dto.getContactEmail();
        if (email != null && restaurantRepository.existsByContactEmailAndIsDeletedFalse(email)) {
            throw new RuntimeException("Restaurant with email already exists: " + email);
        }
        
        String id = UUID.randomUUID().toString();
        Restaurant restaurant = RestaurantMapper.toEntity(dto, id);
        
        Restaurant saved = restaurantRepository.save(restaurant);
        log.info("✅ Saved restaurant to MySQL: {} with version: {}", saved.getId(), saved.getVersion());
        
        RestaurantDTO savedDTO = RestaurantMapper.toDTO(saved);
        RestaurantEvent event = RestaurantEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(RestaurantEvent.EventType.CREATE)
                .restaurantData(savedDTO)
                .timestamp(System.currentTimeMillis())
                .version(saved.getVersion())
                .build();
        
        kafkaProducerService.publishEvent(restaurantCreatedTopic, saved.getId(), event);
        log.info("📤 Published restaurant.created event for: {}", saved.getId());
        
        return savedDTO;
    }
    
    @RateLimiter(name = "restaurant-create", fallbackMethod = "createFallback")
    public RestaurantDTO createRestaurantAsync(RestaurantDTO dto) {
        log.info("Creating restaurant asynchronously: {}", dto.getName());
        
        String id = UUID.randomUUID().toString();
        dto.setId(id);
        dto.setCreatedAt(LocalDateTime.now());
        dto.setUpdatedAt(LocalDateTime.now());
        dto.setVersion(1L);
        dto.setIsDeleted(false);
        
        RestaurantEvent event = RestaurantEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(RestaurantEvent.EventType.CREATE)
                .restaurantData(dto)
                .timestamp(System.currentTimeMillis())
                .version(1L)
                .build();
        
        kafkaProducerService.publishEvent(restaurantCreateAsyncTopic, id, event);
        log.info("📤 Published restaurant.create.async event for: {}", id);
        
        return dto;
    }
    
    private RestaurantDTO createFallback(RestaurantDTO dto, Exception e) {
        if (e instanceof io.github.resilience4j.ratelimiter.RequestNotPermitted) {
            log.error("❌ Rate limit exceeded during create");
            throw new RuntimeException("Rate limit exceeded. Please try again later.");
        }
        
        log.error("❌ Error during create: {}", e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
    }
    
    @Transactional
    @RateLimiter(name = "restaurant-update", fallbackMethod = "updateFallback")
    public RestaurantDTO updateRestaurant(String id, RestaurantDTO dto) {
        log.info("Updating restaurant: {}", id);
        
        Restaurant restaurant = restaurantRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Restaurant not found or deleted: " + id));
        
        Long currentVersion = restaurant.getVersion();
        log.info("Current version: {}", currentVersion);
        
        String oldCity = restaurant.getCity();
        Double oldRating = restaurant.getRating();
        String oldCuisineType = restaurant.getCuisineType();
        
        RestaurantMapper.updateEntityFromDTO(restaurant, dto);

        try {
            Restaurant updated = restaurantRepository.save(restaurant);
            log.info("✅ Updated restaurant in MySQL: {} (version {} → {})",
                    updated.getId(), currentVersion, updated.getVersion());
            
            RestaurantDTO updatedDTO = RestaurantMapper.toDTO(updated);
            RestaurantEvent event = RestaurantEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .eventType(RestaurantEvent.EventType.UPDATE)
                    .restaurantData(updatedDTO)
                    .timestamp(System.currentTimeMillis())
                    .version(updated.getVersion())
                    .city(oldCity)
                    .rating(oldRating)
                    .cuisineType(oldCuisineType)
                    .build();
            
            kafkaProducerService.publishEvent(restaurantUpdatedTopic, updated.getId(), event);
            log.info("📤 Published restaurant.updated event for: {}", updated.getId());
            
            return updatedDTO;
            
        } catch (ObjectOptimisticLockingFailureException e) {
            log.error("❌ Optimistic locking failure - concurrent update detected for: {}", id);
            throw new RuntimeException("Restaurant was updated by another user. Please refresh and try again.");
        }
    }
    
    private RestaurantDTO updateFallback(String id, RestaurantDTO dto, Exception e) {
        if (e instanceof io.github.resilience4j.ratelimiter.RequestNotPermitted) {
            log.error("❌ Rate limit exceeded during update");
            throw new RuntimeException("Rate limit exceeded. Please try again later.");
        }
        
        log.error("❌ Error during update: {}", e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
    }
    
    @Transactional
    @RateLimiter(name = "restaurant-delete", fallbackMethod = "deleteFallback")
    public void deleteRestaurant(String id) {
        log.info("Soft deleting restaurant: {}", id);
        
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("Restaurant ID is required for deletion");
        }
        
        Restaurant restaurant = restaurantRepository.findByIdIncludingDeleted(id)
                .orElseThrow(() -> new RuntimeException("Restaurant not found with ID: " + id));
        
        if (restaurant.getIsDeleted()) {
            log.warn("⚠️ Restaurant already deleted: {}", id);
            throw new RuntimeException("Restaurant is already deleted with ID: " + id);
        }
        
        restaurant.setIsDeleted(true);
        restaurant.setDeletedAt(LocalDateTime.now());
        restaurant.setUpdatedAt(LocalDateTime.now());
        
        Restaurant deleted = restaurantRepository.save(restaurant);
        log.info("✅ Soft deleted restaurant in MySQL: {}", deleted.getId());

        RestaurantDTO deletedDTO = RestaurantDTO.builder()
                .id(deleted.getId())
                .build();
        
        RestaurantEvent event = RestaurantEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(RestaurantEvent.EventType.DELETE)
                .restaurantData(deletedDTO)
                .timestamp(System.currentTimeMillis())
                .version(deleted.getVersion())
                .city(deleted.getCity())
                .rating(deleted.getRating())
                .cuisineType(deleted.getCuisineType())
                .build();
        
        kafkaProducerService.publishEvent(restaurantDeletedTopic, deleted.getId(), event);
        log.info("📤 Published restaurant.deleted event for: {}", deleted.getId());
    }
    
    @Cacheable(value = "restaurants", key = "#p0")
    @RateLimiter(name = "restaurant-get", fallbackMethod = "getFallback")
    public RestaurantDTO getRestaurantById(String id) {
        log.warn("🔴 CACHE MISS - Fetching restaurant from MySQL: {}", id);
        
        Restaurant restaurant = restaurantRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Restaurant not found or deleted: " + id));
        
        log.info("✅ Retrieved from MySQL: {}", id);
        
        return RestaurantMapper.toDTO(restaurant);
    }


    @Cacheable(value = "restaurant-list", key = "#p0 + '-' + (#p1 ?: 'all') + '-' + (#p3 ?: 'first') + '-' + #p2")
    @RateLimiter(name = "restaurant-list", fallbackMethod = "listFallback")
    @CircuitBreaker(name = "dynamodb", fallbackMethod = "listRestaurantsCursorFallback")
    public PaginationResult<RestaurantDTO> listRestaurantsByCityAndCuisine(
            String city, String cuisineType, int pageSize, String cursor) {
        
        log.info("🔴 CACHE MISS - Fetching restaurants from DynamoDB: city={}, cuisine={}, pageSize={}, cursor={}",
                city, cuisineType, pageSize, cursor != null ? "present" : "null");
        
        int limit = Math.min(Math.max(pageSize, 1), 100);
        
        int fetchLimit = limit + 1;
        
        try {
            Map<String, AttributeValue> exclusiveStartKey = PaginationCursorUtil.decodeCursor(cursor);
            
            DynamoDbRepository.QueryResult result;
            if (cuisineType != null && !cuisineType.trim().isEmpty()) {
                result = dynamoDbRepository.queryByCityAndCuisine(city, cuisineType, fetchLimit, exclusiveStartKey);
            } else {
                result = dynamoDbRepository.queryByCity(city, fetchLimit, exclusiveStartKey);
            }
            
            List<RestaurantDTO> allRestaurants = result.getItems().stream()
                    .map(RestaurantMapper::dynamoToDTO)
                    .collect(Collectors.toList());
            
            boolean hasMore = allRestaurants.size() > limit;
            
            List<RestaurantDTO> restaurants = hasMore
                    ? new ArrayList<>(allRestaurants.subList(0, limit))
                    : allRestaurants;
            
            String encodedNextCursor = null;
            if (hasMore && !restaurants.isEmpty()) {
                RestaurantDTO lastItem = restaurants.get(restaurants.size() - 1);
                Map<String, AttributeValue> nextKey = new HashMap<>();
                nextKey.put("city", AttributeValue.builder().s(lastItem.getCity()).build());
                nextKey.put("sortKey", AttributeValue.builder()
                        .s(DynamoRestaurant.generateSortKey(lastItem.getRating(), lastItem.getId()))
                        .build());
                if (cuisineType != null && !cuisineType.trim().isEmpty()) {
                    nextKey.put("cityCuisine", AttributeValue.builder()
                            .s(DynamoRestaurant.generateCityCuisineKey(lastItem.getCity(), lastItem.getCuisineType()))
                            .build());
                }
                encodedNextCursor = PaginationCursorUtil.encodeCursor(nextKey);
            }
            
            log.info("✅ Retrieved {} restaurants from DynamoDB, hasMore={}",
                    restaurants.size(), hasMore);
            
            return PaginationResult.<RestaurantDTO>builder()
                    .items(restaurants)
                    .count(restaurants.size())
                    .nextCursor(encodedNextCursor)
                    .hasMore(hasMore)
                    .build();
                    
        } catch (Exception e) {
            log.error("❌ DynamoDB error: {}", e.getMessage());
            throw e;
        }
    }
    

    private PaginationResult<RestaurantDTO> listRestaurantsCursorFallback(
            String city, String cuisineType, int pageSize, String cursor, Exception e) {
        
        if (e instanceof io.github.resilience4j.ratelimiter.RequestNotPermitted) {
            log.error("❌ Rate limit exceeded during list");
            throw new RuntimeException("Rate limit exceeded. Please try again later.");
        }
        
        log.error("❌ Circuit breaker OPEN - DynamoDB unavailable: {}", e.getMessage());
        throw new RuntimeException("Service temporarily unavailable. Please try again later.", e);
    }
    
    private void deleteFallback(String id, Exception e) {
        if (e instanceof io.github.resilience4j.ratelimiter.RequestNotPermitted) {
            log.error("❌ Rate limit exceeded during delete");
            throw new RuntimeException("Rate limit exceeded. Please try again later.");
        }
        
        log.error("❌ Error during delete: {}", e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
    }
    
    private RestaurantDTO getFallback(String id, Exception e) {
        if (e instanceof io.github.resilience4j.ratelimiter.RequestNotPermitted) {
            log.error("❌ Rate limit exceeded during get");
            throw new RuntimeException("Rate limit exceeded. Please try again later.");
        }
        
        log.error("❌ Error during get: {}", e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
    }
    
    private PaginationResult<RestaurantDTO> listFallback(
            String city, String cuisineType, int pageSize, String cursor, Exception e) {
        if (e instanceof io.github.resilience4j.ratelimiter.RequestNotPermitted) {
            log.error("❌ Rate limit exceeded during list");
            // Re-throw the original RequestNotPermitted so circuit breaker can ignore it
            throw (io.github.resilience4j.ratelimiter.RequestNotPermitted) e;
        }
        
        log.error("❌ Error during list: {}", e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
    }
    
}
