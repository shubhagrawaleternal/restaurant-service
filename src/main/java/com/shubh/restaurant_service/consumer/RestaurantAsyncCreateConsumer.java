
package com.shubh.restaurant_service.consumer;

import com.shubh.restaurant_service.dto.RestaurantDTO;
import com.shubh.restaurant_service.entity.Restaurant;
import com.shubh.restaurant_service.event.RestaurantEvent;
import com.shubh.restaurant_service.repository.RestaurantRepository;
import com.shubh.restaurant_service.service.IdempotencyService;
import com.shubh.restaurant_service.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class RestaurantAsyncCreateConsumer {
    
    private final RestaurantRepository restaurantRepository;
    private final KafkaProducerService kafkaProducerService;
    private final IdempotencyService idempotencyService;
    
    @Value("${kafka.topic.restaurant-created}")
    private String restaurantCreatedTopic;
    
    private static final String EVENT_TYPE = "restaurant-create-async";
    
    @KafkaListener(
        topics = "${kafka.topic.restaurant-create-async}",
        groupId = "${kafka.consumer.group.async-create}",
        concurrency = "3"
    )
    @Transactional
    public void consume(RestaurantEvent event, Acknowledgment ack) {
        try {
            log.info("📥 Received restaurant.create.async event: restaurantId={}, eventId={}", 
                    event.getRestaurantData().getId(), 
                    event.getEventId());
            
            RestaurantDTO dto = event.getRestaurantData();
            String restaurantId = dto.getId();
            
            if (idempotencyService.isProcessed(EVENT_TYPE, restaurantId)) {
                log.warn("🔴 Duplicate event detected - already processed: {}", restaurantId);
                ack.acknowledge();
                return;
            }
            
            if (restaurantRepository.findByIdIncludingDeleted(restaurantId).isPresent()) {
                log.warn("⚠️ Restaurant already exists in MySQL: {}", restaurantId);
                idempotencyService.markAsProcessed(EVENT_TYPE, restaurantId);
                ack.acknowledge();
                return;
            }
            
            String email = nullIfEmpty(dto.getContactEmail());
            
            if (email != null && restaurantRepository.existsByContactEmailAndIsDeletedFalse(email)) {
                log.error("❌ Restaurant with email already exists: {}", email);
                ack.acknowledge();
                return;
            }
            
            Restaurant restaurant = Restaurant.builder()
                    .id(restaurantId)
                    .name(dto.getName())
                    .address(nullIfEmpty(dto.getAddress()))
                    .contactEmail(email)
                    .contactNumber(nullIfEmpty(dto.getContactNumber()))
                    .cuisineType(nullIfEmpty(dto.getCuisineType()))
                    .city(nullIfEmpty(dto.getCity()))
                    .latitude(dto.getLatitude())
                    .longitude(dto.getLongitude())
                    .rating(dto.getRating() != null ? dto.getRating() : 0.0)
                    .totalReviews(dto.getTotalReviews() != null ? dto.getTotalReviews() : 0)
                    .priceRange(dto.getPriceRange() != null ? dto.getPriceRange() : 2)
                    .isOpen(dto.getIsOpen() != null ? dto.getIsOpen() : true)
                    .imageUrl(nullIfEmpty(dto.getImageUrl()))
                    .description(nullIfEmpty(dto.getDescription()))
                    .tags(nullIfEmpty(dto.getTags()))
                    .version(1L)
                    .isDeleted(false)
                    .createdAt(dto.getCreatedAt())
                    .updatedAt(dto.getUpdatedAt())
                    .build();
            
            Restaurant saved = restaurantRepository.save(restaurant);
            log.info("✅ Saved restaurant to MySQL (async flow): {}", saved.getId());
            
            RestaurantEvent createdEvent = RestaurantEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .eventType(RestaurantEvent.EventType.CREATE)
                    .restaurantData(convertToDTO(saved))
                    .timestamp(System.currentTimeMillis())
                    .version(saved.getVersion())
                    .build();
            
            kafkaProducerService.publishEvent(restaurantCreatedTopic, saved.getId(), createdEvent);
            log.info("📤 Published restaurant.created event from async consumer: {}", saved.getId());
            
            idempotencyService.markAsProcessed(EVENT_TYPE, restaurantId);
            ack.acknowledge();
            log.info("✅ Acknowledged restaurant.create.async event: {}", restaurantId);
            
        } catch (Exception e) {
            log.error("❌ Error processing restaurant.create.async event: eventId={}",
                    event.getEventId(), e);
        }
    }
    
    private RestaurantDTO convertToDTO(Restaurant restaurant) {
        return RestaurantDTO.builder()
                .id(restaurant.getId())
                .name(restaurant.getName())
                .address(restaurant.getAddress())
                .contactEmail(restaurant.getContactEmail())
                .contactNumber(restaurant.getContactNumber())
                .cuisineType(restaurant.getCuisineType())
                .city(restaurant.getCity())
                .latitude(restaurant.getLatitude())
                .longitude(restaurant.getLongitude())
                .rating(restaurant.getRating())
                .totalReviews(restaurant.getTotalReviews())
                .priceRange(restaurant.getPriceRange())
                .isOpen(restaurant.getIsOpen())
                .imageUrl(restaurant.getImageUrl())
                .description(restaurant.getDescription())
                .tags(restaurant.getTags())
                .createdAt(restaurant.getCreatedAt())
                .updatedAt(restaurant.getUpdatedAt())
                .version(restaurant.getVersion())
                .isDeleted(restaurant.getIsDeleted())
                .deletedAt(restaurant.getDeletedAt())
                .build();
    }
    
    private String nullIfEmpty(String value) {
        return (value == null || value.trim().isEmpty()) ? null : value;
    }
}
