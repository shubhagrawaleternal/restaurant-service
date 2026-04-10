
package com.shubh.restaurant_service.util;

import com.shubh.restaurant_service.dto.RestaurantDTO;
import com.shubh.restaurant_service.entity.Restaurant;
import com.shubh.restaurant_service.model.DynamoRestaurant;

import java.time.LocalDateTime;

public class RestaurantMapper {
    
    public static RestaurantDTO toDTO(Restaurant restaurant) {
        if (restaurant == null) {
            return null;
        }
        
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
    
    
    public static Restaurant toEntity(RestaurantDTO dto, String id) {
        if (dto == null) {
            return null;
        }
        
        return Restaurant.builder()
                .id(id)
                .name(dto.getName())
                .address(dto.getAddress())
                .contactEmail(dto.getContactEmail())
                .contactNumber(dto.getContactNumber())
                .cuisineType(dto.getCuisineType())
                .city(dto.getCity())
                .latitude(dto.getLatitude())
                .longitude(dto.getLongitude())
                .rating(dto.getRating() != null ? dto.getRating() : 0.0)
                .totalReviews(dto.getTotalReviews() != null ? dto.getTotalReviews() : 0)
                .priceRange(dto.getPriceRange() != null ? dto.getPriceRange() : 2)
                .isOpen(dto.getIsOpen() != null ? dto.getIsOpen() : true)
                .imageUrl(dto.getImageUrl())
                .description(dto.getDescription())
                .tags(dto.getTags())
                .version(1L)
                .isDeleted(false)
                .build();
    }
    
    
    public static void updateEntityFromDTO(Restaurant restaurant, RestaurantDTO dto) {
        if (restaurant == null || dto == null) {
            return;
        }
        
        if (dto.getName() != null) {
            restaurant.setName(dto.getName());
        }
        if (dto.getAddress() != null) {
            restaurant.setAddress(dto.getAddress());
        }
        if (dto.getContactEmail() != null) {
            restaurant.setContactEmail(dto.getContactEmail());
        }
        if (dto.getContactNumber() != null) {
            restaurant.setContactNumber(dto.getContactNumber());
        }
        if (dto.getCuisineType() != null) {
            restaurant.setCuisineType(dto.getCuisineType());
        }
        
        if (dto.getCity() != null) {
            restaurant.setCity(dto.getCity());
        }
        if (dto.getLatitude() != null) {
            restaurant.setLatitude(dto.getLatitude());
        }
        if (dto.getLongitude() != null) {
            restaurant.setLongitude(dto.getLongitude());
        }
        
        if (dto.getRating() != null) {
            restaurant.setRating(dto.getRating());
        }
        if (dto.getTotalReviews() != null) {
            restaurant.setTotalReviews(dto.getTotalReviews());
        }
        
        if (dto.getPriceRange() != null) {
            restaurant.setPriceRange(dto.getPriceRange());
        }
        if (dto.getIsOpen() != null) {
            restaurant.setIsOpen(dto.getIsOpen());
        }
        if (dto.getImageUrl() != null) {
            restaurant.setImageUrl(dto.getImageUrl());
        }
        if (dto.getDescription() != null) {
            restaurant.setDescription(dto.getDescription());
        }
        if (dto.getTags() != null) {
            restaurant.setTags(dto.getTags());
        }
        
        restaurant.setUpdatedAt(LocalDateTime.now());
    }
    
    
    public static DynamoRestaurant toDynamoModel(Restaurant restaurant) {
        if (restaurant == null) {
            return null;
        }
        
        String sortKey = DynamoRestaurant.generateSortKey(
            restaurant.getRating(),
            restaurant.getId()
        );
        
        String city = restaurant.getCity() != null ? restaurant.getCity() : "UNKNOWN";
        String cuisine = restaurant.getCuisineType() != null ? restaurant.getCuisineType() : "UNKNOWN";
        String cityCuisine = DynamoRestaurant.generateCityCuisineKey(city, cuisine);
        
        return DynamoRestaurant.builder()
                .city(city)
                .sortKey(sortKey)
                .cityCuisine(cityCuisine)
                .restaurantId(restaurant.getId())
                .name(restaurant.getName())
                .latitude(restaurant.getLatitude())
                .longitude(restaurant.getLongitude())
                .cuisineType(restaurant.getCuisineType())
                .rating(restaurant.getRating())
                .totalReviews(restaurant.getTotalReviews())
                .priceRange(restaurant.getPriceRange())
                .isOpen(restaurant.getIsOpen())
                .imageUrl(restaurant.getImageUrl())
                .description(restaurant.getDescription())
                .tags(restaurant.getTags())
                .contactEmail(restaurant.getContactEmail())
                .contactNumber(restaurant.getContactNumber())
                .version(restaurant.getVersion())
                .isDeleted(restaurant.getIsDeleted())
                .build();
    }
    
    
    public static RestaurantDTO dynamoToDTO(DynamoRestaurant dynamo) {
        if (dynamo == null) {
            return null;
        }
        
        return RestaurantDTO.builder()
                .id(dynamo.getRestaurantId())
                .name(dynamo.getName())
                .city(dynamo.getCity())
                .cuisineType(dynamo.getCuisineType())
                .latitude(dynamo.getLatitude())
                .longitude(dynamo.getLongitude())
                .rating(dynamo.getRating())
                .totalReviews(dynamo.getTotalReviews())
                .priceRange(dynamo.getPriceRange())
                .isOpen(dynamo.getIsOpen())
                .imageUrl(dynamo.getImageUrl())
                .description(dynamo.getDescription())
                .tags(dynamo.getTags())
                .contactEmail(dynamo.getContactEmail())
                .contactNumber(dynamo.getContactNumber())
                .version(dynamo.getVersion())
                .isDeleted(dynamo.getIsDeleted())
                .build();
    }
    
    
    public static DynamoRestaurant toDynamoModel(RestaurantDTO dto) {
        if (dto == null) {
            return null;
        }
        
        if (dto.getId() == null || dto.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("Restaurant ID cannot be null or empty for DynamoDB");
        }
        
        String city = dto.getCity() != null && !dto.getCity().trim().isEmpty()
            ? dto.getCity().trim()
            : "UNKNOWN";
        
        String sortKey = DynamoRestaurant.generateSortKey(
            dto.getRating() != null ? dto.getRating() : 0.0,
            dto.getId()
        );
        
        String cuisine = dto.getCuisineType() != null ? dto.getCuisineType() : "UNKNOWN";
        String cityCuisine = DynamoRestaurant.generateCityCuisineKey(city, cuisine);
        
        return DynamoRestaurant.builder()
                .city(city)
                .sortKey(sortKey)
                .cityCuisine(cityCuisine)
                .restaurantId(dto.getId())
                .name(dto.getName())
                .latitude(dto.getLatitude())
                .longitude(dto.getLongitude())
                .cuisineType(dto.getCuisineType())
                .rating(dto.getRating() != null ? dto.getRating() : 0.0)
                .totalReviews(dto.getTotalReviews() != null ? dto.getTotalReviews() : 0)
                .priceRange(dto.getPriceRange() != null ? dto.getPriceRange() : 2)
                .isOpen(dto.getIsOpen() != null ? dto.getIsOpen() : true)
                .imageUrl(dto.getImageUrl())
                .description(dto.getDescription())
                .tags(dto.getTags())
                .contactEmail(dto.getContactEmail())
                .contactNumber(dto.getContactNumber())
                .version(dto.getVersion())
                .isDeleted(dto.getIsDeleted() != null ? dto.getIsDeleted() : false)
                .build();
    }
    
    
}
