
package com.shubh.restaurant_service.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RestaurantDTO implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("address")
    private String address;
    
    @JsonProperty("contact_email")
    private String contactEmail;
    
    @JsonProperty("contact_number")
    private String contactNumber;
    
    @JsonProperty("cuisine_type")
    private String cuisineType;
    
    @JsonProperty("city")
    private String city;
    
    @JsonProperty("latitude")
    private Double latitude;
    
    @JsonProperty("longitude")
    private Double longitude;
    
    @JsonProperty("rating")
    private Double rating;
    
    @JsonProperty("total_reviews")
    private Integer totalReviews;
    
    @JsonProperty("price_range")
    private Integer priceRange;
    
    @JsonProperty("is_open")
    private Boolean isOpen;
    
    @JsonProperty("image_url")
    private String imageUrl;
    
    @JsonProperty("description")
    private String description;
    
    @JsonProperty("tags")
    private String tags;
    
    @JsonProperty("created_at")
    private LocalDateTime createdAt;
    
    @JsonProperty("updated_at")
    private LocalDateTime updatedAt;
    
    @JsonProperty("version")
    private Long version;
    
    @JsonProperty("is_deleted")
    private Boolean isDeleted;
    
    @JsonProperty("deleted_at")
    private LocalDateTime deletedAt;
}
