
package com.shubh.restaurant_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DynamoRestaurant {
    
    private String city;
    private String sortKey;
    private String cityCuisine;
    private String restaurantId;
    private String name;
    private Double latitude;
    private Double longitude;
    private String cuisineType;
    private Double rating;
    private Integer totalReviews;
    private Integer priceRange;
    private Boolean isOpen;
    private String imageUrl;
    private String description;
    private String tags;
    private String contactEmail;
    private String contactNumber;
    private Long version;
    private Boolean isDeleted;
    
    @DynamoDbPartitionKey
    @DynamoDbAttribute("city")
    public String getCity() {
        return city;
    }
    
    @DynamoDbSortKey
    @DynamoDbAttribute("sortKey")
    public String getSortKey() {
        return sortKey;
    }
    
    @DynamoDbSecondaryPartitionKey(indexNames = "city-cuisine-rating-index")
    @DynamoDbAttribute("cityCuisine")
    public String getCityCuisine() {
        return cityCuisine;
    }
    
    @DynamoDbAttribute("restaurantId")
    public String getRestaurantId() {
        return restaurantId;
    }
    
    @DynamoDbAttribute("name")
    public String getName() {
        return name;
    }
    
    @DynamoDbAttribute("latitude")
    public Double getLatitude() {
        return latitude;
    }
    
    @DynamoDbAttribute("longitude")
    public Double getLongitude() {
        return longitude;
    }
    
    @DynamoDbAttribute("cuisineType")
    public String getCuisineType() {
        return cuisineType;
    }
    
    @DynamoDbAttribute("rating")
    public Double getRating() {
        return rating;
    }
    
    @DynamoDbAttribute("totalReviews")
    public Integer getTotalReviews() {
        return totalReviews;
    }
    
    @DynamoDbAttribute("priceRange")
    public Integer getPriceRange() {
        return priceRange;
    }
    
    @DynamoDbAttribute("isOpen")
    public Boolean getIsOpen() {
        return isOpen;
    }
    
    @DynamoDbAttribute("imageUrl")
    public String getImageUrl() {
        return imageUrl;
    }
    
    @DynamoDbAttribute("description")
    public String getDescription() {
        return description;
    }
    
    @DynamoDbAttribute("tags")
    public String getTags() {
        return tags;
    }
    
    @DynamoDbAttribute("contactEmail")
    public String getContactEmail() {
        return contactEmail;
    }
    
    @DynamoDbAttribute("contactNumber")
    public String getContactNumber() {
        return contactNumber;
    }
    
    @DynamoDbAttribute("version")
    public Long getVersion() {
        return version;
    }
    
    @DynamoDbAttribute("isDeleted")
    public Boolean getIsDeleted() {
        return isDeleted;
    }
    
    public static String generateSortKey(Double rating, String restaurantId) {
        String ratingStr = String.format("%05.2f", rating != null ? rating : 0.0);
        return "rating#" + ratingStr + "#id#" + restaurantId;
    }
    
    public static String generateCityCuisineKey(String city, String cuisineType) {
        return city + "#" + cuisineType;
    }
}
