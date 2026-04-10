
package com.shubh.restaurant_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.springframework.data.domain.Persistable;

import java.time.LocalDateTime;


@Entity
@Table(name = "restaurants", indexes = {
    @Index(name = "idx_restaurant_email", columnList = "contact_email"),
    @Index(name = "idx_is_deleted", columnList = "is_deleted")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Restaurant implements Persistable<String> {
    
    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private String id;
    
    @Column(name = "name", nullable = false, length = 255)
    private String name;
    
    @Column(name = "address", length = 500)
    private String address;
    
    @Column(name = "contact_email", unique = true, nullable = false, length = 255)
    private String contactEmail;
    
    @Column(name = "contact_number", length = 20)
    private String contactNumber;
    
    @Column(name = "cuisine_type", length = 100)
    private String cuisineType;
    
    @Column(name = "city", length = 100)
    private String city;
    
    @Column(name = "latitude")
    private Double latitude;
    
    @Column(name = "longitude")
    private Double longitude;
    
    @Column(name = "rating")
    @Builder.Default
    private Double rating = 0.0;
    @Column(name = "total_reviews")
    @Builder.Default
    private Integer totalReviews = 0;
    
    @Column(name = "price_range")
    @Builder.Default
    private Integer priceRange = 2;
    
    @Column(name = "is_open")
    @Builder.Default
    private Boolean isOpen = true;
    
    @Column(name = "image_url", length = 500)
    private String imageUrl;
    
    @Column(name = "description", length = 1000)
    private String description;
    
    @Column(name = "tags", length = 500)
    private String tags;
    
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
    
    @Version
    @Column(name = "version", nullable = false)
    private Long version = 1L;
    
    @Column(name = "is_deleted", nullable = false)
    @Builder.Default
    private Boolean isDeleted = false;
    
    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;
    
    @Transient
    @Builder.Default
    private boolean isNew = true;
    
    @Override
    public boolean isNew() {
        return createdAt == null;
    }
}
