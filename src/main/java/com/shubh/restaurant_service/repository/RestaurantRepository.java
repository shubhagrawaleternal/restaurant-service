
package com.shubh.restaurant_service.repository;

import com.shubh.restaurant_service.entity.Restaurant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RestaurantRepository extends JpaRepository<Restaurant, String> {
    
    @Query("SELECT r FROM Restaurant r WHERE r.id = :id AND r.isDeleted = false")
    Optional<Restaurant> findById(@Param("id") String id);
    
    boolean existsByContactEmailAndIsDeletedFalse(String email);
    
    @Query("SELECT r FROM Restaurant r WHERE r.id = :id")
    Optional<Restaurant> findByIdIncludingDeleted(@Param("id") String id);
    
}
