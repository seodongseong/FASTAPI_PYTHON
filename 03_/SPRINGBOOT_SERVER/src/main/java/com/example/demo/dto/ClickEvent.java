package com.example.demo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClickEvent {
    
    @JsonProperty("event_type")
    private String eventType = "product_click";
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("product_id")
    private Long productId;
    
    @JsonProperty("product_name")
    private String productName;
    
    @JsonProperty("category")
    private String category;
    
    @JsonProperty("price")
    private Integer price;
    
    @JsonProperty("timestamp")
    private LocalDateTime timestamp;
    
    @JsonProperty("session_id")
    private String sessionId;
    
    public ClickEvent(Long productId, String productName, String category, Integer price) {
        this.productId = productId;
        this.productName = productName;
        this.category = category;
        this.price = price;
        this.timestamp = LocalDateTime.now();
        this.userId = "anonymous"; // 실제 구현에서는 인증된 사용자 ID 사용
        this.sessionId = "session_" + System.currentTimeMillis();
    }
}
