package com.example.demo.service;

import com.example.demo.dto.ClickEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaEventService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.click-events}")
    private String clickEventsTopic;

    public void sendClickEvent(ClickEvent clickEvent) {
        try {
            String eventJson = objectMapper.writeValueAsString(clickEvent);
            String key = clickEvent.getProductId().toString();
            
            kafkaTemplate.send(clickEventsTopic, key, eventJson)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("카프카 이벤트 전송 성공 - Product ID: {}, Topic: {}", 
                            clickEvent.getProductId(), clickEventsTopic);
                    } else {
                        log.error("카프카 이벤트 전송 실패 - Product ID: {}, Error: {}", 
                            clickEvent.getProductId(), ex.getMessage());
                    }
                });
                
        } catch (JsonProcessingException e) {
            log.error("이벤트 JSON 변환 실패: {}", e.getMessage());
        }
    }
}
