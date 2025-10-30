package com.example.demo.controller;


import com.example.demo.dto.ClickEvent;
import com.example.demo.service.KafkaEventService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Arrays;
import java.util.Optional;

@Controller
@RequestMapping("/item")
@Slf4j
@RequiredArgsConstructor
public class ItemController {

    private final KafkaEventService kafkaEventService;
    private final RestTemplate restTemplate;
    List<Map<String, Object>> items = new ArrayList<>();
    
    // FastAPI 서버 설정
    private static final String FASTAPI_BASE_URL = "http://localhost:8000";

    @PostConstruct
    public void init(){
        // 카테고리별 템플릿 데이터 정의
        String[][] categoryTemplates = {
            // 전자제품
            {"전자제품", "스마트폰", "150000,2500000", "/images/phone-{}.jpg"},
            {"전자제품", "노트북", "800000,3000000", "/images/laptop-{}.jpg"},
            {"전자제품", "태블릿", "400000,1500000", "/images/tablet-{}.jpg"},
            {"전자제품", "이어폰", "50000,300000", "/images/earphone-{}.jpg"},
            
            // 패션
            {"패션", "티셔츠", "15000,80000", "/images/tshirt-{}.jpg"},
            {"패션", "신발", "50000,500000", "/images/shoes-{}.jpg"},
            {"패션", "가방", "30000,4000000", "/images/bag-{}.jpg"},
            {"패션", "바지", "20000,200000", "/images/pants-{}.jpg"},
            
            // 가전
            {"가전", "TV", "500000,5000000", "/images/tv-{}.jpg"},
            {"가전", "냉장고", "800000,2000000", "/images/fridge-{}.jpg"},
            {"가전", "세탁기", "400000,1500000", "/images/washer-{}.jpg"},
            {"가전", "에어컨", "300000,1200000", "/images/aircon-{}.jpg"}
        };
        
        Random random = new Random();
        
        // 100개 상품 생성
        for (int i = 1; i <= 100; i++) {
            // 랜덤하게 카테고리 템플릿 선택
            String[] template = categoryTemplates[random.nextInt(categoryTemplates.length)];
            String category = template[0];
            String productType = template[1];
            String[] priceRange = template[2].split(",");
            String imageTemplate = template[3];
            
            // 가격 범위에서 랜덤 생성
            int minPrice = Integer.parseInt(priceRange[0]);
            int maxPrice = Integer.parseInt(priceRange[1]);
            int price = minPrice + random.nextInt(maxPrice - minPrice + 1);
            
            Map<String, Object> item = new HashMap<>();
            item.put("id", (long) i);
            item.put("category", category);
            item.put("productName", String.format("%s 브랜드 %s %d번", 
                category.equals("전자제품") ? 
                    Arrays.asList("삼성", "애플", "LG", "소니", "샤오미").get(random.nextInt(5)) :
                category.equals("패션") ? 
                    Arrays.asList("나이키", "아디다스", "유니클로", "ZARA", "구찌").get(random.nextInt(5)) :
                    Arrays.asList("삼성", "LG", "대우", "위니아", "캐리어").get(random.nextInt(5)), 
                productType, i));
            item.put("price", price);
            item.put("image", imageTemplate.replace("{}", String.valueOf(i)));
            item.put("description", String.format("고품질 %s %s입니다", category, productType));
            item.put("brand", category.equals("전자제품") ? 
                Arrays.asList("Samsung", "Apple", "LG", "Sony", "Xiaomi").get(random.nextInt(5)) :
                Arrays.asList("Nike", "Adidas", "Uniqlo", "ZARA", "Gucci").get(random.nextInt(5)));
            item.put("rating", String.format("%.1f", 3.0 + random.nextDouble() * 2.0)); // 3.0 ~ 5.0
            
            items.add(item);
        }

        log.info("총 {}개의 상품 데이터 생성 완료 (카테고리: 전자제품, 패션, 가전)", items.size());
    }


    @GetMapping("/list")
    public String list(Model model){
        log.info("GET /item/list... 상품 {}개 조회", items.size());
        model.addAttribute("items", items);
        return "item/list";
    }


    @GetMapping("/details")
    public String details(@RequestParam("id") Long id, Model model){
        log.info("GET /item/details?id={}", id);
        
        // ID로 상품 찾기
        Optional<Map<String, Object>> itemOptional = items.stream()
            .filter(item -> ((Long) item.get("id")).equals(id))
            .findFirst();
        
        if (itemOptional.isPresent()) {
            Map<String, Object> item = itemOptional.get();
            model.addAttribute("item", item);
            
            // Kafka 이벤트는 list.html에서 클릭 시에만 전송하므로 여기서는 제거
            log.info("상품 상세 정보 로드: {}", item.get("productName"));
            return "item/details";
        } else {
            log.warn("상품을 찾을 수 없습니다. ID: {}", id);
            model.addAttribute("error", "상품을 찾을 수 없습니다.");
            return "item/details";
        }
    }

    @PostMapping("/click")
    public ResponseEntity<String> handleProductClick(@RequestParam("id") Long id) {
        log.info("POST /item/click?id={}", id);
        
        // ID로 상품 찾기
        Optional<Map<String, Object>> itemOptional = items.stream()
            .filter(item -> ((Long) item.get("id")).equals(id))
            .findFirst();
        
        if (itemOptional.isPresent()) {
            Map<String, Object> item = itemOptional.get();
            sendClickEvent(item);
            return ResponseEntity.ok("클릭 이벤트 전송 완료");
        } else {
            log.warn("상품을 찾을 수 없습니다. ID: {}", id);
            return ResponseEntity.badRequest().body("상품을 찾을 수 없습니다.");
        }
    }

    private void sendClickEvent(Map<String, Object> item) {
        try {
            ClickEvent clickEvent = new ClickEvent(
                (Long) item.get("id"),
                (String) item.get("productName"),
                (String) item.get("category"),
                (Integer) item.get("price")
            );
            
            kafkaEventService.sendClickEvent(clickEvent);
            log.info("상품 클릭 이벤트 전송: {}", item.get("productName"));
        } catch (Exception e) {
            log.error("클릭 이벤트 전송 실패: {}", e.getMessage());
        }
    }

    // =============================================================================
    // 상품 추천 관련 엔드포인트들
    // =============================================================================

    @GetMapping("/recommend")
    public String recommendPage(Model model) {
        log.info("GET /item/recommend - 상품 추천 페이지");
        
        // 인기 상품 목록을 모델에 추가 (추천 기준으로 사용)
        List<Map<String, Object>> popularItems = items.stream()
            .sorted((a, b) -> {
                // 가격이 낮을수록 인기 상품으로 간주 (예시)
                Integer priceA = (Integer) a.get("price");
                Integer priceB = (Integer) b.get("price");
                return priceA.compareTo(priceB);
            })
            .limit(10)
            .toList();
        
        model.addAttribute("popularItems", popularItems);
        model.addAttribute("allItems", items);
        
        // FastAPI에서 통계 정보 가져오기
        try {
            String analyticsUrl = FASTAPI_BASE_URL + "/analytics/products";
            Map<String, Object> analyticsData = restTemplate.getForObject(analyticsUrl, Map.class);
            
            if (analyticsData != null) {
                log.info("통계 정보 로드 성공");
                model.addAttribute("analyticsData", analyticsData);
            } else {
                log.warn("통계 정보 로드 실패 - 응답이 null");
                model.addAttribute("analyticsData", Map.of("error", "통계 정보를 가져올 수 없습니다"));
            }
        } catch (Exception e) {
            log.error("통계 정보 로드 중 오류: {}", e.getMessage());
            model.addAttribute("analyticsData", Map.of("error", "통계 서비스에 연결할 수 없습니다: " + e.getMessage()));
        }
        
        return "item/recommand";
    }

    @GetMapping("/recommend/product")
    public ResponseEntity<Map<String, Object>> getProductRecommendations(
            @RequestParam("productName") String productName,
            @RequestParam(value = "minConfidence", defaultValue = "0.5") double minConfidence,
            @RequestParam(value = "maxRecommendations", defaultValue = "5") int maxRecommendations,
            @RequestParam(value = "timeWindow", defaultValue = "5") int timeWindow) {
        
        log.info("GET /item/recommend/product?productName={}&minConfidence={}&maxRecommendations={}&timeWindow={}", 
                productName, minConfidence, maxRecommendations, timeWindow);
        
        try {
            // FastAPI의 상품 추천 API 호출
            String url = String.format("%s/recommend/%s?min_confidence=%f&max_recommendations=%d&time_window=%d", 
                    FASTAPI_BASE_URL, productName, minConfidence, maxRecommendations, timeWindow);
            
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
            
            if (response != null) {
                log.info("상품 추천 API 호출 성공: {}", productName);
                return ResponseEntity.ok(response);
            } else {
                log.warn("상품 추천 API 응답이 null입니다: {}", productName);
                return ResponseEntity.badRequest().body(Map.of("error", "추천 결과를 가져올 수 없습니다"));
            }
            
        } catch (Exception e) {
            log.error("상품 추천 API 호출 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "추천 서비스에 연결할 수 없습니다: " + e.getMessage()));
        }
    }

    @GetMapping("/recommend/session")
    public ResponseEntity<Map<String, Object>> getSessionRecommendations(
            @RequestParam("sessionId") String sessionId,
            @RequestParam(value = "minConfidence", defaultValue = "0.5") double minConfidence,
            @RequestParam(value = "maxRecommendations", defaultValue = "5") int maxRecommendations) {
        
        log.info("GET /item/recommend/session?sessionId={}&minConfidence={}&maxRecommendations={}", 
                sessionId, minConfidence, maxRecommendations);
        
        try {
            // FastAPI의 세션 기반 추천 API 호출
            String url = String.format("%s/recommend/session/%s?min_confidence=%f&max_recommendations=%d", 
                    FASTAPI_BASE_URL, sessionId, minConfidence, maxRecommendations);
            
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
            
            if (response != null) {
                log.info("세션 기반 추천 API 호출 성공: {}", sessionId);
                return ResponseEntity.ok(response);
            } else {
                log.warn("세션 기반 추천 API 응답이 null입니다: {}", sessionId);
                return ResponseEntity.badRequest().body(Map.of("error", "세션 추천 결과를 가져올 수 없습니다"));
            }
            
        } catch (Exception e) {
            log.error("세션 기반 추천 API 호출 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "세션 추천 서비스에 연결할 수 없습니다: " + e.getMessage()));
        }
    }

    @GetMapping("/groups/info")
    public ResponseEntity<Map<String, Object>> getGroupsInfo(
            @RequestParam(value = "timeWindow", defaultValue = "5") int timeWindow) {
        log.info("GET /item/groups/info?timeWindow={} - 그룹 정보 조회", timeWindow);
        
        try {
            // FastAPI의 그룹 정보 API 호출
            String url = String.format("%s/groups/info?time_window=%d", FASTAPI_BASE_URL, timeWindow);
            
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
            
            if (response != null) {
                log.info("그룹 정보 API 호출 성공");
                return ResponseEntity.ok(response);
            } else {
                log.warn("그룹 정보 API 응답이 null입니다");
                return ResponseEntity.badRequest().body(Map.of("error", "그룹 정보를 가져올 수 없습니다"));
            }
            
        } catch (Exception e) {
            log.error("그룹 정보 API 호출 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "그룹 정보 서비스에 연결할 수 없습니다: " + e.getMessage()));
        }
    }

    @GetMapping("/association-rules")
    public ResponseEntity<Map<String, Object>> getAssociationRules(
            @RequestParam(value = "minSupport", defaultValue = "0.2") double minSupport,
            @RequestParam(value = "minConfidence", defaultValue = "0.5") double minConfidence,
            @RequestParam(value = "limit", defaultValue = "20") int limit) {
        
        log.info("GET /item/association-rules?minSupport={}&minConfidence={}&limit={}", 
                minSupport, minConfidence, limit);
        
        try {
            // FastAPI의 연관규칙 API 호출
            String url = String.format("%s/association-rules?min_support=%f&min_confidence=%f&limit=%d", 
                    FASTAPI_BASE_URL, minSupport, minConfidence, limit);
            
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
            
            if (response != null) {
                log.info("연관규칙 API 호출 성공");
                return ResponseEntity.ok(response);
            } else {
                log.warn("연관규칙 API 응답이 null입니다");
                return ResponseEntity.badRequest().body(Map.of("error", "연관규칙을 가져올 수 없습니다"));
            }
            
        } catch (Exception e) {
            log.error("연관규칙 API 호출 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "연관규칙 서비스에 연결할 수 없습니다: " + e.getMessage()));
        }
    }

    @GetMapping("/analytics")
    public ResponseEntity<Map<String, Object>> getAnalytics() {
        log.info("GET /item/analytics - 상품 분석 정보 조회");
        
        try {
            // FastAPI의 분석 API 호출
            String url = FASTAPI_BASE_URL + "/analytics/products";
            
            Map<String, Object> response = restTemplate.getForObject(url, Map.class);
            
            if (response != null) {
                log.info("분석 정보 API 호출 성공");
                return ResponseEntity.ok(response);
            } else {
                log.warn("분석 정보 API 응답이 null입니다");
                return ResponseEntity.badRequest().body(Map.of("error", "분석 정보를 가져올 수 없습니다"));
            }
            
        } catch (Exception e) {
            log.error("분석 정보 API 호출 실패: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "분석 정보 서비스에 연결할 수 없습니다: " + e.getMessage()));
        }
    }

}
