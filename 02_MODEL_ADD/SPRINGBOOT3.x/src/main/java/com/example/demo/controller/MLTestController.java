package com.example.demo.controller;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.HashMap;

@Controller
@Slf4j
public class MLTestController {

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String FASTAPI_BASE_URL = "http://localhost:8000"; // FastAPI 서버 주소

    @GetMapping("/")
    public String home(Model model){
        // main.py에서 가져온 API 정보를 모델에 추가
        model.addAttribute("apiTitle", "소득 예측 API");
        model.addAttribute("apiDescription", "개인 정보를 기반으로 연간 소득을 예측하는 API");
        model.addAttribute("apiVersion", "1.0.0");
        model.addAttribute("welcomeMessage", "소득 예측 API에 오신 것을 환영합니다!");

        // 예제 데이터 (main.py의 InputData 예제)
        model.addAttribute("exampleData", new ExampleData(
                38, 0, 200000, 12, 10, 0, 4, 2, 0, 1, 0, 0, 40.0, 0
        ));

        return "index";
    }

    @PostMapping("/predict")
    @ResponseBody
    public ResponseEntity<?> predict(@ModelAttribute PredictionRequest request, Model model) {
        try {
            log.info("예측 요청 받음: {}", request);

            // FastAPI로 요청할 데이터 구성
            Map<String, Object> requestData = new HashMap<>();
            requestData.put("age", request.getAge());
            requestData.put("workclass", request.getWorkclass());
            requestData.put("fnlwgt", request.getFnlwgt());
            requestData.put("education", request.getEducation());
            requestData.put("education_num", request.getEducationNum());
            requestData.put("marital_status", request.getMaritalStatus());
            requestData.put("occupation", request.getOccupation());
            requestData.put("relationship", request.getRelationship());
            requestData.put("race", request.getRace());
            requestData.put("sex", request.getSex());
            requestData.put("capital_gain", request.getCapitalGain());
            requestData.put("capital_loss", request.getCapitalLoss());
            requestData.put("hours_per_week", request.getHoursPerWeek());
            requestData.put("native_country", request.getNativeCountry());

            // FastAPI 호출
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestData, headers);

            ResponseEntity<Map> response = restTemplate.postForEntity(
                    FASTAPI_BASE_URL + "/predict",
                    entity,
                    Map.class
            );

            log.info("FastAPI 응답: {}", response.getBody());

            return ResponseEntity.ok(response.getBody());

        } catch (Exception e) {
            log.error("예측 중 오류 발생", e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put("error", "예측 중 오류가 발생했습니다: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
    // 예제 데이터 클래스
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExampleData {
        private int age;
        private int workclass;
        private int fnlwgt;
        private int education;
        private int educationNum;
        private int maritalStatus;
        private int occupation;
        private int relationship;
        private int race;
        private int sex;
        private int capitalGain;
        private int capitalLoss;
        private double hoursPerWeek;
        private int nativeCountry;
    }
    // 예측 요청 데이터 클래스
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PredictionRequest {
        private int age;
        private int workclass;
        private int fnlwgt;
        private int education;
        private int educationNum;
        private int maritalStatus;
        private int occupation;
        private int relationship;
        private int race;
        private int sex;
        private int capitalGain;
        private int capitalLoss;
        private double hoursPerWeek;
        private int nativeCountry;
    }
}