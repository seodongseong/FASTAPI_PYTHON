"""
=============================================================================
FastAPI + Redis 기반 상품 추천 시스템
=============================================================================

이 애플리케이션은 다음과 같은 기능을 제공합니다:

1. Redis에 저장된 클릭 이벤트를 10초 간격으로 그룹화
2. Apriori 알고리즘을 사용한 연관규칙 분석
3. 상품 추천 API 제공

주요 구성 요소:
- FastAPI 웹 프레임워크
- Redis 데이터 저장소
- 연관규칙 분석 (mlxtend 라이브러리)

API 엔드포인트:
- GET  /                    : 기본 정보
- GET  /analytics/products  : 상품 분석 정보
- GET  /recommend/{product} : 상품 추천
- GET  /groups/info         : 그룹 정보
=============================================================================
"""

# 기본 라이브러리
from fastapi import FastAPI, HTTPException
import uvicorn
import logging
import json
import os
import threading
from datetime import datetime
from collections import defaultdict

# 데이터 분석 라이브러리
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

# Redis 라이브러리
import redis

# Kafka 라이브러리
from kafka import KafkaConsumer

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI 앱 인스턴스 생성
app = FastAPI(
    title="Product Recommendation API",
    description="Redis 기반 상품 추천 시스템",
    version="1.0.0"
)

# Redis 설정
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_KEY = 'click_events'

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = 'click-events'
KAFKA_GROUP_ID = 'fastapi-consumer-group'

# =============================================================================
# Kafka Consumer 함수들
# =============================================================================

def start_kafka_consumer():
    """Kafka Consumer를 백그라운드에서 실행"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest'
        )
        
        redis_client = get_redis_client()
        
        logger.info(f"Kafka Consumer 시작 - Topic: {KAFKA_TOPIC}, Group: {KAFKA_GROUP_ID}")
        
        for message in consumer:
            try:
                event_data = message.value
                # Redis에 저장 (한글 깨짐 방지)
                event_json = json.dumps(event_data, ensure_ascii=False)
                redis_client.lpush(REDIS_KEY, event_json)
                logger.info(f"Kafka → Redis 저장 완료: {event_data}")
            except Exception as e:
                logger.error(f"Kafka → Redis 저장 실패: {e}")
                
    except Exception as e:
        logger.error(f"Kafka Consumer 시작 실패: {e}")

def run_kafka_consumer():
    """Kafka Consumer를 별도 스레드에서 실행"""
    thread = threading.Thread(target=start_kafka_consumer)
    thread.daemon = True
    thread.start()
    logger.info("Kafka Consumer 스레드 시작")

# =============================================================================
# Redis 관련 함수들
# =============================================================================

def get_redis_client():
    """Redis 클라이언트 인스턴스를 반환"""
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        redis_client.ping()
        return redis_client
    except Exception as e:
        logger.error(f"Redis 연결 실패: {e}")
        return None

def load_events_from_redis():
    """Redis에서 클릭 이벤트들을 읽어오기"""
    try:
        redis_client = get_redis_client()
        if redis_client is None:
            return []
        
        events_json = redis_client.lrange(REDIS_KEY, 0, -1)
        events = []
        
        for event_json in events_json:
            try:
                event_data = json.loads(event_json)
                events.append(event_data)
            except json.JSONDecodeError as e:
                logger.error(f"JSON 파싱 오류: {e}")
                continue
        
        logger.info(f"Redis에서 {len(events)}개 이벤트 로드")
        return events
        
    except Exception as e:
        logger.error(f"Redis 이벤트 읽기 중 오류: {e}")
        return []

# =============================================================================
# 연관규칙 분석 함수들
# =============================================================================

def prepare_transaction_data(time_window=5):
    """Redis 데이터를 시간 간격으로 그룹화하여 거래 데이터 생성"""
    try:
        events = load_events_from_redis()
        
        if not events:
            logger.warning("Redis에 클릭 이벤트가 없습니다")
            return pd.DataFrame(), {}
        
        # 시간 간격으로 그룹화
        time_based_groups = defaultdict(list)
        group_info = {}
        
        for event in events:
            product_name = event.get('product_name', 'unknown')
            timestamp = event.get('timestamp', [])
            
            # 타임스탬프를 초 단위로 변환
            if len(timestamp) >= 6:
                year, month, day, hour, minute, second = timestamp[:6]
                # 초 단위로 그룹화 (기본 5초)
                time_group = (second // time_window) * time_window
                group_id = f"{year}-{month:02d}-{day:02d}_{hour:02d}-{minute:02d}-{time_group:02d}"
            else:
                group_id = "unknown"
            
            # 그룹에 상품 추가
            time_based_groups[group_id].append({
                'product_name': product_name,
                'category': event.get('category', 'unknown'),
                'price': event.get('price', 0),
                'timestamp': timestamp
            })
        
        # 그룹 정보 정리
        groups = []
        for group_id, group_events in time_based_groups.items():
            # 중복 제거
            unique_products = []
            seen_products = set()
            
            for event in group_events:
                product_name = event['product_name']
                if product_name not in seen_products:
                    unique_products.append(product_name)
                    seen_products.add(product_name)
            
            groups.append(unique_products)
            
            # 그룹 정보 저장
            group_info[group_id] = {
                'products': [
                    {
                        'product_name': product,
                        'category': next((e['category'] for e in group_events if e['product_name'] == product), 'unknown'),
                        'price': next((e['price'] for e in group_events if e['product_name'] == product), 0)
                    }
                    for product in unique_products
                ],
                'total_events': len(group_events),
                'unique_products': len(unique_products),
                'time_window': time_window
            }
        
        logger.info(f"{time_window}초 간격 그룹화 완료: {len(groups)}개 그룹")
        
        if len(groups) < 2:
            logger.warning(f"그룹이 부족합니다: {len(groups)}개")
        
        # One-Hot Encoding 수행
        transaction_data = []
        transaction_ids = []
        
        for i, group_products in enumerate(groups):
            if len(group_products) > 0:
                transaction_ids.append(f"group_{i+1}")
                transaction_data.append(group_products)
        
        # DataFrame 생성
        oht = pd.DataFrame()
        for i, items in enumerate(transaction_data):
            row = {item: True for item in items}
            oht = pd.concat([oht, pd.DataFrame([row])], ignore_index=True)
        
        oht = oht.fillna(False)
        oht['TransactionID'] = transaction_ids
        oht = oht.set_index('TransactionID')
        oht = oht.astype(bool)
        
        logger.info(f"거래 데이터 변환 완료: {len(oht)}개 거래, {len(oht.columns)}개 상품")
        
        return oht, group_info
        
    except Exception as e:
        logger.error(f"거래 데이터 준비 중 오류: {e}")
        return pd.DataFrame(), {}

def generate_association_rules(min_support=0.1, min_confidence=0.3, time_window=5):
    """연관규칙 생성"""
    try:
        oht, group_info = prepare_transaction_data(time_window=time_window)
        
        if oht.empty:
            return {
                "error": "거래 데이터가 없습니다",
                "frequent_itemsets": pd.DataFrame(),
                "rules": pd.DataFrame(),
                "summary": {}
            }
        
        # 데이터 양에 따라 임계값 조정
        total_transactions = len(oht)
        if total_transactions < 5:
            adjusted_min_support = max(0.05, min_support * 0.5)
            adjusted_min_confidence = max(0.1, min_confidence * 0.5)
            logger.info(f"임계값 조정: 지지도 {min_support} → {adjusted_min_support}, 신뢰도 {min_confidence} → {adjusted_min_confidence}")
        else:
            adjusted_min_support = min_support
            adjusted_min_confidence = min_confidence
        
        # Apriori 알고리즘 실행
        frequent_itemsets = apriori(oht, min_support=adjusted_min_support, use_colnames=True)
        
        if frequent_itemsets.empty:
            return {
                "error": "빈발 항목 집합이 없습니다",
                "frequent_itemsets": frequent_itemsets,
                "rules": pd.DataFrame(),
                "summary": {"total_transactions": len(oht), "total_products": len(oht.columns)}
            }
        
        # 연관규칙 도출
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=adjusted_min_confidence)
        
        if not rules.empty:
            rules = rules.sort_values(['confidence', 'lift'], ascending=[False, False])
        
        summary = {
            "total_transactions": len(oht),
            "total_products": len(oht.columns),
            "frequent_itemsets_count": len(frequent_itemsets),
            "rules_count": len(rules),
            "min_support": adjusted_min_support,
            "min_confidence": adjusted_min_confidence,
            "top_products": list(oht.sum().sort_values(ascending=False).head(5).index)
        }
        
        logger.info(f"연관규칙 분석 완료: {len(frequent_itemsets)}개 빈발 항목, {len(rules)}개 규칙")
        
        return {
            "frequent_itemsets": frequent_itemsets,
            "rules": rules,
            "summary": summary,
            "group_info": group_info
        }
        
    except Exception as e:
        logger.error(f"연관규칙 생성 중 오류: {e}")
        return {
            "error": f"연관규칙 생성 중 오류: {str(e)}",
            "frequent_itemsets": pd.DataFrame(),
            "rules": pd.DataFrame(),
            "summary": {}
        }

def recommend_products(product_name, min_confidence=0.3, max_recommendations=5, time_window=5):
    """특정 상품을 기반으로 추천 상품 반환"""
    try:
        rules_result = generate_association_rules(min_confidence=min_confidence, time_window=time_window)
        
        if "error" in rules_result:
            return {
                "error": rules_result["error"],
                "recommendations": [],
                "product_name": product_name
            }
        
        rules = rules_result["rules"]
        
        if rules.empty:
            return {
                "message": f"'{product_name}' 상품에 대한 추천 규칙이 없습니다",
                "recommendations": [],
                "product_name": product_name
            }
        
        # 해당 상품이 전제에 포함된 규칙 찾기
        recommendations = []
        
        for _, rule in rules.iterrows():
            antecedents = list(rule['antecedents'])
            consequents = list(rule['consequents'])
            
            # 해당 상품이 전제에 포함된 경우
            if product_name in antecedents and product_name not in consequents:
                for consequent in consequents:
                    recommendations.append({
                        "product": consequent,
                        "confidence": round(rule['confidence'], 3),
                        "lift": round(rule['lift'], 3),
                        "support": round(rule['support'], 3),
                        "rule": f"{product_name} → {consequent}"
                    })
        
        # 만약 해당 상품에 대한 직접적인 규칙이 없다면, 모든 규칙에서 추천
        if len(recommendations) == 0 and len(rules) > 0:
            logger.info(f"'{product_name}'에 대한 직접 규칙이 없어 일반 추천을 제공합니다")
            
            # 모든 규칙에서 상위 추천 제공
            for _, rule in rules.iterrows():
                antecedents = list(rule['antecedents'])
                consequents = list(rule['consequents'])
                
                # 자기 자신이 아닌 상품들 추천
                for consequent in consequents:
                    if consequent != product_name:
                        recommendations.append({
                            "product": consequent,
                            "confidence": round(rule['confidence'], 3),
                            "lift": round(rule['lift'], 3),
                            "support": round(rule['support'], 3),
                            "rule": f"{', '.join(antecedents)} → {consequent}"
                        })
                        
                        if len(recommendations) >= max_recommendations:
                            break
                
                if len(recommendations) >= max_recommendations:
                    break
        
        # 신뢰도 기준으로 정렬하고 최대 개수만큼 반환
        recommendations = sorted(recommendations, key=lambda x: x['confidence'], reverse=True)[:max_recommendations]
        
        logger.info(f"'{product_name}' 상품 추천 완료: {len(recommendations)}개 추천")
        
        return {
            "product_name": product_name,
            "recommendations": recommendations,
            "total_recommendations": len(recommendations),
            "min_confidence": min_confidence,
            "learning_groups": rules_result.get("group_info", {}),
            "summary": rules_result.get("summary", {})
        }
        
    except Exception as e:
        logger.error(f"상품 추천 중 오류: {e}")
        return {
            "error": f"상품 추천 중 오류: {str(e)}",
            "recommendations": [],
            "product_name": product_name
        }

# =============================================================================
# API 엔드포인트들
# =============================================================================

@app.get("/")
async def root():
    """기본 정보"""
    return {"message": "Product Recommendation API에 오신 것을 환영합니다!"}

@app.get("/analytics/products")
async def get_product_analytics():
    """상품 분석 정보"""
    logger.info("상품 분석 요청")
    
    try:
        events = load_events_from_redis()
        
        if not events:
            return {
                "message": "분석할 클릭 이벤트가 없습니다",
                "total_products": 0,
                "total_clicks": 0,
                "top_products": [],
                "category_distribution": {},
                "price_statistics": {}
            }
        
        # 상품별 통계 수집
        product_stats = defaultdict(lambda: {"clicks": 0, "categories": set(), "prices": []})
        category_stats = defaultdict(int)
        all_prices = []
        
        for event in events:
            # Redis 데이터는 직접 필드에 접근
            product_name = event.get('product_name', 'unknown')
            category = event.get('category', 'unknown')
            price = event.get('price', 0)
            
            product_stats[product_name]["clicks"] += 1
            product_stats[product_name]["categories"].add(category)
            product_stats[product_name]["prices"].append(price)
            
            category_stats[category] += 1
            
            if price > 0:
                all_prices.append(price)
        
        # 상품별 통계 정리
        top_products = []
        for product, stats in product_stats.items():
            avg_price = sum(stats["prices"]) / len(stats["prices"]) if stats["prices"] else 0
            top_products.append({
                "product_name": product,
                "clicks": stats["clicks"],
                "categories": list(stats["categories"]),
                "average_price": round(avg_price, 2),
                "price_range": {
                    "min": min(stats["prices"]) if stats["prices"] else 0,
                    "max": max(stats["prices"]) if stats["prices"] else 0
                }
            })
        
        # 클릭 수 기준으로 정렬
        top_products = sorted(top_products, key=lambda x: x["clicks"], reverse=True)
        
        # 가격 통계
        price_stats = {}
        if all_prices:
            price_stats = {
                "average": round(sum(all_prices) / len(all_prices), 2),
                "min": min(all_prices),
                "max": max(all_prices),
                "count": len(all_prices)
            }
        
        logger.info(f"상품 분석 완료: {len(product_stats)}개 상품, {len(events)}개 클릭")
        
        return {
            "total_products": len(product_stats),
            "total_clicks": len(events),
            "top_products": top_products[:10],
            "category_distribution": dict(category_stats),
            "price_statistics": price_stats
        }
        
    except Exception as e:
        logger.error(f"상품 분석 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"상품 분석 중 오류: {str(e)}")

@app.get("/recommend/{product_name}")
async def get_product_recommendations(
    product_name: str,
    min_confidence: float = 0.3,
    max_recommendations: int = 5,
    time_window: int = 5
):
    """상품 추천"""
    logger.info(f"상품 추천 요청: {product_name}, 신뢰도: {min_confidence}, 시간간격: {time_window}초")
    
    result = recommend_products(product_name, min_confidence, max_recommendations, time_window)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return result

@app.get("/groups/info")
async def get_groups_info(time_window: int = 5):
    """그룹 정보 조회"""
    try:
        oht, group_info = prepare_transaction_data(time_window=time_window)
        
        if oht.empty:
            return {
                "error": "그룹 데이터가 없습니다",
                "total_groups": 0,
                "groups": [],
                "group_types": {}
            }
        
        # 그룹 정보 수집
        groups_info = []
        
        for group_id, info in group_info.items():
            products = info.get('products', [])
            product_names = [p.get('product_name', 'unknown') for p in products]
            
            groups_info.append({
                "group_id": group_id,
                "group_type": "time",
                "product_count": len(product_names),
                "products": product_names,
                "categories": list(set([p.get('category', 'unknown') for p in products if p.get('category')]))
            })
        
        # 그룹 타입별 통계
        group_types = {
            "session_groups": 0,
            "time_groups": len(groups_info),
            "total_groups": len(groups_info)
        }
        
        logger.info(f"그룹 정보 조회 완료: {len(groups_info)}개 그룹")
        
        return {
            "total_groups": len(groups_info),
            "groups": groups_info,
            "group_types": group_types,
            "summary": {
                "total_transactions": len(oht),
                "total_products": len(oht.columns)
            }
        }
        
    except Exception as e:
        logger.error(f"그룹 정보 조회 중 오류: {e}")
        return {
            "error": f"그룹 정보 조회 중 오류: {str(e)}",
            "total_groups": 0,
            "groups": [],
            "group_types": {}
        }

# FastAPI 이벤트 핸들러
@app.on_event("startup")
async def startup_event():
    """FastAPI 시작 시 Kafka Consumer 실행"""
    run_kafka_consumer()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)