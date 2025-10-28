from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import joblib
import pandas as pd
from typing import Optional
import uvicorn

# FastAPI 앱 생성
app = FastAPI(
    title="소득 예측 API",
    description="개인 정보를 기반으로 연간 소득을 예측하는 API",
    version="1.0.0"
)

# CORS 설정 (프론트엔드에서 접근 가능하도록)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 모델 로드 (서버 시작 시 한 번만 로드)
try:
    model = joblib.load("model.pkl")
    print("모델 로드 완료")
    print(f"학습된 특징: {model.feature_names_in_}")
except Exception as e:
    print(f"모델 로드 실패: {e}")
    model = None


# 입력 데이터 스키마 정의
class InputData(BaseModel):
    age: int = Field(..., ge=17, le=90, description="나이 (17-90)")
    workclass: int = Field(..., ge=0, le=7, description="근무 형태 (0-7)")
    fnlwgt: int = Field(..., ge=0, description="인구 가중치")
    education: int = Field(..., ge=0, le=15, description="최종 학력 (0-15)")
    education_num: int = Field(..., ge=1, le=16, description="교육 연수 (1-16)")
    marital_status: int = Field(..., ge=0, le=6, description="결혼 상태 (0-6)")
    occupation: int = Field(..., ge=0, le=14, description="직업 (0-14)")
    relationship: int = Field(..., ge=0, le=5, description="가족 관계 (0-5)")
    race: int = Field(..., ge=0, le=4, description="인종 (0-4)")
    sex: int = Field(..., ge=0, le=1, description="성별 (0: 여성, 1: 남성)")
    capital_gain: int = Field(..., ge=0, description="자본 이득")
    capital_loss: int = Field(..., ge=0, description="자본 손실")
    hours_per_week: float = Field(..., ge=1, le=100, description="주당 근무 시간")
    native_country: int = Field(..., ge=0, le=40, description="출신 국가 (0-40)")

    class Config:
        schema_extra = {
            "example": {
                "age": 38,
                "workclass": 0,
                "fnlwgt": 200000,
                "education": 12,
                "education_num": 10,
                "marital_status": 0,
                "occupation": 4,
                "relationship": 2,
                "race": 0,
                "sex": 1,
                "capital_gain": 0,
                "capital_loss": 0,
                "hours_per_week": 40,
                "native_country": 0
            }
        }


# 예측 결과 스키마 정의
class PredictionResponse(BaseModel):
    prediction: str = Field(..., description="예측된 소득 구간 (<=50K 또는 >50K)")
    probability_low: float = Field(..., description="<=50K 확률")
    probability_high: float = Field(..., description=">50K 확률")
    confidence: float = Field(..., description="예측 신뢰도 (최대 확률값)")
    interpretation: str = Field(..., description="결과 해석")


# 루트 엔드포인트
@app.get("/")
def read_root():
    return {
        "message": "소득 예측 API에 오신 것을 환영합니다!",
        "docs": "/docs",
        "health": "/health"
    }


# 헬스 체크 엔드포인트
@app.get("/health")
def health_check():
    if model is None:
        raise HTTPException(status_code=503, detail="모델이 로드되지 않았습니다")
    return {
        "status": "healthy",
        "model_loaded": True,
        "features": len(model.feature_names_in_)
    }


# 예측 엔드포인트
@app.post("/predict", response_model=PredictionResponse)
def predict(data: InputData):
    """
    개인 정보를 받아 연간 소득을 예측합니다.
    
    - **age**: 나이
    - **workclass**: 근무 형태
    - **fnlwgt**: 인구 가중치
    - **education**: 최종 학력
    - **education_num**: 교육 연수
    - **marital_status**: 결혼 상태
    - **occupation**: 직업
    - **relationship**: 가족 관계
    - **race**: 인종
    - **sex**: 성별
    - **capital_gain**: 자본 이득
    - **capital_loss**: 자본 손실
    - **hours_per_week**: 주당 근무 시간
    - **native_country**: 출신 국가
    """
    
    if model is None:
        raise HTTPException(status_code=503, detail="모델이 로드되지 않았습니다")
    
    try:
        # 입력 데이터를 DataFrame으로 변환 (컬럼명에 점 포함)
        input_dict = {
            'age': data.age,
            'workclass': data.workclass,
            'fnlwgt': data.fnlwgt,
            'education': data.education,
            'education.num': data.education_num,
            'marital.status': data.marital_status,
            'occupation': data.occupation,
            'relationship': data.relationship,
            'race': data.race,
            'sex': data.sex,
            'capital.gain': data.capital_gain,
            'capital.loss': data.capital_loss,
            'hours.per.week': data.hours_per_week,
            'native.country': data.native_country
        }
        
        df = pd.DataFrame([input_dict])
        
        # 예측 수행
        prediction_class = model.predict(df)[0]
        probabilities = model.predict_proba(df)[0]
        
        # 결과 해석
        prob_high = float(probabilities[1])
        confidence = float(max(probabilities))
        
        if prob_high > 0.7:
            interpretation = "연간 소득이 $50,000를 초과할 가능성이 매우 높습니다."
        elif prob_high > 0.5:
            interpretation = "연간 소득이 $50,000를 초과할 가능성이 높습니다."
        else:
            interpretation = "연간 소득이 $50,000 이하일 가능성이 높습니다."
        
        return PredictionResponse(
            prediction="<=50K" if prediction_class == 0 else ">50K",
            probability_low=float(probabilities[0]),
            probability_high=float(probabilities[1]),
            confidence=confidence,
            interpretation=interpretation
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"예측 중 오류 발생: {str(e)}")


# 배치 예측 엔드포인트
@app.post("/predict/batch")
def predict_batch(data_list: list[InputData]):
    """
    여러 개의 데이터를 한 번에 예측합니다.
    """
    
    if model is None:
        raise HTTPException(status_code=503, detail="모델이 로드되지 않았습니다")
    
    try:
        results = []
        
        for data in data_list:
            input_dict = {
                'age': data.age,
                'workclass': data.workclass,
                'fnlwgt': data.fnlwgt,
                'education': data.education,
                'education.num': data.education_num,
                'marital.status': data.marital_status,
                'occupation': data.occupation,
                'relationship': data.relationship,
                'race': data.race,
                'sex': data.sex,
                'capital.gain': data.capital_gain,
                'capital.loss': data.capital_loss,
                'hours.per.week': data.hours_per_week,
                'native.country': data.native_country
            }
            
            df = pd.DataFrame([input_dict])
            prediction_class = model.predict(df)[0]
            probabilities = model.predict_proba(df)[0]
            
            results.append({
                "prediction": "<=50K" if prediction_class == 0 else ">50K",
                "probability_high": float(probabilities[1])
            })
        
        return {"results": results, "count": len(results)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"배치 예측 중 오류 발생: {str(e)}")


# 모델 정보 엔드포인트
@app.get("/model/info")
def model_info():
    """
    로드된 모델의 정보를 반환합니다.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="모델이 로드되지 않았습니다")
    
    return {
        "model_type": str(type(model).__name__),
        "features": model.feature_names_in_.tolist(),
        "n_features": len(model.feature_names_in_),
        "classes": model.classes_.tolist() if hasattr(model, 'classes_') else None
    }


# 서버 실행 (이 파일을 직접 실행할 때)
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True  # 개발 모드: 코드 변경 시 자동 재시작
    )