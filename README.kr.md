# Semantic Model Weaver

> Snowflake 데이터베이스를 읽어 Cortex Analyst용 시맨틱 모델을 자동 생성하고, 합성된 자연어 시나리오로 테스트하며, TruLens로 품질을 평가하고, 사람이 직접 작성하지 않아도 모델을 반복 개선하는 에이전틱 파이프라인.
>
> Snowflake Hackathon 2026 Seoul

## 문제

Cortex Analyst 시맨틱 YAML을 직접 손으로 작성하는 일은:
- **수동** — 스키마를 읽고, 동의어를 추측하고, 조인을 직접 연결해야 함
- **맹목적** — 누군가 실제로 질문하기 전까지 모델이 제대로 동작하는지 알 수 없음
- **검증 불가** — "이 모델이 얼마나 좋은가"를 측정할 지표가 없음

Semantic Model Weaver는 이 세 가지 문제를 모두 해결합니다.

## 작동 방식

```
[SchemaDiscovery]
    Snowpark가 INFORMATION_SCHEMA를 읽고, 컬럼을 프로파일링하며,
    데이터를 샘플링하고, 이름/타입 매칭으로 FK 후보를 추론.
        ↓
[YAMLWriter]
    Cortex Arctic이 Cortex Analyst 규격을 따르는 시맨틱 YAML 초안 작성:
    테이블, 차원, 측정값, 시간 차원, 동의어, 조인.
        ↓
[ScenarioGenerator]
    LLM이 테이블 클러스터별 자연어 질문 ~20개 생성
    (조인, 집계, 필터 포함). 원본 테이블을 직접 쿼리해 정답 구축.
        ↓
[CortexAnalystProbe]
    Cortex Analyst REST API에 초안 YAML과 함께 각 질문 전송.
    생성된 SQL, 실행 성공 여부, 조인 수, 결과 형태, 답변 수집.
        ↓
[EvalLogger + TruLens]
    TruLens(Snowflake 인큐베이팅 OSS, Snowflake에 로깅)가 각 질문 채점:
    근거 충실성, 관련성, 정답과의 일치도.
    정답 = 수작업 YAML이 아닌 원본 테이블 직접 쿼리.
    TruLens 대시보드가 기본 UI.
        ↓
[RefinementAgent]
    실패 리포트를 읽고 YAML 패치(누락된 동의어, 잘못된 조인,
    매핑되지 않은 측정값), Probe로 루프백.
```

## 정답(Ground Truth)

정답은 Cortex Analyst와 독립적으로 실제 Snowflake 테이블에 정확한 SQL을 직접 실행해 산출합니다. 평가가 객관적입니다: 생성된 시맨틱 모델이 실제 데이터에 대해 올바르게 답변하거나, 그렇지 않거나 둘 중 하나입니다.

## UI

**TruLens 대시보드**가 기본 인터페이스입니다 — 실험 추적, 질문별 점수, 실행 비교를 바로 제공합니다. TruLens는 Snowflake 인큐베이팅 OSS이며 모든 실행을 Snowflake 테이블에 기록합니다.

Streamlit in Snowflake 래퍼는 향후 개선 사항으로 `examples/streamlit-on-snowflake/`에 문서화되어 있습니다.

## 벤치마크 데이터셋

해커톤 마켓플레이스 데이터셋은 도메인이 아닌 테스트 대상입니다:

| 데이터베이스 | 데이터셋 | 검증 항목 |
|---|---|---|
| `NEXTRADE_EQUITY_MARKET_DATA` | Nextrade — 한국 주식시장 | 다중 테이블 조인, 시계열 측정값 |
| `KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA` | Richgo — 아파트 시세 및 인구 이동 | 지역 + 가격 차원 |
| `SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS` | SPH — 생활인구·소비·자산 | 광폭 테이블, 다수 측정값 |
| `SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION` | 아정당 — 통신 계약 | 지역 FK 추론 |

## 기술 스택

| 레이어 | 기술 |
|---|---|
| 스키마 탐색 | Snowflake Snowpark (`INFORMATION_SCHEMA`) |
| 모델 생성 | Snowflake Cortex Arctic |
| 시맨틱 쿼리 테스트 | Cortex Analyst REST API |
| 평가 및 채점 | TruLens (Snowflake 인큐베이팅, Snowflake에 로깅) |
| 언어 | Python |
| 패키지 매니저 | uv |

## 프로젝트 구조

```
.
├── pyproject.toml
├── CLAUDE.md
├── README.md
├── weaver/
│   ├── __main__.py         # CLI 진입점: python -m weaver --database ... --schema ...
│   ├── discovery.py        # SchemaDiscovery — Snowpark 스키마 프로파일링
│   ├── writer.py           # YAMLWriter — LLM 기반 시맨틱 YAML 생성
│   ├── scenarios.py        # ScenarioGenerator — 자연어 질문 및 정답 합성
│   ├── probe.py            # CortexAnalystProbe — REST API 테스트 하네스
│   ├── logger.py           # EvalLogger — 지표 수집
│   ├── evaluator.py        # TruLens 채점
│   └── refiner.py          # RefinementAgent — YAML 패치 루프
└── examples/
    └── streamlit-on-snowflake/
        ├── README.md           # 향후: Streamlit in Snowflake 래퍼
        └── manifest/
            └── nti_model.yaml  # 수작업 시맨틱 YAML (피벗 이전 NTI 앱)
```

## 시작하기

```bash
# 1. 인증 정보 입력
cp .env.example .env
# .env 편집 — WEAVER_SNOWFLAKE_* 및 WEAVER_ALLOWED_IP(외부 IP) 설정
# IP 확인: curl -s https://checkip.amazonaws.com
```

## 사용법

```bash
# 최초 실행 — 워크스페이스 데이터베이스, 스키마, 네트워크 정책 생성
uv run python -m weaver --setup

# 한국 주식시장 (Nextrade)
uv run python -m weaver --database NEXTRADE_EQUITY_MARKET_DATA --schema FIN

# 아파트 시세 및 인구 이동 (Richgo)
uv run python -m weaver --database KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA --schema HACKATHON_2025Q2

# 생활인구·소비·자산 (SPH)
uv run python -m weaver --database SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS --schema GRANDATA

# 통신 구독 분석 (아정당)
uv run python -m weaver --database SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION --schema TELECOM_INSIGHTS

# 반복 횟수 및 버전 태그 지정 실행
uv run python -m weaver --database NEXTRADE_EQUITY_MARKET_DATA --schema FIN --iterations 5 --version v2

# 체크포인트 디렉터리에서 이전 실행 재개 (단계는 아티팩트로 자동 감지)
uv run python -m weaver --resume manifest/NEXTRADE_EQUITY_MARKET_DATA.FIN/20260406_202959/

# 모든 평가 기록 삭제 후 초기화
uv run python -m weaver --reset-workspace
uv run python -m weaver --reset-workspace --yes   # 확인 프롬프트 건너뜀
```

실행 완료 후 **Snowsight → AI & ML → Evaluations**에서 결과를 확인하세요.

## 채팅 앱

각 데이터셋에는 생성된 시맨틱 YAML을 Cortex Analyst에 전송하고 결과를 인터랙티브 테이블과 차트로 렌더링하는 독립형 Streamlit 채팅 앱이 있습니다.

먼저 파이프라인 실행에서 최종 모델을 앱 디렉터리로 복사하세요:

```bash
cp manifest/NEXTRADE_EQUITY_MARKET_DATA.FIN/<timestamp>/model.final.yaml                                                                    examples/nextrade/model.yaml
cp manifest/KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA.HACKATHON_2025Q2/<timestamp>/model.final.yaml                                    examples/korean_population/model.yaml
cp manifest/SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA/<timestamp>/model.final.yaml                       examples/seoul_floating_population/model.yaml
cp manifest/SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS/<timestamp>/model.final.yaml  examples/telecom/model.yaml
```

그런 다음 앱을 실행하세요:

```bash
# 📈 넥스트레이드 — 한국 주식시장 데이터
uv run streamlit run examples/nextrade/app.py
       
# 🏘️ Richgo — 한국 아파트 시세 및 인구 이동 데이터
uv run streamlit run examples/korean_population/app.py

# 🗺️ SPH / GranData — 서울 생활인구·소비·자산 데이터
uv run streamlit run examples/seoul_floating_population/app.py

# 📡 아정당 — 통신 계약·마케팅·콜센터 지역별 분석 데이터
uv run streamlit run examples/telecom/app.py
```

또는 복사 없이 모델 경로를 직접 지정:

```bash
WEAVER_MODEL_YAML=manifest/NEXTRADE_EQUITY_MARKET_DATA.FIN/<timestamp>/model.final.yaml \
  uv run --project examples/nextrade streamlit run examples/nextrade/app.py
```

### 컨테이너로 실행

`main` 브랜치에 푸시할 때마다 GitHub Container Registry에 사전 빌드된 이미지가 게시됩니다.

채팅 앱 이미지 Pull 및 실행 (`<app>`을 `nextrade`, `korean-population`, `seoul-floating-population`, `telecom` 중 하나로 교체):

```bash
docker run -p 8501:8501 \
  -e WEAVER_SNOWFLAKE_ACCOUNT=<your_account> \
  -e WEAVER_SNOWFLAKE_USER=<your_user> \
  -e WEAVER_SNOWFLAKE_PASSWORD=<your_password> \
  -e WEAVER_SNOWFLAKE_ROLE=ACCOUNTADMIN \
  -e WEAVER_SNOWFLAKE_WAREHOUSE=COMPUTE_WH \
  ghcr.io/cynicdog/semantic-model-weaver/<app>:latest
```

브라우저에서 `http://localhost:8501`을 열면 됩니다.

컨테이너에서 weaver 파이프라인 CLI 실행:

```bash
docker run --rm \
  -e WEAVER_SNOWFLAKE_ACCOUNT=<your_account> \
  -e WEAVER_SNOWFLAKE_USER=<your_user> \
  -e WEAVER_SNOWFLAKE_PASSWORD=<your_password> \
  -e WEAVER_SNOWFLAKE_ROLE=ACCOUNTADMIN \
  -e WEAVER_SNOWFLAKE_WAREHOUSE=COMPUTE_WH \
  -e WEAVER_SNOWFLAKE_DATABASE=semantic_model_weaver \
  -e WEAVER_SNOWFLAKE_SCHEMA=trulens \
  -v $(pwd)/manifest:/app/manifest \
  ghcr.io/cynicdog/semantic-model-weaver/weaver-cli:latest \
  --database NEXTRADE_EQUITY_MARKET_DATA --schema FIN
```

`./manifest`를 마운트하면 파이프라인 출력(YAML 스냅샷, 시나리오)이 호스트에 저장됩니다.

로컬에서 이미지 빌드:

```bash
# Weaver CLI
docker build -t weaver-cli .

# 채팅 앱 (빌드 컨텍스트는 예제 디렉터리)
docker build -t nextrade examples/nextrade
docker build -t korean-population examples/korean_population
docker build -t seoul-floating-population examples/seoul_floating_population
docker build -t telecom examples/telecom
```

## 테스트

두 계층으로 구성 — 단위 테스트는 어디서나 실행 가능, 통합 테스트는 실제 Snowflake 세션 필요.

### 단위 테스트

Snowflake 연결 불필요. DSL 구조, 직렬화, 모든 유효성 검사기를 검증합니다.

```bash
uv run pytest tests/test_dsl.py -v
```

검증 항목:
- `examples/streamlit-on-snowflake/manifest/nti_model.yaml` 파싱 (실제 Cortex Analyst YAML)
- 왕복 충실도: `from_yaml(to_yaml(model))`이 동등한 모델을 생성하는지
- YAML 출력 규칙: `schema` 별칭, 빈 리스트/문자열 제거, `default_aggregation` 없음
- 유효성 검사기: `TimeDimension`이 비시간형 타입 거부, `Relationship`이 잘못된 테이블 참조 거부

### 통합 테스트

생성된 YAML을 실제 Cortex Analyst REST API에 전송합니다. 환경에 `SNOWSQL_PWD`가 필요합니다(`~/.zshrc`에 이미 설정됨).

```bash
uv run pytest tests/test_cortex_analyst_api.py -v -m integration
```

검증 항목:
- weaver가 생성한 모델이 API에 수락되는지 (200, 에러 없음)
- 간단한 카운트 질문이 `sql` 응답 타입을 반환하는지
- 의도적으로 잘못된 YAML이 API 에러를 발생시키는지 — 수락 테스트가 유의미함을 증명
- `nti_model.yaml`이 DSL 왕복 후에도 API에 수락되는지

### 전체 실행

```bash
uv run pytest -v                    # 단위 테스트만 (통합 테스트는 자격 증명 없으면 건너뜀)
uv run pytest -v -m integration     # 통합 테스트만
uv run pytest -v -m "not integration" # 명시적으로 단위 테스트만
```

## 팀

- CynicDog (이은상) — 데이터 엔지니어, MetLife Korea
