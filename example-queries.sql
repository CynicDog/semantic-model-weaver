-- ============================================================
-- Dataset 1: NEXTRADE_EQUITY_MARKET_DATA (Korean equity market)
-- ============================================================

-- 종목 기준정보 — 컬럼 어휘 사전
SELECT
    dwdd          AS "기준일",
    isu_cd        AS "종목코드",
    isu_abwd_nm   AS "종목약명",
    secu_grp_id   AS "증권그룹",
    lsst_ct       AS "상장주식수",
    capt_amt      AS "자본금",
    base_prc      AS "기준가",
    ulpr          AS "상한가",
    lwlp          AS "하한가",
    ind_id        AS "업종코드"
FROM nextrade_equity_market_data.fin.nx_ht_bat_refer_a0
LIMIT 1;

-- 체결 시세 — 컬럼 어휘 사전
SELECT
    dwdd          AS "기준일",
    mkt_id        AS "시장구분",
    isu_cd        AS "종목코드",
    td_prc        AS "체결가",
    trd_qty       AS "거래량",
    acc_trd_qty   AS "누적거래량",
    acc_trval     AS "누적거래대금",
    hgpr          AS "고가",
    lwpr          AS "저가",
    oppr          AS "시가"
FROM nextrade_equity_market_data.fin.nx_ht_onl_mktpr_a3
LIMIT 1;

-- 시장 등락 현황 — 컬럼 어휘 사전
SELECT
    dwdd          AS "기준일",
    mkt_id        AS "시장구분",
    fluc_up_ct    AS "상승종목수",
    fluc_dn_ct    AS "하락종목수",
    fluc_stdn_ct  AS "보합종목수",
    fluc_uplm_ct  AS "상한가종목수",
    fluc_lwlm_ct  AS "하한가종목수"
FROM nextrade_equity_market_data.fin.nx_ht_onl_stats_b5
LIMIT 1;

-- 프로그램 매매 — 컬럼 어휘 사전
SELECT
    dwdd          AS "기준일",
    mkt_id        AS "시장구분",
    isu_cd        AS "종목코드",
    ask_arb_qty   AS "매도차익거래량",
    bid_arb_qty   AS "매수차익거래량",
    ask_narb_qty  AS "매도비차익거래량",
    bid_narb_qty  AS "매수비차익거래량"
FROM nextrade_equity_market_data.fin.nx_ht_onl_stats_c3
LIMIT 1;

-- 시장별 일별 거래량 및 거래대금
SELECT
    dwdd         AS "기준일",
    mkt_id       AS "시장구분",
    SUM(trd_qty)   AS "거래량합계",
    SUM(acc_trval) AS "거래대금합계"
FROM nextrade_equity_market_data.fin.nx_ht_onl_mktpr_a3
GROUP BY dwdd, mkt_id
ORDER BY dwdd DESC, mkt_id;

-- KOSPI 시장 당일 누적거래량 상위 10 종목
SELECT
    isu_cd                AS "종목코드",
    MAX(acc_trd_qty)      AS "누적거래량",
    MAX(acc_trval)        AS "누적거래대금"
FROM nextrade_equity_market_data.fin.nx_ht_onl_mktpr_a3
WHERE dwdd = '2026-01-23' AND mkt_id = 'STK'
GROUP BY isu_cd
ORDER BY "누적거래량" DESC
LIMIT 10;

-- 증권그룹별 상장주식수 및 자본금 현황
SELECT
    secu_grp_id        AS "증권그룹",
    COUNT(*)           AS "종목수",
    SUM(lsst_ct)       AS "상장주식수합계",
    SUM(capt_amt)      AS "자본금합계"
FROM nextrade_equity_market_data.fin.nx_ht_bat_refer_a0
WHERE dwdd = '2026-01-23'
GROUP BY secu_grp_id
ORDER BY "종목수" DESC;

-- 시장별 상승/하락/보합 종목 수 (2026-01-23)
SELECT
    mkt_id       AS "시장구분",
    fluc_up_ct   AS "상승종목수",
    fluc_dn_ct   AS "하락종목수",
    fluc_stdn_ct AS "보합종목수"
FROM nextrade_equity_market_data.fin.nx_ht_onl_stats_b5
WHERE dwdd = '2026-01-23'
ORDER BY mkt_id;

-- 종목별 차익/비차익 프로그램 매매량
SELECT
    dwdd          AS "기준일",
    isu_cd        AS "종목코드",
    mkt_id        AS "시장구분",
    SUM(ask_arb_qty)  AS "매도차익거래량합계",
    SUM(bid_arb_qty)  AS "매수차익거래량합계"
FROM nextrade_equity_market_data.fin.nx_ht_onl_stats_c3
GROUP BY dwdd, isu_cd, mkt_id
ORDER BY dwdd DESC, "매도차익거래량합계" DESC
LIMIT 20;


-- ==========================================================================
-- Dataset 2: KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA (apartment prices)
-- ==========================================================================

-- 아파트 시세 — 컬럼 어휘 사전
SELECT
    sgg                              AS "시군구",
    emd                              AS "읍면동",
    yyyymmdd                         AS "기준월",
    total_households                 AS "세대수",
    meme_price_per_supply_pyeong     AS "평당매매가",
    jeonse_price_per_supply_pyeong   AS "평당전세가"
FROM korean_population__apartment_market_price_data.hackathon_2025q2.region_apt_richgo_market_price_m_h
LIMIT 1;

-- 인구 성별·연령 — 컬럼 어휘 사전
SELECT
    sgg          AS "시군구",
    emd          AS "읍면동",
    yyyymmdd     AS "기준일",
    total        AS "총인구",
    male         AS "남성인구",
    female       AS "여성인구",
    age_under20  AS "20세미만인구",
    age_20s      AS "20대인구",
    age_30s      AS "30대인구",
    age_40s      AS "40대인구",
    age_50s      AS "50대인구",
    age_60s      AS "60대인구",
    age_over70   AS "70세이상인구"
FROM korean_population__apartment_market_price_data.hackathon_2025q2.region_mois_population_gender_age_m_h
LIMIT 1;

-- 육아지수 — 컬럼 어휘 사전
SELECT
    sgg                            AS "시군구",
    emd                            AS "읍면동",
    yyyymmdd                       AS "기준일",
    age_under5                     AS "5세미만인구",
    female_20to40                  AS "20-40대여성인구",
    age_under5_per_female_20to40   AS "육아지수"
FROM korean_population__apartment_market_price_data.hackathon_2025q2.region_mois_population_age_under5_per_female_20to40_m_h
LIMIT 1;

-- 구별 월별 평균 매매가 및 전세가 추이
SELECT
    sgg                                        AS "시군구",
    yyyymmdd                                   AS "기준월",
    AVG(meme_price_per_supply_pyeong)          AS "평균평당매매가",
    AVG(jeonse_price_per_supply_pyeong)        AS "평균평당전세가"
FROM korean_population__apartment_market_price_data.hackathon_2025q2.region_apt_richgo_market_price_m_h
WHERE region_level = 'sgg'
GROUP BY sgg, yyyymmdd
ORDER BY yyyymmdd DESC, sgg;

-- 서초구 동별 매매가 순위 (2024년 12월)
SELECT
    emd                              AS "읍면동",
    meme_price_per_supply_pyeong     AS "평당매매가",
    jeonse_price_per_supply_pyeong   AS "평당전세가"
FROM korean_population__apartment_market_price_data.hackathon_2025q2.region_apt_richgo_market_price_m_h
WHERE region_level = 'emd' AND sgg = '서초구' AND yyyymmdd = '2024-12-01'
ORDER BY meme_price_per_supply_pyeong DESC;

-- 구별 총인구 및 성별 인구
SELECT
    sgg     AS "시군구",
    total   AS "총인구",
    male    AS "남성인구",
    female  AS "여성인구"
FROM korean_population__apartment_market_price_data.hackathon_2025q2.region_mois_population_gender_age_m_h
WHERE region_level = 'sgg'
ORDER BY total DESC;

-- 구별 전세가율 추이 (전세가 / 매매가)
SELECT
    sgg          AS "시군구",
    yyyymmdd     AS "기준월",
    ROUND(AVG(jeonse_price_per_supply_pyeong) / NULLIF(AVG(meme_price_per_supply_pyeong), 0) * 100, 1) AS "전세가율"
FROM korean_population__apartment_market_price_data.hackathon_2025q2.region_apt_richgo_market_price_m_h
WHERE region_level = 'sgg'
GROUP BY sgg, yyyymmdd
ORDER BY yyyymmdd, sgg;


-- ==========================================================================================
-- Dataset 3: SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS (SPH)
-- ==========================================================================================

-- 카드 매출 — 컬럼 어휘 사전
SELECT
    standard_year_month       AS "기준연월",
    weekday_weekend           AS "주중주말구분",
    total_sales               AS "카드매출액",
    food_sales                AS "음식업매출",
    coffee_sales              AS "커피매출",
    entertainment_sales       AS "유흥오락매출",
    education_academy_sales   AS "학원교육매출",
    medical_sales             AS "의료매출",
    beauty_sales              AS "미용매출",
    clothing_accessories_sales AS "의류잡화매출",
    e_commerce_sales          AS "이커머스매출"
FROM seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.card_sales_info
LIMIT 1;

-- 유동인구 — 컬럼 어휘 사전
SELECT
    standard_year_month     AS "기준연월",
    weekday_weekend         AS "주중주말구분",
    gender                  AS "성별",
    age_group               AS "연령대",
    time_slot               AS "시간대",
    visiting_population     AS "유동인구",
    working_population      AS "직장인구",
    residential_population  AS "거주인구"
FROM seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.floating_population_info
LIMIT 1;

-- 소득·자산 — 컬럼 어휘 사전
SELECT
    standard_year_month     AS "기준연월",
    average_income          AS "평균소득",
    median_income           AS "중위소득",
    average_score           AS "평균신용점수",
    average_asset_amount    AS "평균자산",
    total_usage_amount      AS "카드이용금액",
    average_card_count      AS "평균카드보유수",
    housing_balance_count   AS "주택담보대출자수"
FROM seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.asset_income_info
LIMIT 1;

-- 지역별 카드 매출 상위 10 (2025년 12월)
SELECT
    m.city_kor_name        AS "시군구명",
    c.standard_year_month  AS "기준연월",
    SUM(c.total_sales)     AS "카드매출액합계"
FROM seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.card_sales_info c
JOIN seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.m_scco_mst m
  ON c.province_code = m.province_code AND c.city_code = m.city_code AND c.district_code = m.district_code
WHERE c.standard_year_month = '202512'
GROUP BY m.city_kor_name, c.standard_year_month
ORDER BY "카드매출액합계" DESC
LIMIT 10;

-- 지역별 평균소득 및 평균신용점수 (2025년 12월)
SELECT
    m.city_kor_name          AS "시군구명",
    m.district_kor_name      AS "법정동명",
    AVG(a.average_income)    AS "평균소득",
    AVG(a.average_score)     AS "평균신용점수",
    AVG(a.average_asset_amount) AS "평균자산"
FROM seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.asset_income_info a
JOIN seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.m_scco_mst m
  ON a.province_code = m.province_code AND a.city_code = m.city_code AND a.district_code = m.district_code
WHERE a.standard_year_month = '202512'
GROUP BY m.city_kor_name, m.district_kor_name
ORDER BY "평균소득" DESC
LIMIT 20;

-- 주중/주말 유동·직장·거주인구 월별 추이
SELECT
    standard_year_month    AS "기준연월",
    weekday_weekend        AS "주중주말구분",
    SUM(visiting_population)    AS "유동인구합계",
    SUM(working_population)     AS "직장인구합계",
    SUM(residential_population) AS "거주인구합계"
FROM seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.floating_population_info
GROUP BY standard_year_month, weekday_weekend
ORDER BY standard_year_month DESC, weekday_weekend;

-- 개인카드 업종별 매출 카테고리 분해
SELECT
    standard_year_month        AS "기준연월",
    SUM(food_sales)            AS "음식업매출",
    SUM(coffee_sales)          AS "커피매출",
    SUM(entertainment_sales)   AS "유흥오락매출",
    SUM(education_academy_sales) AS "학원교육매출"
FROM seoul_districtlevel_data_floating_population_consumption_and_assets.grandata.card_sales_info
WHERE card_type = '1'
GROUP BY standard_year_month
ORDER BY standard_year_month DESC;


-- ============================================================================================================
-- Dataset 4: SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
-- ============================================================================================================

-- 지역별 월별 계약 현황 — 컬럼 어휘 사전
SELECT
    year_month           AS "기준월",
    install_state        AS "설치도",
    install_city         AS "설치시군구",
    main_category_name   AS "상품유형",
    contract_count       AS "계약건수",
    open_cvr             AS "개통전환율",
    payend_cvr           AS "지급전환율",
    total_net_sales      AS "순매출액"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v01_monthly_regional_contract_stats
LIMIT 1;

-- 결합상품 패턴 — 컬럼 어휘 사전
SELECT
    year_month           AS "기준월",
    main_category_name   AS "상품유형",
    bundle_combination   AS "결합상품유형",
    has_tv               AS "TV결합여부",
    has_phone            AS "전화결합여부",
    has_wifi             AS "와이파이결합여부",
    contract_count       AS "계약건수",
    avg_net_sales        AS "평균순매출",
    avg_policy_amount    AS "평균정책금"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v02_service_bundle_patterns
LIMIT 1;

-- 채널별 성과 — 컬럼 어휘 사전
SELECT
    year_month           AS "기준월",
    main_category_name   AS "상품유형",
    receive_path_name    AS "접수경로",
    inflow_path_name     AS "유입경로",
    contract_count       AS "계약건수",
    open_cvr             AS "개통전환율",
    total_net_sales      AS "순매출액"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v04_channel_contract_performance
LIMIT 1;

-- 상품유형별 월별 계약건수 및 순매출
SELECT
    year_month           AS "기준월",
    main_category_name   AS "상품유형",
    SUM(contract_count)  AS "계약건수합계",
    SUM(total_net_sales) AS "순매출합계"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v01_monthly_regional_contract_stats
GROUP BY year_month, main_category_name
ORDER BY year_month DESC, "계약건수합계" DESC;

-- 인터넷 상품 도별 계약 현황 (최근 월)
SELECT
    install_state        AS "설치도",
    SUM(contract_count)  AS "계약건수",
    AVG(open_cvr)        AS "개통전환율",
    SUM(total_net_sales) AS "순매출액"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v01_monthly_regional_contract_stats
WHERE main_category_name = '인터넷'
  AND year_month = (SELECT MAX(year_month) FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v01_monthly_regional_contract_stats)
GROUP BY install_state
ORDER BY "계약건수" DESC;

-- 결합상품 유형별 계약건수 및 평균정책금
SELECT
    bundle_combination   AS "결합상품유형",
    has_tv               AS "TV결합여부",
    has_phone            AS "전화결합여부",
    has_wifi             AS "와이파이결합여부",
    SUM(contract_count)  AS "계약건수",
    AVG(avg_policy_amount) AS "평균정책금"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v02_service_bundle_patterns
GROUP BY bundle_combination, has_tv, has_phone, has_wifi
ORDER BY "계약건수" DESC;

-- 상품유형별 퍼널 전환율 (최근 월)
SELECT
    main_category_name   AS "상품유형",
    cvr_consult_request  AS "상담전환율",
    cvr_subscription     AS "청약전환율",
    cvr_registend        AS "접수전환율",
    cvr_open             AS "개통전환율",
    cvr_payend           AS "지급전환율",
    overall_cvr          AS "전체전환율"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v03_contract_funnel_conversion
WHERE year_month = (SELECT MAX(year_month) FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v03_contract_funnel_conversion)
ORDER BY overall_cvr DESC;

-- 유입경로별 계약건수 및 순매출
SELECT
    inflow_path_name     AS "유입경로",
    SUM(contract_count)  AS "계약건수",
    AVG(open_cvr)        AS "개통전환율",
    SUM(total_net_sales) AS "순매출액"
FROM south_korea_telecom_subscription_analytics__contracts_marketing_and_call_center_insights_by_region.telecom_insights.v04_channel_contract_performance
GROUP BY inflow_path_name
ORDER BY "계약건수" DESC;
