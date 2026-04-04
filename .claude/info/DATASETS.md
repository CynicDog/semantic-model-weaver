# Data Catalog

All datasets are available via the Snowflake Marketplace. Both Tech Track and Business Track participants use the same datasets.

## 1. NEXTRADE_EQUITY_MARKET_DATA

**Marketplace listing:** `GZTHZC6Y4W`
**Schema:** `FIN` (Finished Area — processed sales data)
**Date range (all tables):** 2025-12-15 to 2026-01-23

### NX_HT_BAT_REFER_A0
**Comment:** 배치참조A0종목정보 (Stock reference info — batch)
**Rows:** 72,165

| Column | Type | Description |
|---|---|---|
| DWDD | DATE (NOT NULL) | DW date |
| ISU_CD | VARCHAR(12) (NOT NULL) | Stock code |
| ISU_SRT_CD | VARCHAR(9) | Short stock code |
| ISU_ABWD_NM | VARCHAR(80) | Stock abbreviated name (Korean) |
| ISU_ENAW_NM | VARCHAR(80) | Stock abbreviated name (English) |
| INFO_DSTB_GRP_NO | VARCHAR(5) | Info distribution group number |
| MKOP_PROD_GRP_ID | VARCHAR(3) | Market operation product group ID |
| SECU_GRP_ID | VARCHAR(2) | Security group ID (ST=stock, DR=DR, MF=fund, IF=ETF, FS=futures, RT=REIT, etc.) |
| OFF_TP_CD | VARCHAR(2) | Ex-rights/dividend type code |
| PAPR_CHG_TP_CD | VARCHAR(2) | Par value change type code |
| OPBA_PRC_ISU_YN | VARCHAR(1) | Opening-price-as-base-price issue Y/N |
| RVAL_ISU_RSN_CD | VARCHAR(2) | Revaluation issue reason code |
| BASE_PRC_CHG_ISU_YN | VARCHAR(1) | Base price change issue Y/N |
| RDEN_TG_COND_CD | VARCHAR(1) | Arbitrary termination trigger condition code |
| MKT_ALRT_RISK_ADNT_YN | VARCHAR(1) | Market alert risk advisory Y/N |
| MKT_ALRT_TP_CD | VARCHAR(2) | Market alert type code |
| GVNC_EXLC_YN | VARCHAR(1) | Good governance Y/N |
| AMIS_YN | VARCHAR(1) | Managed issue Y/N |
| UFDC_YN | VARCHAR(1) | Unfaithful disclosure Y/N |
| BKLS_YN | VARCHAR(1) | Back-door listing Y/N |
| TDHL_YN | VARCHAR(1) | Trading halt Y/N |
| IND_ID | VARCHAR(10) | Industry ID |
| SMSC_YN | VARCHAR(1) | SME Y/N |
| BSEC_TP_CD | VARCHAR(1) | Board section type code |
| IVOG_TP_CD | VARCHAR(1) | Investment vehicle type code |
| BASE_PRC | NUMBER(38,0) | Base price |
| KRX_PRDY_CSPR_TP_CD | VARCHAR(1) | KRX previous day closing price type code |
| KRX_PRDY_CSPR | NUMBER(38,0) | KRX previous day closing price |
| PRDY_ACC_TRD_QTY | NUMBER(38,0) | Previous day accumulated trading volume |
| PRDY_ACC_TRVAL | NUMBER(38,0) | Previous day accumulated trading value |
| ULPR | NUMBER(38,0) | Upper limit price |
| LWLP | NUMBER(38,0) | Lower limit price |
| PAPR | NUMBER(38,0) | Par value |
| ISSU_PRC | NUMBER(38,0) | Issue price |
| LIST_DD | VARCHAR(8) | Listing date (YYYYMMDD string) |
| LSST_CT | NUMBER(38,0) | Number of listed shares |
| ARTR_YN | VARCHAR(1) | Cleanup trading Y/N |
| EXIE_STRT_DD | VARCHAR(8) | Existence start date |
| EXIE_END_DD | VARCHAR(8) | Existence end date |
| CAPT_AMT | NUMBER(38,0) | Capital amount |
| CRDT_ODPS_YN | VARCHAR(1) | Credit order allowed Y/N |
| CPIC_TP_CD | VARCHAR(2) | Capital increase type code |
| NTST_YN | VARCHAR(1) | National stock Y/N |
| VALU_PRC | NUMBER(38,0) | Valuation price |
| LWST_OD_PRC | NUMBER(38,0) | Lowest quoted price |
| HGST_OD_PRC | NUMBER(38,0) | Highest quoted price |
| REITS_KIND_CD | VARCHAR(1) | REIT kind code |
| TGST_ISU_CD | VARCHAR(12) | Target principal issue code |
| CURR_ISO_CD | VARCHAR(3) | Currency ISO code |
| NT_CD | VARCHAR(3) | Country code |
| MKMK_PSBT_YN | VARCHAR(1) | Market making allowed Y/N |
| SHSL_PSBT_YN | VARCHAR(1) | Short selling allowed Y/N |
| REGS_YN | VARCHAR(1) | REGS Y/N |
| SPAC_YN | VARCHAR(1) | SPAC Y/N |
| IVCT_RMID_ISU_YN | VARCHAR(1) | Investment caution reminder issue Y/N |
| STRM_OVHT_ISU_TP_CD | VARCHAR(1) | Short-term overheated issue type code |
| LSBR_MBR_NO | VARCHAR(5) | Listing underwriter member number |
| LP_ODPS_YN | VARCHAR(1) | LP order allowed Y/N |
| LWLQ_YN | VARCHAR(1) | Low liquidity Y/N |
| ANSG_YN | VARCHAR(1) | Abnormal surge Y/N |
| IVWR_ISU_YN | VARCHAR(1) | Investment warning issue Y/N |
| LSST_CT_LCK_ISU_YN | VARCHAR(1) | Insufficient listed shares issue Y/N |
| SPAC_DAPP_MRGE_YN | VARCHAR(1) | SPAC extinction merger Y/N |
| SGMT_TP_CD | VARCHAR(1) | Segment type code |
| CPTR_TRD_CHIC_YN | VARCHAR(1) | Competitive trading selected Y/N |
| CPTR_TRDVL_LMT_YN | VARCHAR(1) | Competitive trading volume limit Y/N |
| CPTR_TRD_IPSB_RSN_OCCR_YN | VARCHAR(1) | Competitive trading impossible reason occurred Y/N |
| CPTR_TRD_PSBT_YN | VARCHAR(1) | Competitive trading possible Y/N |
| NGTR_TRD_PSBT_YN | VARCHAR(1) | Negotiated trading possible Y/N |
| NXT_PRDY_CSPR_TP_CD | VARCHAR(1) | NXT previous day closing price type code |
| NXT_PRDY_CSPR | NUMBER(38,0) | NXT previous day closing price |
| CPTR_TRD_PMSN_CD | NUMBER(38,0) | Competitive trading permission code |
| MAMKT_BF_NGTR_PSBT_YN | VARCHAR(1) | Main market pre-transfer negotiated trading possible Y/N |

### NX_HT_ONL_MKTPR_A3
**Comment:** 온라인시세A3체결 (Trade executions)
**Rows:** 95,745,598

| Column | Type | Description |
|---|---|---|
| DWDD | DATE (NOT NULL) | DW date |
| MKT_ID | VARCHAR(3) (NOT NULL) | Market ID (STK=KOSPI, KSQ=KOSDAQ) |
| BRD_ID | VARCHAR(2) (NOT NULL) | Board ID |
| ISU_CD | VARCHAR(12) (NOT NULL) | Stock code |
| INFO_DSTB_SEQ | NUMBER(38,0) (NOT NULL) | Info distribution sequence number |
| SESS_ID | VARCHAR(2) | Session ID |
| TDG_PROCS_DTLS_TIME | VARCHAR(12) | Trade processing detailed time |
| PRDY_CMP_TP_CD | VARCHAR(1) | Previous day comparison type code |
| PRDY_CMP_PRC | NUMBER(38,0) | Price change vs previous day |
| TD_PRC | NUMBER(38,0) | Execution price |
| TRD_QTY | NUMBER(38,0) | Trade quantity |
| OPPR | NUMBER(38,0) | Opening price |
| HGPR | NUMBER(38,0) | High price |
| LWPR | NUMBER(38,0) | Low price |
| ACC_TRD_QTY | NUMBER(38,0) | Accumulated trading volume |
| ACC_TRVAL | NUMBER(38,0) | Accumulated trading value |
| LST_AKNB_TP_CD | VARCHAR(1) | Last ask/bid type code |
| LP_HOLD_QTY | NUMBER(38,0) | LP holding quantity |
| AKFB_OD_PRC | NUMBER(38,0) | Best ask price |
| BDFB_OD_PRC | NUMBER(38,0) | Best bid price |

### NX_HT_ONL_MKTPR_B6
**Comment:** 온라인시세B6우선호가 (Best bid/ask quotes — 10 levels)
**Rows:** 413,904,265

| Column | Type | Description |
|---|---|---|
| DWDD | DATE (NOT NULL) | DW date |
| MKT_ID | VARCHAR(3) (NOT NULL) | Market ID |
| BRD_ID | VARCHAR(2) (NOT NULL) | Board ID |
| ISU_CD | VARCHAR(12) (NOT NULL) | Stock code |
| INFO_DSTB_SEQ | NUMBER(38,0) (NOT NULL) | Info distribution sequence number |
| SESS_ID | VARCHAR(2) | Session ID |
| TDG_PROCS_DTLS_TIME | VARCHAR(12) | Trade processing detailed time |
| ASK_STEP1_BSOD_PRC | NUMBER(38,0) | Ask level 1 priority quote price |
| BID_STEP1_BSOD_PRC | NUMBER(38,0) | Bid level 1 priority quote price |
| ASK_STEP1_BSOD_RQTY | NUMBER(38,0) | Ask level 1 remaining quantity |
| BID_STEP1_BSOD_RQTY | NUMBER(38,0) | Bid level 1 remaining quantity |
| ASK_STEP2_BSOD_PRC … ASK_STEP10_BSOD_PRC | NUMBER(38,0) | Ask levels 2–10 price |
| BID_STEP2_BSOD_PRC … BID_STEP10_BSOD_PRC | NUMBER(38,0) | Bid levels 2–10 price |
| ASK_STEP2_BSOD_RQTY … ASK_STEP10_BSOD_RQTY | NUMBER(38,0) | Ask levels 2–10 remaining quantity |
| BID_STEP2_BSOD_RQTY … BID_STEP10_BSOD_RQTY | NUMBER(38,0) | Bid levels 2–10 remaining quantity |
| AKOD_OPN_STEP_RQTY_SUM_QTY | NUMBER(38,0) | Total ask open step remaining quantity |
| BIOD_OPN_STEP_RQTY_SUM_QTY | NUMBER(38,0) | Total bid open step remaining quantity |
| DEEM_TD_PRC | NUMBER(38,0) | Expected execution price |
| DEEM_TD_QTY | NUMBER(38,0) | Expected execution quantity |
| MID_PRC | NUMBER(38,0) | Mid price |
| ASK_MDPR_RQTY_SUM_QTY | NUMBER(38,0) | Ask mid-price remaining quantity total |
| BID_MDPR_RQTY_SUM_QTY | NUMBER(38,0) | Bid mid-price remaining quantity total |

### NX_HT_ONL_MKTPR_E1
**Comment:** 온라인시세E1종가매매호가 (Closing auction quotes)
**Rows:** 80,324

| Column | Type | Description |
|---|---|---|
| DWDD | DATE (NOT NULL) | DW date |
| MKT_ID | VARCHAR(3) (NOT NULL) | Market ID |
| BRD_ID | VARCHAR(2) (NOT NULL) | Board ID |
| ISU_CD | VARCHAR(12) (NOT NULL) | Stock code |
| INFO_DSTB_SEQ | NUMBER(38,0) (NOT NULL) | Info distribution sequence number |
| AKOD_TOT_RQTY | NUMBER(38,0) | Total ask quote remaining quantity |
| BIOD_TOT_RQTY | NUMBER(38,0) | Total bid quote remaining quantity |

### NX_HT_ONL_STATS_B5
**Comment:** 온라인통계B5현재등락 (Price change stats)
**Rows:** 233,334

| Column | Type | Description |
|---|---|---|
| DWDD | DATE (NOT NULL) | DW date |
| DATA_CALC_TIME | VARCHAR (NOT NULL) | Data calculation time |
| MKT_ID | VARCHAR(3) (NOT NULL) | Market ID |
| FLUC_OBJ_ISU_CT | NUMBER(38,0) | Number of issues subject to fluctuation |
| FLUC_TRD_FORM_CT | NUMBER(38,0) | Number of issues that formed a trade |
| FLUC_UPLM_CT | NUMBER(38,0) | Upper limit count |
| FLUC_UP_CT | NUMBER(38,0) | Rising count |
| FLUC_STDN_CT | NUMBER(38,0) | Unchanged count |
| FLUC_LWLM_CT | NUMBER(38,0) | Lower limit count |
| FLUC_DN_CT | NUMBER(38,0) | Falling count |
| FLUC_BQUT_CT | NUMBER(38,0) | Momentum count |
| FLUC_BQUT_UP_CT | NUMBER(38,0) | Upward momentum count |
| FLUC_BQUT_DN_CT | NUMBER(38,0) | Downward momentum count |

### NX_HT_ONL_STATS_C3
**Comment:** 온라인통계C3프로그램매매종목별 (Program trading by stock)
**Rows:** 8,630,692

| Column | Type | Description |
|---|---|---|
| DWDD | DATE (NOT NULL) | DW date |
| DATA_CALC_TIME | VARCHAR (NOT NULL) | Data calculation time |
| MKT_ID | VARCHAR(3) (NOT NULL) | Market ID |
| ISU_CD | VARCHAR(12) (NOT NULL) | Stock code |
| ASK_ARB_RQTY | NUMBER(38,0) | Ask arbitrage remaining quantity |
| BID_ARB_RQTY | NUMBER(38,0) | Bid arbitrage remaining quantity |
| ASK_NARB_RQTY | NUMBER(38,0) | Ask non-arbitrage remaining quantity |
| BID_NARB_RQTY | NUMBER(38,0) | Bid non-arbitrage remaining quantity |
| ASK_ARB_QTY | NUMBER(38,0) | Ask arbitrage quantity |
| BID_ARB_QTY | NUMBER(38,0) | Bid arbitrage quantity |
| ASK_NARB_QTY | NUMBER(38,0) | Ask non-arbitrage quantity |
| BID_NARB_QTY | NUMBER(38,0) | Bid non-arbitrage quantity |
| TRAK_ARB_TD_QTY | NUMBER(38,0) | Entrusted ask arbitrage executed quantity |
| PCAK_ARB_TD_QTY | NUMBER(38,0) | Proprietary ask arbitrage executed quantity |
| TRBD_ARB_TD_QTY | NUMBER(38,0) | Entrusted bid arbitrage executed quantity |
| PCBD_ARB_TD_QTY | NUMBER(38,0) | Proprietary bid arbitrage executed quantity |
| TRAK_NARB_TD_QTY | NUMBER(38,0) | Entrusted ask non-arbitrage executed quantity |
| PCAK_NARB_TD_QTY | NUMBER(38,0) | Proprietary ask non-arbitrage executed quantity |
| TRBD_NARB_TD_QTY | NUMBER(38,0) | Entrusted bid non-arbitrage executed quantity |
| PCBD_NARB_TD_QTY | NUMBER(38,0) | Proprietary bid non-arbitrage executed quantity |
| TRAK_ARB_TRVAL | NUMBER(38,0) | Entrusted ask arbitrage trade value |
| PCAK_ARB_TRVAL | NUMBER(38,0) | Proprietary ask arbitrage trade value |
| TRBD_ARB_TRVAL | NUMBER(38,0) | Entrusted bid arbitrage trade value |
| PCBD_ARB_TRVAL | NUMBER(38,0) | Proprietary bid arbitrage trade value |
| TRAK_NARB_TRVAL | NUMBER(38,0) | Entrusted ask non-arbitrage trade value |
| PCAK_NARB_TRVAL | NUMBER(38,0) | Proprietary ask non-arbitrage trade value |
| TRBD_NARB_TRVAL | NUMBER(38,0) | Entrusted bid non-arbitrage trade value |
| PCBD_NARB_TRVAL | NUMBER(38,0) | Proprietary bid non-arbitrage trade value |

### NX_HT_ONL_STATS_P0
**Comment:** 온라인통계P0프로그램매매투자자별 (Program trading by investor type)
**Rows:** 164,744

| Column | Type | Description |
|---|---|---|
| DWDD | DATE (NOT NULL) | DW date |
| DATA_CALC_TIME | VARCHAR (NOT NULL) | Data calculation time |
| MKT_ID | VARCHAR(3) (NOT NULL) | Market ID |
| IVTR_TP_CD | VARCHAR(4) (NOT NULL) | Investor type code |
| ASK_ARB_TD_QTY | NUMBER(38,0) | Ask arbitrage executed quantity |
| ASK_ARB_TRVAL | NUMBER(38,0) | Ask arbitrage trade value |
| ASK_NARB_TD_QTY | NUMBER(38,0) | Ask non-arbitrage executed quantity |
| ASK_NARB_TRVAL | NUMBER(38,0) | Ask non-arbitrage trade value |
| BID_ARB_TD_QTY | NUMBER(38,0) | Bid arbitrage executed quantity |
| BID_ARB_TRVAL | NUMBER(38,0) | Bid arbitrage trade value |
| BID_NARB_TD_QTY | NUMBER(38,0) | Bid non-arbitrage executed quantity |
| BID_NARB_TRVAL | NUMBER(38,0) | Bid non-arbitrage trade value |


## 2. KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA

**Marketplace listing:** `GZTHZ4PPG`
**Schema:** `HACKATHON_2025Q2`
**Coverage:** Seoul — 서초구, 영등포구, 중구 only

### REGION_APT_RICHGO_MARKET_PRICE_M_H
**Comment:** 서울특별시 서초구, 영등포구, 중구의 리치고 실거래기반 AI 매매/전세 시세 (AI-estimated apartment sale/jeonse prices)
**Rows:** 4,356
**Date range:** 2012-01-01 to 2024-12-01 (monthly)
**Region levels:** `sgg` (시군구), `emd` (읍면동)

| Column | Type | Description |
|---|---|---|
| REGION_LEVEL | VARCHAR | Region level: `sgg` or `emd` |
| BJD_CODE | VARCHAR | Legal district code (법정동코드) |
| SD | VARCHAR | Province name (시도명) — always "서울" |
| SGG | VARCHAR | District name (시군구명) — 서초구, 영등포구, or 중구 |
| EMD | VARCHAR | Neighborhood name (읍면동명) |
| TOTAL_HOUSEHOLDS | NUMBER(38,0) | Total households in complex (세대) |
| YYYYMMDD | DATE | Date (first of month) |
| MEME_PRICE_PER_SUPPLY_PYEONG | FLOAT | AI-estimated sale price per supply pyeong (만원) |
| JEONSE_PRICE_PER_SUPPLY_PYEONG | FLOAT | AI-estimated jeonse price per supply pyeong (만원) |

### REGION_MOIS_POPULATION_GENDER_AGE_M_H
**Comment:** 서울특별시 서초구, 영등포구, 중구의 성별 연령별 주민등록 인구수 (Registered population by gender and age)
**Rows:** 118
**Date range:** 2025-01-01 only (snapshot)
**Region levels:** `cty`, `sd`, `sgg`, `emd`

| Column | Type | Description |
|---|---|---|
| REGION_LEVEL | VARCHAR | Region level (cty/sd/sgg/emd) |
| BJD_CODE | VARCHAR | Legal district code |
| SD | VARCHAR | Province name |
| SGG | VARCHAR | District name |
| EMD | VARCHAR | Neighborhood name |
| YYYYMMDD | DATE | Reference date |
| TOTAL | NUMBER(38,0) | Total population |
| MALE | NUMBER(38,0) | Total male population |
| FEMALE | NUMBER(38,0) | Total female population |
| AGE_UNDER20 | NUMBER(38,0) | Population under 20 |
| AGE_20S | NUMBER(38,0) | Population in their 20s |
| AGE_30S | NUMBER(38,0) | Population in their 30s |
| AGE_40S | NUMBER(38,0) | Population in their 40s |
| AGE_50S | NUMBER(38,0) | Population in their 50s |
| AGE_60S | NUMBER(38,0) | Population in their 60s |
| AGE_OVER70 | NUMBER(38,0) | Population 70 and over |

### REGION_MOIS_POPULATION_AGE_UNDER5_PER_FEMALE_20TO40_M_H
**Comment:** 서울특별시 서초구, 영등포구, 중구의 20~40세 여성인구수 대비 5세미만 인구수 (Under-5 population ratio relative to women 20–40)
**Rows:** 118
**Date range:** 2025-01-01 only (snapshot)
**Region levels:** `sgg`, `emd`

| Column | Type | Description |
|---|---|---|
| REGION_LEVEL | VARCHAR(3) | Region level: `sgg` or `emd` |
| BJD_CODE | VARCHAR(10) | Legal district code |
| SD | VARCHAR(10) | Province name |
| SGG | VARCHAR(20) | District name |
| EMD | VARCHAR(20) | Neighborhood name |
| YYYYMMDD | DATE | Reference date |
| AGE_UNDER5 | NUMBER(38,0) | Population under 5 years old |
| FEMALE_20TO40 | NUMBER(38,0) | Female population aged 20–40 |
| AGE_UNDER5_PER_FEMALE_20TO40 | FLOAT | Ratio of under-5 to women 20–40 (fertility proxy) |


## 3. SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS

**Marketplace listing:** `GZTHZ6GSX7`
**Schema:** `GRANDATA`
**Coverage:** Seoul — nationwide legal districts (467 in M_SCCO_MST); consumption/population data focused on Seoul
**Date range (data tables):** 202101 to 202512 (monthly, YYYYMM format)

### M_SCCO_MST
**Comment:** Administrative boundary (legal district) master
**Rows:** 467

| Column | Type | Description |
|---|---|---|
| PROVINCE_CODE | VARCHAR(2) | Province code |
| CITY_CODE | VARCHAR(5) | City/county/district code |
| DISTRICT_CODE | VARCHAR(8) | Town/township/neighborhood (법정동) code |
| PROVINCE_KOR_NAME | VARCHAR(50) | Province name (Korean) |
| CITY_KOR_NAME | VARCHAR(50) | City/county/district name (Korean) |
| DISTRICT_KOR_NAME | VARCHAR(50) | Neighborhood name (Korean) |
| PROVINCE_ENG_NAME | VARCHAR(50) | Province name (English) |
| CITY_ENG_NAME | VARCHAR(50) | City/county/district name (English) |
| DISTRICT_ENG_NAME | VARCHAR(50) | Neighborhood name (English) |
| DISTRICT_GEOM | GEOGRAPHY | Polygon geometry of the legal district |

### CODE_MASTER
**Comment:** Common Code Master Table (lookup for categorical codes used in other tables)
**Rows:** 37

| Column | Type | Description |
|---|---|---|
| CODE_ID | VARCHAR(10) (NOT NULL) | Code group ID |
| CODE_NAME | VARCHAR(100) (NOT NULL) | Code group name |
| SUB_CODE | VARCHAR(10) (NOT NULL) | Sub-code value |
| SUB_CODE_NAME | VARCHAR(100) (NOT NULL) | Sub-code label |
| SORT_ORDER | NUMBER(38,0) | Display order |
| USE_YN | VARCHAR(1) | In-use flag (Y/N), default 'Y' |
| REMARKS | VARCHAR(500) | Notes |

**Code groups defined:**

| CODE_ID | CODE_NAME | Values |
|---|---|---|
| M01 | 성별 (Gender) | M=남(Male), F=여(Female) |
| M02 | 연령대 (Age group) | 10=00-14, 15=15-19, 20=20-24, 25=25-29, 30=30-34, 35=35-39, 40=40-44, 45=45-49, 50=50-54, 55=55-59, 60=60-64, 65=65-69, 70=70-74, 75=75-79, 80=80-84, 85=85-89 |
| M03 | 시간대 (Time slot) | T06=아침(Morning), T09=오전(Forenoon), T12=점심(Noon), T15=오후(Afternoon), T18=저녁(Evening), T21=심야(Late night), T24=기타(Other) |
| M04 | 주중/주말 (Weekday/Weekend) | H=주말(Weekend), W=주중(Weekday) |
| M06 | 라이프스타일 (Lifestyle) | L01=싱글, L02=신혼부부, L03=영유아가족, L04=청소년가족, L05=성인자녀가족, L06=실버 |
| M07 | 신용관리사_파일구분 (KCB file type) | 0=기업(Corporate), 1=주거(Residential) |
| M08 | 카드사_파일구분 (Card file type) | 0=법인(Corporate), 1=개인(Individual) |

### FLOATING_POPULATION_INFO
**Comment:** Monthly pedestrian traffic data by location, time, and demographics (residential, working, and visiting populations)
**Rows:** 2,577,120
**Date range:** 202101 to 202512

| Column | Type | Description |
|---|---|---|
| PROVINCE_CODE | VARCHAR(2) (NOT NULL) | Province code |
| CITY_CODE | VARCHAR(5) (NOT NULL) | City code |
| DISTRICT_CODE | VARCHAR(8) (NOT NULL) | District code |
| STANDARD_YEAR_MONTH | VARCHAR(6) (NOT NULL) | Year-month (YYYYMM) |
| WEEKDAY_WEEKEND | VARCHAR(1) (NOT NULL) | W=Weekday, H=Weekend (see M04) |
| GENDER | VARCHAR(1) (NOT NULL) | M/F (see M01) |
| AGE_GROUP | VARCHAR(2) (NOT NULL) | Age group code (see M02) |
| TIME_SLOT | VARCHAR(3) (NOT NULL) | Time slot (see M03) |
| RESIDENTIAL_POPULATION | FLOAT | Residential population count (persons) |
| WORKING_POPULATION | FLOAT | Working population count (persons) |
| VISITING_POPULATION | FLOAT | Visiting population count (persons) |

### CARD_SALES_INFO
**Comment:** Monthly Card Sales Information (Shinhan Card spending by district, category, and demographic)
**Rows:** 6,208,957
**Date range:** 202101 to 202512

| Column | Type | Description |
|---|---|---|
| PROVINCE_CODE | VARCHAR(2) (NOT NULL) | Province code |
| CITY_CODE | VARCHAR(5) (NOT NULL) | City code |
| DISTRICT_CODE | VARCHAR(8) (NOT NULL) | District code |
| STANDARD_YEAR_MONTH | VARCHAR(6) (NOT NULL) | Year-month (YYYYMM) |
| CARD_TYPE | VARCHAR(1) (NOT NULL) | 0=Corporate, 1=Individual (see M08) |
| WEEKDAY_WEEKEND | VARCHAR(1) (NOT NULL) | W=Weekday, H=Weekend (see M04) |
| GENDER | VARCHAR(1) (NOT NULL) | M/F (see M01) |
| AGE_GROUP | VARCHAR(2) (NOT NULL) | Age group code (see M02) |
| TIME_SLOT | VARCHAR(3) (NOT NULL) | Time slot (see M03) |
| LIFESTYLE | VARCHAR(3) (NOT NULL) | Lifestyle code (see M06) |
| TOTAL_SALES | NUMBER(38,0) | Total sales amount (KRW) |
| FOOD_SALES | NUMBER(38,0) | Food category sales (KRW) |
| COFFEE_SALES | NUMBER(38,0) | Coffee sales (KRW) |
| ENTERTAINMENT_SALES | NUMBER(38,0) | Entertainment sales (KRW) |
| DEPARTMENT_STORE_SALES | NUMBER(38,0) | Department store sales (KRW) |
| LARGE_DISCOUNT_STORE_SALES | NUMBER(38,0) | Large discount store sales (KRW) |
| SMALL_RETAIL_STORE_SALES | NUMBER(38,0) | Small retail store sales (KRW) |
| CLOTHING_ACCESSORIES_SALES | NUMBER(38,0) | Clothing and accessories sales (KRW) |
| SPORTS_CULTURE_LEISURE_SALES | NUMBER(38,0) | Sports/culture/leisure sales (KRW) |
| ACCOMMODATION_SALES | NUMBER(38,0) | Accommodation sales (KRW) |
| TRAVEL_SALES | NUMBER(38,0) | Travel sales (KRW) |
| BEAUTY_SALES | NUMBER(38,0) | Beauty sales (KRW) |
| HOME_LIFE_SERVICE_SALES | NUMBER(38,0) | Home and life service sales (KRW) |
| EDUCATION_ACADEMY_SALES | NUMBER(38,0) | Education / academy sales (KRW) |
| MEDICAL_SALES | NUMBER(38,0) | Medical sales (KRW) |
| ELECTRONICS_FURNITURE_SALES | NUMBER(38,0) | Electronics and furniture sales (KRW) |
| CAR_SALES | NUMBER(38,0) | Car sales (KRW) |
| CAR_SERVICE_SUPPLIES_SALES | NUMBER(38,0) | Car service and supplies sales (KRW) |
| GAS_STATION_SALES | NUMBER(38,0) | Gas station sales (KRW) |
| E_COMMERCE_SALES | NUMBER(38,0) | E-commerce sales (KRW) |
| TOTAL_COUNT | FLOAT | Total transaction count (cases) |
| FOOD_COUNT | FLOAT | Food transaction count (cases) |
| COFFEE_COUNT | FLOAT | Coffee transaction count (cases) |
| ENTERTAINMENT_COUNT | FLOAT | Entertainment transaction count (cases) |
| DEPARTMENT_STORE_COUNT | FLOAT | Department store transaction count (cases) |
| LARGE_DISCOUNT_STORE_COUNT | FLOAT | Large discount store transaction count (cases) |
| SMALL_RETAIL_STORE_COUNT | FLOAT | Small retail store transaction count (cases) |
| CLOTHING_ACCESSORIES_COUNT | FLOAT | Clothing/accessories transaction count (cases) |
| SPORTS_CULTURE_LEISURE_COUNT | FLOAT | Sports/culture/leisure transaction count (cases) |
| ACCOMMODATION_COUNT | FLOAT | Accommodation transaction count (cases) |
| TRAVEL_COUNT | FLOAT | Travel transaction count (cases) |
| BEAUTY_COUNT | FLOAT | Beauty transaction count (cases) |
| HOME_LIFE_SERVICE_COUNT | FLOAT | Home/life service transaction count (cases) |
| EDUCATION_ACADEMY_COUNT | FLOAT | Education/academy transaction count (cases) |
| MEDICAL_COUNT | FLOAT | Medical transaction count (cases) |
| ELECTRONICS_FURNITURE_COUNT | FLOAT | Electronics/furniture transaction count (cases) |
| CAR_SALES_COUNT | FLOAT | Car sales transaction count (cases) |
| CAR_SERVICE_SUPPLIES_COUNT | FLOAT | Car service/supplies transaction count (cases) |
| GAS_STATION_COUNT | FLOAT | Gas station transaction count (cases) |
| E_COMMERCE_COUNT | FLOAT | E-commerce transaction count (cases) |

### ASSET_INCOME_INFO
**Comment:** Monthly KCB customer group income and assets information
**Rows:** 269,159
**Date range:** 202101 to 202512

| Column | Type | Description |
|---|---|---|
| PROVINCE_CODE | VARCHAR(2) (NOT NULL) | Province code |
| CITY_CODE | VARCHAR(5) (NOT NULL) | City code |
| DISTRICT_CODE | VARCHAR(8) (NOT NULL) | District code |
| STANDARD_YEAR_MONTH | VARCHAR(6) (NOT NULL) | Year-month (YYYYMM) |
| INCOME_TYPE | VARCHAR(1) | 0=Corporate, 1=Residential (see M07) |
| GENDER | VARCHAR(1) (NOT NULL) | M/F (see M01) |
| AGE_GROUP | VARCHAR(2) (NOT NULL) | Age group code (see M02) |
| CUSTOMER_COUNT | NUMBER(38,0) | Total population count (persons) |
| RATE_MODEL_GROUP_LARGE_COMPANY_EMPLOYEE | FLOAT | % large company employees |
| RATE_MODEL_GROUP_GENERAL_EMPLOYEE | FLOAT | % general employees |
| RATE_MODEL_GROUP_PROFESSIONAL_EMPLOYEE | FLOAT | % professional employees |
| RATE_MODEL_GROUP_EXECUTIVES | FLOAT | % executives |
| RATE_MODEL_GROUP_GENERAL_SELF_EMPLOYED | FLOAT | % general self-employed |
| RATE_MODEL_GROUP_PROFESSIONAL_SELF_EMPLOYED | FLOAT | % professional self-employed |
| RATE_MODEL_GROUP_OTHERS | FLOAT | % others (housewives, students, unemployed) |
| PYEONG_UNDER_20_COUNT | NUMBER(38,0) | Residents in <20 pyeong apartments |
| PYEONG_OVER_20_COUNT | NUMBER(38,0) | Residents in 20–29 pyeong apartments |
| PYEONG_OVER_30_COUNT | NUMBER(38,0) | Residents in 30–39 pyeong apartments |
| PYEONG_OVER_40_COUNT | NUMBER(38,0) | Residents in 40+ pyeong apartments |
| AVERAGE_PRICE_GAP | FLOAT | Apartment sale price growth rate (%) |
| AVERAGE_LEASE_GAP | FLOAT | Jeonse deposit growth rate (%) |
| AVERAGE_INCOME | NUMBER(38,0) | Average annual income (thousand KRW) |
| AVERAGE_INCOME_OVER_70 | NUMBER(38,0) | Average income of high earners (thousand KRW) |
| AVERAGE_HOUSEHOLD_INCOME | NUMBER(38,0) | Average household income (thousand KRW) |
| MEDIAN_INCOME | NUMBER(38,0) | Median annual income (thousand KRW) |
| RATE_INCOME_UNDER_20M | FLOAT | % with annual income <20M KRW |
| RATE_INCOME_20M_TO_30M | FLOAT | % with annual income 20–30M KRW |
| RATE_INCOME_30M_TO_40M | FLOAT | % with annual income 30–40M KRW |
| RATE_INCOME_40M_TO_50M | FLOAT | % with annual income 40–50M KRW |
| RATE_INCOME_50M_TO_60M | FLOAT | % with annual income 50–60M KRW |
| RATE_INCOME_60M_TO_70M | FLOAT | % with annual income 60–70M KRW |
| RATE_INCOME_OVER_70M | FLOAT | % with annual income >70M KRW |
| CARD_COUNT | NUMBER(38,0) | Total cardholders (persons) |
| CREDIT_CARD_COUNT | NUMBER(38,0) | Credit card holders |
| CHECK_CARD_COUNT | NUMBER(38,0) | Debit card holders |
| AVERAGE_CARD_COUNT | NUMBER(38,0) | Avg cards per person |
| AVERAGE_CREDIT_CARD_COUNT | NUMBER(38,0) | Avg credit cards per person |
| AVERAGE_CHECK_CARD_COUNT | NUMBER(38,0) | Avg debit cards per person |
| TOTAL_USAGE_AMOUNT | NUMBER(38,0) | 3-month card usage amount (thousand KRW) |
| TOTAL_SALES_USAGE_AMOUNT | NUMBER(38,0) | 3-month credit sales usage (thousand KRW) |
| TOTAL_FULL_PAYMENT_USAGE_AMOUNT | NUMBER(38,0) | 3-month lump-sum payment usage (thousand KRW) |
| TOTAL_INSTALLMENT_USAGE_AMOUNT | NUMBER(38,0) | 3-month installment usage (thousand KRW) |
| TOTAL_CASH_ADVANCE_USAGE_AMOUNT | NUMBER(38,0) | 3-month cash advance usage (thousand KRW) |
| TOTAL_CREDIT_CARD_USAGE_AMOUNT | NUMBER(38,0) | 3-month credit card usage (thousand KRW) |
| TOTAL_CHECK_CARD_USAGE_AMOUNT | NUMBER(38,0) | 3-month debit card usage (thousand KRW) |
| TOTAL_ABROAD_AMOUNT | NUMBER(38,0) | 3-month overseas consumption (thousand KRW) |
| AVERAGE_USAGE_AMOUNT | NUMBER(38,0) | Avg 3-month card usage (thousand KRW) |
| AVERAGE_SALES_USAGE_AMOUNT | NUMBER(38,0) | Avg 3-month credit sales usage (thousand KRW) |
| AVERAGE_FULL_PAYMENT_USAGE_AMOUNT | NUMBER(38,0) | Avg 3-month lump-sum payment usage (thousand KRW) |
| AVERAGE_INSTALLMENT_USAGE_AMOUNT | NUMBER(38,0) | Avg 3-month installment usage (thousand KRW) |
| AVERAGE_CASH_ADVANCE_USAGE_AMOUNT | NUMBER(38,0) | Avg 3-month cash advance usage (thousand KRW) |
| AVERAGE_CREDIT_CARD_USAGE_AMOUNT | NUMBER(38,0) | Avg 3-month credit card usage (thousand KRW) |
| AVERAGE_CHECK_CARD_USAGE_AMOUNT | NUMBER(38,0) | Avg 3-month debit card usage (thousand KRW) |
| AVERAGE_ABROAD_AMOUNT | NUMBER(38,0) | Avg 3-month overseas consumption (thousand KRW) |
| AVERAGE_TOTAL_LIMIT_AMOUNT | NUMBER(38,0) | Avg total card credit limit (thousand KRW) |
| BALANCE_COUNT | NUMBER(38,0) | Loan holders (persons) |
| BANK_BALANCE_COUNT | NUMBER(38,0) | Primary financial institution loan holders |
| NON_BANK_BALANCE_COUNT | NUMBER(38,0) | Secondary financial institution loan holders |
| CREDIT_BALANCE_COUNT | NUMBER(38,0) | Unsecured loan holders |
| HOUSING_BALANCE_COUNT | NUMBER(38,0) | Mortgage loan holders |
| MORTGAGE1_BALANCE_COUNT | NUMBER(38,0) | Deposit/securities-backed loan holders |
| MORTGAGE2_BALANCE_COUNT | NUMBER(38,0) | Asset-backed loan holders |
| AVERAGE_BALANCE_COUNT | NUMBER(38,0) | Avg number of loans per person |
| AVERAGE_BANK_BALANCE_COUNT | NUMBER(38,0) | Avg primary institution loans per person |
| AVERAGE_NON_BANK_BALANCE_COUNT | NUMBER(38,0) | Avg secondary institution loans per person |
| AVERAGE_CREDIT_BALANCE_COUNT | NUMBER(38,0) | Avg unsecured loans per person |
| AVERAGE_HOUSING_BALANCE_COUNT | NUMBER(38,0) | Avg mortgage loans per person |
| AVERAGE_MORTGAGE1_BALANCE_COUNT | NUMBER(38,0) | Avg deposit/securities-backed loans per person |
| AVERAGE_MORTGAGE2_BALANCE_COUNT | NUMBER(38,0) | Avg asset-backed loans per person |
| TOTAL_BALANCE_AMOUNT | NUMBER(38,0) | Total loan balance (thousand KRW) |
| TOTAL_BANK_BALANCE_AMOUNT | NUMBER(38,0) | Total primary institution loan balance (thousand KRW) |
| TOTAL_NON_BANK_BALANCE_AMOUNT | NUMBER(38,0) | Total secondary institution loan balance (thousand KRW) |
| TOTAL_CREDIT_BALANCE_AMOUNT | NUMBER(38,0) | Total unsecured loan balance (thousand KRW) |
| TOTAL_HOUSING_BALANCE_AMOUNT | NUMBER(38,0) | Total mortgage loan balance (thousand KRW) |
| TOTAL_MORTGAGE1_BALANCE_AMOUNT | NUMBER(38,0) | Total deposit/securities-backed loan balance (thousand KRW) |
| TOTAL_MORTGAGE2_BALANCE_AMOUNT | NUMBER(38,0) | Total asset-backed loan balance (thousand KRW) |
| AVERAGE_BALANCE_AMOUNT | NUMBER(38,0) | Avg loan balance (thousand KRW) |
| AVERAGE_BANK_BALANCE_AMOUNT | NUMBER(38,0) | Avg primary institution loan balance (thousand KRW) |
| AVERAGE_NON_BANK_BALANCE_AMOUNT | NUMBER(38,0) | Avg secondary institution loan balance (thousand KRW) |
| AVERAGE_CREDIT_BALANCE_AMOUNT | NUMBER(38,0) | Avg unsecured loan balance (thousand KRW) |
| AVERAGE_HOUSING_BALANCE_AMOUNT | NUMBER(38,0) | Avg mortgage balance (thousand KRW) |
| AVERAGE_MORTGAGE1_BALANCE_AMOUNT | NUMBER(38,0) | Avg deposit/securities-backed loan balance (thousand KRW) |
| AVERAGE_MORTGAGE2_BALANCE_AMOUNT | NUMBER(38,0) | Avg asset-backed loan balance (thousand KRW) |
| NEW_BALANCE_COUNT | NUMBER(38,0) | New loans count (cases) |
| NEW_BANK_BALANCE_COUNT | NUMBER(38,0) | New primary institution loans (cases) |
| NEW_NON_BANK_BALANCE_COUNT | NUMBER(38,0) | New secondary institution loans (cases) |
| NEW_CREDIT_BALANCE_COUNT | NUMBER(38,0) | New unsecured loans (cases) |
| NEW_HOUSING_BALANCE_COUNT | NUMBER(38,0) | New mortgage loans (cases) |
| NEW_MORTGAGE1_BALANCE_COUNT | NUMBER(38,0) | New deposit/securities-backed loans (cases) |
| NEW_MORTGAGE2_BALANCE_COUNT | NUMBER(38,0) | New asset-backed loans (cases) |
| NEW_TOTAL_BALANCE_AMOUNT | NUMBER(38,0) | Total new loan balance (thousand KRW) |
| NEW_TOTAL_BANK_BALANCE_AMOUNT | NUMBER(38,0) | Total new primary institution loan balance (thousand KRW) |
| NEW_TOTAL_NON_BANK_BALANCE_AMOUNT | NUMBER(38,0) | Total new secondary institution loan balance (thousand KRW) |
| NEW_TOTAL_CREDIT_BALANCE_AMOUNT | NUMBER(38,0) | Total new unsecured loan balance (thousand KRW) |
| NEW_TOTAL_HOUSING_BALANCE_AMOUNT | NUMBER(38,0) | Total new mortgage loan balance (thousand KRW) |
| NEW_TOTAL_MORTGAGE1_BALANCE_AMOUNT | NUMBER(38,0) | Total new deposit/securities-backed loan balance (thousand KRW) |
| NEW_TOTAL_MORTGAGE2_BALANCE_AMOUNT | NUMBER(38,0) | Total new asset-backed loan balance (thousand KRW) |
| DELINQUENT0_COUNT | NUMBER(38,0) | Mildly delinquent borrowers (persons) |
| DELINQUENT30_COUNT | NUMBER(38,0) | Moderately delinquent borrowers (persons) |
| DELINQUENT90_COUNT | NUMBER(38,0) | Severely delinquent borrowers (persons) |
| AVERAGE_DELINQUENT_COUNT | NUMBER(38,0) | Avg delinquency count (persons) |
| AVERAGE_DELINQUENT_DAYS | NUMBER(38,0) | Avg delinquency days |
| AVERAGE_MAX_DELINQUENT_DAYS | NUMBER(38,0) | Avg max delinquency days |
| AVERAGE_DELINQUENT_AMOUNT | NUMBER(38,0) | Avg delinquent amount (thousand KRW) |
| MEDIAN_DELINQUENT_AMOUNT | NUMBER(38,0) | Median delinquent amount (thousand KRW) |
| AVERAGE_SCORE | NUMBER(38,0) | Avg credit score (grade) |
| RATE_SCORE1 | FLOAT | % high credit score holders |
| RATE_SCORE2 | FLOAT | % medium credit score holders |
| RATE_SCORE3 | FLOAT | % low credit score holders |
| AVERAGE_LOAN_POTENTIAL1 | NUMBER(38,0) | Deposit capacity amount 1 (thousand KRW) |
| AVERAGE_LOAN_POTENTIAL2 | NUMBER(38,0) | Deposit capacity amount 2 (thousand KRW) |
| OWN_HOUSING_COUNT | NUMBER(38,0) | Homeowners (persons) |
| MULTIPLE_HOUSING_COUNT | NUMBER(38,0) | Multiple property owners (persons) |
| AVERAGE_ASSET_AMOUNT | NUMBER(38,0) | Avg total asset valuation (thousand KRW) |
| RATE_HIGHEND | FLOAT | % high-end target customers |


## 4. SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION

**Marketplace listing:** `GZTHZC6PR8`
**Schema:** `TELECOM_INSIGHTS` (all objects are secure views — no base tables are exposed)
**Comment:** 텔레콤 서비스 비식별 통계 데이터 - 계약, 채널, GA4, 콜센터 인사이트 (De-identified aggregated telecom statistics)
**Date range:** 2023-02-01 to 2027-06-01 (across views; bulk of data from ~2023 onward)

All 11 objects in this schema are secure views. The views contain pre-aggregated, anonymized data.

### V01_MONTHLY_REGIONAL_CONTRACT_STATS
**Comment:** 월별 지역(시도/시군구)별 상품 유형별 계약 통계 (Monthly contract stats by region and product type)
**Row count (sample):** 23,584

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month (first day of month) |
| INSTALL_STATE | VARCHAR | Installation address province (시도, e.g. 서울특별시) |
| INSTALL_CITY | VARCHAR | Installation address city/district (시군구, e.g. 강남구) |
| MAIN_CATEGORY_NAME | VARCHAR | Main product type (인터넷, 렌탈, 모바일, 알뜰 요금제 등) |
| CONTRACT_COUNT | NUMBER(18,0) | Total contracts created |
| CONSULT_REQUEST_COUNT | NUMBER(38,0) | Contracts created from consultation requests |
| REGISTEND_COUNT | NUMBER(38,0) | Registration completed count |
| OPEN_COUNT | NUMBER(38,0) | Activation completed count |
| PAYEND_COUNT | NUMBER(38,0) | Payment completed count |
| REGISTEND_CVR | NUMBER(38,2) | Registration conversion rate (%) |
| OPEN_CVR | NUMBER(38,2) | Activation conversion rate (%) |
| PAYEND_CVR | NUMBER(38,2) | Payment conversion rate (%) |
| AVG_NET_SALES | NUMBER(38,0) | Average net revenue excluding zero (KRW) |
| TOTAL_NET_SALES | NUMBER(38,0) | Total net revenue (KRW) |

### V02_SERVICE_BUNDLE_PATTERNS
**Comment:** 인터넷 서비스 결합 상품 패턴 — TV/전화/WiFi 결합 여부별 계약 현황 및 정책금 분석

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month |
| MAIN_CATEGORY_NAME | VARCHAR | Product type |
| BUNDLE_COMBINATION | VARCHAR | Bundle discount type (단독가입 or combination name) |
| HAS_TV | VARCHAR(1) | TV bundle Y/N |
| HAS_PHONE | VARCHAR(1) | Internet phone bundle Y/N |
| HAS_WIFI | VARCHAR(1) | WiFi bundle Y/N |
| CONTRACT_COUNT | NUMBER(18,0) | Contracts with this bundle pattern |
| PAYEND_COUNT | NUMBER(38,0) | Payment completed count |
| AVG_NET_SALES | NUMBER(38,0) | Average net revenue excluding zero (KRW) |
| AVG_POLICY_AMOUNT | NUMBER(38,0) | Average total policy subsidy excluding zero (KRW) |

### V03_CONTRACT_FUNNEL_CONVERSION
**Comment:** 월별 상품 유형별 계약 퍼널 전환율 — 상담요청→청약→접수→개통→지급 단계별 CVR

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month |
| MAIN_CATEGORY_NAME | VARCHAR | Product type |
| TOTAL_COUNT | NUMBER(18,0) | Total contracts (funnel top) |
| CONSULT_REQUEST_COUNT | NUMBER(38,0) | Consultation request count |
| SUBSCRIPTION_COUNT | NUMBER(38,0) | Subscription (청약) count |
| REGISTEND_COUNT | NUMBER(38,0) | Registration completed count |
| OPEN_COUNT | NUMBER(38,0) | Activation completed count |
| PAYEND_COUNT | NUMBER(38,0) | Payment completed count |
| CVR_CONSULT_REQUEST | NUMBER(38,2) | Consult CVR (%) = consult / total |
| CVR_SUBSCRIPTION | NUMBER(38,2) | Subscription CVR (%) = subscription / consult |
| CVR_REGISTEND | NUMBER(38,2) | Registration CVR (%) = registration / subscription |
| CVR_OPEN | NUMBER(38,2) | Activation CVR (%) = activation / registration |
| CVR_PAYEND | NUMBER(38,2) | Payment CVR (%) = payment / activation |
| OVERALL_CVR | NUMBER(38,2) | Overall CVR (%) = payment / total |

### V04_CHANNEL_CONTRACT_PERFORMANCE
**Comment:** 유입 채널(접수 경로/마케팅 경로)별 계약 성과 — 개통률, 지급률, 매출 포함

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month |
| MAIN_CATEGORY_NAME | VARCHAR | Product type |
| RECEIVE_PATH_NAME | VARCHAR | Reception channel (e.g. 콜센터, 셀프접수, 파트너) |
| INFLOW_PATH_NAME | VARCHAR | Marketing inflow channel (e.g. 그로스마케팅, 랜딩페이지, 바로상담) |
| CONTRACT_COUNT | NUMBER(18,0) | Total contracts for this channel |
| REGISTEND_COUNT | NUMBER(38,0) | Registration completed count |
| OPEN_COUNT | NUMBER(38,0) | Activation completed count |
| PAYEND_COUNT | NUMBER(38,0) | Payment completed count |
| OPEN_CVR | NUMBER(38,2) | Activation conversion rate (%) |
| PAYEND_CVR | NUMBER(38,2) | Payment conversion rate (%) |
| AVG_NET_SALES | NUMBER(38,0) | Average net revenue excluding zero (KRW) |
| TOTAL_NET_SALES | NUMBER(38,0) | Total net revenue (KRW) |

### V05_REGIONAL_NEW_INSTALL
**Comment:** 지역별 인터넷 신규 설치 현황 — 단독/결합 비율, 개통/지급 건수, 평균 매출

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month |
| INSTALL_STATE | VARCHAR | Province |
| INSTALL_CITY | VARCHAR | City/district |
| CONTRACT_COUNT | NUMBER(18,0) | Total internet contracts |
| OPEN_COUNT | NUMBER(38,0) | Activation completed count |
| PAYEND_COUNT | NUMBER(38,0) | Payment completed count |
| BUNDLE_COUNT | NUMBER(13,0) | Bundle (TV/phone) contract count |
| STANDALONE_COUNT | NUMBER(13,0) | Internet-only contract count |
| AVG_NET_SALES | NUMBER(38,0) | Average net revenue excluding zero (KRW) |

### V06_RENTAL_CATEGORY_TRENDS
**Comment:** 렌탈 상품 카테고리(대분류/소분류)별 월별 트렌드 — 지역, 개통/지급 전환율, 정책금 포함

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month |
| INSTALL_STATE | VARCHAR | Province |
| RENTAL_MAIN_CATEGORY | VARCHAR | Rental main category (e.g. 정수기, 공기청정기) |
| RENTAL_SUB_CATEGORY | VARCHAR | Rental sub-category (brand/model line) |
| CONTRACT_COUNT | NUMBER(18,0) | Contract count |
| OPEN_COUNT | NUMBER(38,0) | Activation completed count |
| PAYEND_COUNT | NUMBER(38,0) | Payment completed count |
| OPEN_CVR | NUMBER(38,2) | Activation CVR (%) |
| PAYEND_CVR | NUMBER(38,2) | Payment CVR (%) |
| AVG_NET_SALES | NUMBER(38,0) | Average net revenue excluding zero (KRW) |
| AVG_POLICY_AMOUNT | NUMBER(38,0) | Average total policy subsidy excluding zero (KRW) |

### V07_GA4_MARKETING_ATTRIBUTION
**Comment:** GA4 기반 UTM 소스/매체별 마케팅 전환 분석 — 세션, 상담요청, 계약 CVR, 매출 포함

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month |
| UTM_SOURCE | VARCHAR | UTM source (e.g. google, naver, facebook, (direct)) |
| UTM_MEDIUM | VARCHAR | UTM medium (e.g. cpc, organic, referral, (none)) |
| TOTAL_SESSIONS | NUMBER(30,0) | Total sessions |
| TOTAL_USERS | NUMBER(30,0) | Total users |
| TOTAL_CONSULT_REQUESTS | NUMBER(30,0) | Consultation request events |
| TOTAL_CONSULT_PHONE | NUMBER(30,0) | Phone consultation clicks |
| TOTAL_CONSULT_KAKAO | NUMBER(30,0) | KakaoTalk consultation clicks |
| TOTAL_CONSULT_CHANNELTALK | NUMBER(30,0) | ChannelTalk consultation clicks |
| TOTAL_CONTRACTS | NUMBER(30,0) | Total contracts |
| CONSULT_CVR | NUMBER(38,2) | Consultation CVR (%) = consult / session |
| CONTRACT_CVR | NUMBER(38,2) | Contract CVR (%) = contract / session |
| TOTAL_REVENUE | NUMBER(38,0) | Total revenue (KRW) |

### V08_GA4_DEVICE_STATS
**Comment:** GA4 디바이스 유형별(모바일/데스크탑/태블릿) 세션 수, 사용자 수, 전환 이벤트 집계 (from 2025-01-01 onward)

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month (2025-01-01 onward) |
| DEVICE_CATEGORY | VARCHAR | Device type (mobile, desktop, tablet) |
| SESSION_COUNT | NUMBER(30,0) | Total sessions (session_start event basis) |
| USER_COUNT | NUMBER(30,0) | Unique users per session |
| CONVERSION_EVENT_COUNT | NUMBER(30,0) | Total conversion events |
| CONVERSION_RATE | NUMBER(38,2) | Conversion rate (%) = conversion events / sessions |

### V09_MONTHLY_CALL_STATS
**Comment:** 월별 콜센터 통화 통계 — 수신/발신 구분, 상품 유형별 통화량, 평균 통화 시간, 연결률

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month |
| DIVISION_NAME | VARCHAR | Call direction (수신=inbound, 발신=outbound) |
| MAIN_CATEGORY_NAME | VARCHAR | Consulting product type (인터넷, 렌탈, 모바일, etc.) |
| CALL_COUNT | NUMBER(18,0) | Total call attempts |
| AVG_BILL_SECOND | NUMBER(38,1) | Average billed duration excluding zero (seconds) |
| AVG_BILL_MINUTE | NUMBER(38,1) | Average billed duration excluding zero (minutes) |
| MAX_BILL_SECOND | NUMBER(38,0) | Maximum call duration (seconds) |
| TOTAL_BILL_SECOND | NUMBER(38,0) | Total call duration sum (seconds) |
| CONNECTED_COUNT | NUMBER(13,0) | Successfully connected calls |
| UNCONNECTED_COUNT | NUMBER(13,0) | Failed/unconnected calls |
| CONNECTION_RATE | NUMBER(23,2) | Connection rate (%) = connected / total attempts |

### V10_HOURLY_CALL_DISTRIBUTION
**Comment:** 콜센터 시간대(0~23시) × 요일별 통화 분포 — 피크타임 및 연결률 분석용

| Column | Type | Description |
|---|---|---|
| HOUR_OF_DAY | NUMBER(2,0) | Hour of call (0–23) |
| DAY_OF_WEEK | NUMBER(2,0) | Day of week number (1=Sunday, 2=Monday, …, 7=Saturday) |
| DAY_OF_WEEK_NAME | VARCHAR | Day name (일/월/화/수/목/금/토) |
| DIVISION_NAME | VARCHAR | Call direction (수신/발신) |
| CALL_COUNT | NUMBER(18,0) | Total calls in this hour/day slot |
| CONNECTED_COUNT | NUMBER(13,0) | Connected calls |
| UNCONNECTED_COUNT | NUMBER(13,0) | Unconnected calls |
| CONNECTION_RATE | NUMBER(23,2) | Connection rate (%) |
| AVG_BILL_SECOND | NUMBER(38,1) | Average billed duration excluding zero (seconds) |
| AVG_BILL_MINUTE | NUMBER(38,1) | Average billed duration excluding zero (minutes) |

### V11_CALL_TO_CONTRACT_CONVERSION
**Comment:** 콜센터 통화에서 계약서 생성까지의 전환율 및 리드타임 분석 (평균/중앙값)

| Column | Type | Description |
|---|---|---|
| YEAR_MONTH | DATE | Reference month (based on call request date) |
| MAIN_CATEGORY_NAME | VARCHAR | Consulting product type |
| DIVISION_NAME | VARCHAR | Call direction (수신/발신) |
| TOTAL_CALLS | NUMBER(18,0) | Total call count |
| LINKED_CONTRACTS | NUMBER(18,0) | Unique contracts linked to calls |
| CALLS_PER_CONTRACT | NUMBER(25,1) | Avg calls per contract |
| CALL_TO_CONTRACT_CVR | NUMBER(28,2) | Call-to-contract conversion rate (%) |
| AVG_LEADTIME_DAYS | NUMBER(28,1) | Avg lead time from call to contract creation (days) |
| MEDIAN_LEADTIME_DAYS | NUMBER(13,1) | Median lead time from call to contract creation (days) |
