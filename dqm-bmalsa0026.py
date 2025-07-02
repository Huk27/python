import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from datalabQuery import QueryProcessor

real_q_processor = QueryProcessor(clear_auth_tf=False)

# --- 1. 현재 검증 대상 데이터 로드 (DataFrame) ---
# 실제로는 real_q_processor.fetch_to_pandas("SELECT * FROM USER_PROVIDED_TABLE WHERE ced = '오늘날짜'") 등으로 가져옵니다.
# 여기서는 테스트를 위해 샘플 DataFrame을 직접 생성합니다.
current_validation_date = datetime.now().strftime("%Y%m%d")
# print(current_data_df.head()) # 필요시 주석 해제
print("-" * 30)

table_name = 'mdb.bmalsa0026'
current_data_query = f"SELECT * from {table_name} where bgda_plf_pti_id = {current_validation_date}"
current_data_df = real_q_processor.fetch_to_pandas(query=current_data_query, engine="hive", limit=None)
current_data_df.attrs['name'] = table_name
partition_key_column = "bgda_plf_pti_id"

# --- 2. 검증 규칙 설정 (제공된 테이블 스키마 기반) ---
# USER_PROVIDED_TABLE 이라는 테이블명을 사용하고, bgda_plf_pti_id를 날짜 컬럼으로 사용한다고 가정
rules_config = {
    "columns": {
        "ced": [
            {"name": "CED 검증", "type": "not_null", "message": "기준일자(ced)는 필수입니다."},
            {"name": "CED 포맷검증(YYYYMMDD)", "type": "regex_pattern", "params": {"pattern": r"^\d{8}$"},
             "message": "기준일자(ced)는 YYYYMMDD 형식이어야 합니다."}
        ],
        "wid_cty_cd": [
            {"name": "wid_cty_cd_not_null", "type": "not_null"},
            #                         {"name": "wid_cty_cd_allowed", "type": "allowed_values", "params": {"values": ["11", "21", "31", "41", "51"]}, "message": "유효하지 않은 광역도시코드입니다."} # 예시 허용 값
        ],
        "kto_mct_ccd_vl": [
            {"name": "kto_ccd_vl 코드 빈도 변화(vs 비정상기간)",
             "type": "distribution_change",
             "params": {
                 "current_data_filter": f"bgda_plf_pti_id == '{current_validation_date}'",
                 # <<<< 추가된 파라미터 // 과거 분포는 "지난달 전체 데이터"의 kto_mct_ccd_vl 분포
                 "historical_data_table": table_name,
                 "historical_data_column": "kto_mct_ccd_vl",
                 "historical_data_filter": "{partition_key_column} >= '{hist_start}' AND {partition_key_column} <= '{hist_end}'",
                 "historical_data_query_params": {
                     "partition_key_column": partition_key_column,
                     "hist_start": "20250101",  # 예시: 지난달 시작
                     "hist_end": "20250115"  # 예시: 지난달 종료
                 }, "thresholds": {
                     "new_code_max_ratio": 0.2,
                     "freq_change_tolerance_abs": 0.1,
                     "unique_count_tolerance_ratio": 0.05
                 },
             }
             },
            {"name": "kto_ccd_vl 코드 빈도 변화 (정상기간)",
             "type": "distribution_change",
             "params": {
                 "current_data_filter": f"bgda_plf_pti_id == '{current_validation_date}'",
                 # <<<< 추가된 파라미터 // 과거 분포는 "지난달 전체 데이터"의 kto_mct_ccd_vl 분포
                 "historical_data_table": table_name,
                 "historical_data_column": "kto_mct_ccd_vl",
                 "historical_data_filter": "{partition_key_column} >= '{hist_start}' AND {partition_key_column} <= '{hist_end}'",
                 "historical_data_query_params": {
                     "partition_key_column": partition_key_column,
                     "hist_start": "20250501",  # 예시: 지난달 시작
                     "hist_end": "20250515"  # 예시: 지난달 종료
                 }, "thresholds": {
                     "new_code_max_ratio": 0.2,
                     "freq_change_tolerance_abs": 0.1,
                     "unique_count_tolerance_ratio": 0.05
                 }
             }
             }
        ],
        "mct_ue_cln_tcd_vl": [
            {"name": "mct_customer_type_allowed", "type": "allowed_values", "params": {"values": ["외지인", "내지인"]}}
            # T3_INVALID는 여기서 걸림
        ],
        "aso_saa": [
            {"name": "ASO_SAA NULL 검증", "type": "not_null", "message": "추정매출금액은 비어있을 수 없습니다."},
            {"name": "ASO_SAA NULL 범위 검증", "type": "numeric_range", "params": {"min": 0},
             "message": "추정매출금액은 0 이상이어야 합니다."},
            #             {"name": "ASO_SAA 통계적 검수(그룹핑)", "type": "numeric_volatility",
            #                  "params": {
            #                      "method": "iqr",
            #                      "group_by_columns": ["wid_cty_cd", "hpsn_bzn_cd"],
            #                      "current_data_filter": f"{partition_key_column} == '{current_validation_date}'",
            #                      "historical_data_table": table_name,
            #                      "historical_data_column": "aso_saa",
            #                      "historical_data_filter": "{partition_key_column} >= '{start_date_prev_month}' AND {partition_key_column} <= '{end_date_prev_month}'",
            #                      "historical_data_query_params": {
            #                          "partition_key_column": partition_key_column,
            #                          "start_date_prev_month": (datetime.strptime(current_validation_date, "%Y%m%d") - relativedelta
            #                              (months=2)).replace(day=1).strftime("%Y%m%d"),
            #                          "end_date_prev_month":
            #                                      (datetime.strptime(current_validation_date, "%Y%m%d").replace(day=1) - timedelta(
            #                                  days=1)).strftime("%Y%m%d")
            #                      },
            #                      "thresholds": {"iqr_multiplier": 2.0},  # IQR 배수를 좀 더 관대하게 설정
            #                  }
            #             }
        ],
        "bgda_plf_pti_id": [
            {"name": "partition_id_format", "type": "regex_pattern", "params": {"pattern": r"^\d{8}$"}}
        ]
    },
    "table_level_rules": [
        {
            "name": "도시별_매출합계_7일_연속하락_감지",
            "type": "consecutive_trend_check",  # 규칙 타입
            "params": {
                "column_to_aggregate": "aso_saa",  # 집계 대상 컬럼
                "aggregate_function": "SUM",  # 사용할 집계 함수 ("SUM", "AVG", "COUNT")
                "group_by_columns": ["wid_cty_cd"],  # (선택적) 그룹핑 기준 컬럼
                "date_column_for_trend": partition_key_column,  # (필수) 일별 추세를 확인할 날짜 컬럼명 (YYYYMMDD 형식 가정)
                "trend_type": "down",  # (필수) 감지할 추세 유형: "down" (하락) 또는 "up" (상승)
                "consecutive_periods": 7,  # (필수) 연속되어야 하는 기간 수 (아래 period_unit 기준)
                "period_unit": "days",  # (필수) 기간 단위 (현재는 "days"만 지원)

                # 과거 데이터 조회 관련 설정
                "historical_data_table": table_name,
                "historical_lookback_periods": 6,  # (필수) 추세 분석을 위해 조회할 과거 기간 수 (consecutive_periods 이상)
                "historical_base_filter": "aso_saa > 0",  # (선택적) 과거 데이터 조회 시 추가할 기본 SQL WHERE 조건
                "engine": "hive",  # (선택적, QueryProcessor에 따라 필요할 수 있음)

                # (선택적) 현재 DataFrame에 이 규칙 실행 전 적용할 필터
                #                 "current_data_filter": f"{partition_key_column} == '{current_validation_date}'"
            },
            "message": "그룹 {group_key}의 {column_to_aggregate} {aggregate_function} 값이 {consecutive_periods_detected}일({period_unit}) 연속 {trend_type} 추세를 보였습니다. 최근값: {trend_values[-1]}, 추세 기간: {trend_dates[0]} ~ {trend_dates[-1]}"
        },
        {"name": "스키마_자동_변경_감지",
         "type": "schema_change_check",
         "params": {
             "table_name_in_db": table_name,
             "engine": "hive",
             "auto_manage_baseline": True,
             "check_options": {
                 "detect_new_columns": True,
                 "detect_missing_columns": True,
                 "detect_type_changes": True,
                 "detect_nullable_changes": True
             },
             "force_update_baseline": False,
             # 첫 실행 시 또는 기준을 강제 업데이트하고 싶을 때 True로 설정, 운영 시에는 False 또는 생략하고, 변경 감지 시 수동으로 baseline 파일 업데이트
             "update_baseline_if_no_change": False  # 스키마 변경이 없을 때도 baseline 파일의 timestamp 등을 갱신할지 여부
         }
         },
        {"name": "행 COUNT 검증 vs 이전 7일 평균", "type": "total_row_count_trend",
         "params": {
             "current_period_value": current_validation_date,  # YYYYMMDD 형식
             "historical_data_table": table_name,
             "date_column_for_period": partition_key_column,  # DB의 일별 날짜 컬럼
             "date_column_format": "YYYYMMDD",  # DB 컬럼 형식 명시
             "comparison_periods": {
                 "type": "average_of_previous_n_days",
                 "n": 7  # 어제 데이터와 비교
             },
             "threshold_ratio_decrease": 0.15,
             "threshold_ratio_increase": 0.15
         }},
        {
            "name": "전체_매출합계_과거7일대비_변동",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "current_period_value": current_validation_date,  # 오늘 날짜 YYYYMMDD

                "historical_data_table": table_name,  # 실제 과거 데이터 테이블명
                "date_column_for_period": partition_key_column,  # 과거 테이블의 일별 날짜 컬럼
                "date_column_format": "YYYYMMDD",  # 해당 컬럼의 형식

                "comparison_periods": {
                    "type": "average_of_previous_n_days",
                    "n": 7
                },
                "threshold_ratio_increase": 0.15,  # 20% 이상 증가 시 오류
                "threshold_ratio_decrease": 0.15  # 20% 이상 감소 시 오류
            }
        },
        {
            "name": "도시별_매출합계_과거7일대비_변동",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "group_by_columns": ["wid_cty_cd"],
                "current_period_value": current_validation_date,
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMMDD",
                "comparison_periods": {
                    "type": "average_of_previous_n_days",
                    "n": 7
                },
                "threshold_ratio_increase": 0.20,  # 그룹별 임계치는 다르게 설정 가능
                "threshold_ratio_decrease": 0.20,
                "message": "도시코드 [{group_key}]의 매출합계({current_value:.0f})가 어제({historical_value:.0f}) 대비 크게 변동(변화율: {change_ratio:.2%})."
            }
        }
    ]

}

# --- 3. 검증 실행 ---
print("\n>>> 실제 DB 대상 검증 시작 (MyActualDatabaseQueryProcessor 사용) <<<\n")

detailed_errors, summary_report = run_data_validation(
    dataframe=current_data_df,
    rules_config=rules_config,
    query_processor_instance=real_q_processor,
    save_to_hive=True,  # Hive 저장 활성화
    hive_db_name="tdb",
    hive_validation_runs_table_name="dqm_validation_runs",
    hive_summary_table_name="dqm_rule_execution_summary",
    hive_errors_table_name="dqm_detailed_errors",
    hive_partition_value=current_validation_date,  # 파티션 값
    hive_partition_column_name="dt",  # Hive 테이블의 파티션 컬럼명
    hive_save_mode_is_overwrite=False,  # False면 append (파티션 덮어쓰기는 True)
    max_errors_to_log=50
)

if detailed_errors:
    print(f"\n>>> 검증 완료: . 결과 파일을 확인하세요.")
else:
    print("\n>>> 검증 완료: 모든 검증을 통과했습니다.")