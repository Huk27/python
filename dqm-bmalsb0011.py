import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import argparse  # argparse 라이브러리 임포트

# dqmlib.py와 datalabQuery.py가 있는 경로를 sys.path에 추가하거나,
# 같은 디렉토리에 위치시켜야 합니다.
from dqmlib import run_data_validation
from datalabQuery import QueryProcessor

# ====================================================================
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼ 인자 처리 로직 ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# ====================================================================
parser = argparse.ArgumentParser(description="mdb.bmalsb0011 테이블에 대한 데이터 검증을 실행합니다.")
parser.add_argument(
    'date',
    nargs='?',
    help="검증을 수행할 날짜 (YYYYMMDD 형식). 지정하지 않으면 오늘 날짜로 실행됩니다."
)
args = parser.parse_args()

if args.date:
    try:
        datetime.strptime(args.date, '%Y%m%d')
        current_validation_date = args.date
        print(f"INFO: 인자로 받은 날짜 '{current_validation_date}'를 기준으로 검증을 시작합니다.")
    except ValueError:
        print(f"오류: 날짜 형식이 잘못되었습니다. YYYYMMDD 형식으로 입력해주세요. (입력값: {args.date})")
        exit(1)
else:
    current_validation_date = datetime.now().strftime("%Y%m%d")
    print(f"INFO: 인자가 없어 오늘 날짜 '{current_validation_date}'를 기준으로 검증을 시작합니다.")
# ====================================================================

# 실제 DB 연결을 위한 QueryProcessor 인스턴스 생성
real_q_processor = QueryProcessor(clear_auth_tf=False)


# --- 1. 현재 검증 대상 데이터 로드 ---
print("-" * 30)

table_name = 'mdb.bmalsb0011'
partition_key_column = "bgda_plf_pti_id"

current_data_query = f"SELECT * from {table_name} where {partition_key_column} = {current_validation_date}"
# 실제 운영 시에는 아래 쿼리를 사용합니다.
current_data_df = real_q_processor.fetch_to_pandas(query=current_data_query, engine="hive", limit=None)
current_data_df.attrs['name'] = table_name


# --- 2. 검증 규칙 설정 ---
rules_config = {
    "columns": {
        "bgda_plf_pti_id": [
            {"name": "파티션ID_필수값", "type": "not_null"},
            {"name": "파티션ID_형식(YYYYMMDD)", "type": "regex_pattern", "params": {"pattern": r"^\d{8}$"}}
        ],
        "ced": [
            {"name": "기준일자_필수값", "type": "not_null"},
            {"name": "기준일자_형식(YYYYMMDD)", "type": "regex_pattern", "params": {"pattern": r"^\d{8}$"}}
        ],
        "apv_tm": [
            {"name": "승인시간_필수값", "type": "not_null"},
            {"name": "승인시간_형식(HHMMSS)", "type": "regex_pattern", "params": {"pattern": r"^\d{6}$"}}
        ],
        "vs_iss_crd_ntn_nm": [
            {"name": "해외발급카드국가명_필수값", "type": "not_null"},
            {
                "name": "해외발급카드국가명_분포변화",
                "type": "distribution_change",
                "params": {
                    "historical_data_table": table_name,
                    "historical_data_column": "vs_iss_crd_ntn_nm",
                    # 비교 기준: 지난달 같은 날의 국가별 분포
                    "historical_data_filter": "{partition_key_column} = '{hist_date}'",
                    "historical_data_query_params": {
                        "partition_key_column": partition_key_column,
                        "hist_date": (datetime.strptime(current_validation_date, "%Y%m%d") - relativedelta(months=1)).strftime("%Y%m%d")
                    },
                    "thresholds": {"new_code_max_ratio": 0.1, "freq_change_tolerance_abs": 0.05}
                }
            }
        ],
        "onl_st_bne_ccd": [
            {"name": "온라인결제구분_허용값", "type": "allowed_values", "params": {"values": ["ON", "OFF"]}}
        ],
        "mct_ry_nm": [
            {"name": "가맹점업종명_필수값", "type": "not_null"}
        ],
        "dl_aso_saa": [
            {"name": "상세추정매출금액_필수값", "type": "not_null"},
            {"name": "상세추정매출금액_범위(0이상)", "type": "numeric_range", "params": {"min": 0}},
            {
                "name": "상세추정매출금액_이상치탐지(IQR)",
                "type": "numeric_volatility",
                "params": {
                    "method": "iqr",
                    "group_by_columns": ["mct_ry_nm"], # 업종별로 그룹지어 이상치 탐지
                    "historical_data_table": table_name,
                    "historical_data_column": "dl_aso_saa",
                    # 비교 기준: 지난 7일간의 데이터
                    "historical_data_filter": "{partition_key_column} >= '{start_date}' AND {partition_key_column} < '{end_date}'",
                    "historical_data_query_params": {
                        "partition_key_column": partition_key_column,
                        "start_date": (datetime.strptime(current_validation_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d"),
                        "end_date": current_validation_date
                    },
                    "thresholds": {"iqr_multiplier": 3.0}, # 일반적인 이상치보다 관대한 3.0배수 적용
                }
            }
        ],
        "dl_aso_sls_ct": [
            {"name": "상세추정매출건수_필수값", "type": "not_null"},
            {"name": "상세추정매출건수_범위(0이상)", "type": "numeric_range", "params": {"min": 0}}
        ]
    },
    "table_level_rules": [
        {
            "name": "스키마_자동_변경_감지",
            "type": "schema_change_check",
            "params": {
                "table_name_in_db": table_name,
                "engine": "hive",
                "auto_manage_baseline": True, # 첫 실행 시 현재 스키마를 기준으로 자동 생성
            }
        },
        {
            "name": "일일_총거래건수_추이분석",
            "type": "total_row_count_trend",
            "params": {
                "current_period_value": current_validation_date,
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMMDD",
                # 비교 기준: 지난 7일간의 일평균 거래 건수
                "comparison_periods": {"type": "average_of_previous_n_days", "n": 7},
                "threshold_ratio_decrease": 0.3, # 7일 평균 대비 30% 이상 감소 시 경고
                "threshold_ratio_increase": 0.5  # 7일 평균 대비 50% 이상 증가 시 경고
            }
        },
        {
            "name": "국가별_일일_매출합계_추이분석",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "dl_aso_saa",
                "aggregate_function": "SUM",
                "group_by_columns": ["vs_iss_crd_ntn_nm"], # 국가별로 그룹지어 분석
                "current_period_value": current_validation_date,
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMMDD",
                "comparison_periods": {"type": "average_of_previous_n_days", "n": 7},
                "threshold_ratio_decrease": 0.4, # 특정 국가의 매출이 40% 이상 급감 시 경고
            }
        },
        {
            "name": "국가별_매출액_7일_연속감소_탐지",
            "type": "consecutive_trend_check",
            "params": {
                "column_to_aggregate": "dl_aso_saa",  # 집계 대상: 상세추정매출금액
                "aggregate_function": "SUM",  # 집계 함수: 합계
                "group_by_columns": ["vs_iss_crd_ntn_nm"],  # 그룹 기준: 국가명
                "date_column_for_trend": partition_key_column,  # 추세 기준: 일자 파티션
                "trend_type": "down",  # 감지할 추세: 하락
                "consecutive_periods": 7,  # 연속 기간: 7일
                "historical_data_table": table_name,
                "historical_lookback_periods": 6,  # 추세 분석을 위해 조회할 과거 기간
                "engine": "hive",
            },
            "message": "경고: 국가 [{group_key}]의 매출액이 {consecutive_periods_detected}일 연속 감소 추세입니다. (최근값: {latest_value:,.0f})"
        }
    ]
}

# --- 3. 검증 실행 ---
print("\n>>> 실제 DB 대상 검증 시작 <<<\n")

detailed_errors, summary_report = run_data_validation(
    dataframe=current_data_df,
    rules_config=rules_config,
    query_processor_instance=real_q_processor,
    save_to_hive=True,
    hive_db_name="tdb",
    hive_validation_runs_table_name="dqm_validation_runs",
    hive_summary_table_name="dqm_rule_execution_summary",
    hive_errors_table_name="dqm_detailed_errors",
    hive_partition_value=current_validation_date,
    hive_partition_column_name="dt",
    hive_save_mode_is_overwrite=False,
    max_errors_to_log=100
)

# --- 4. 결과 출력 ---
if detailed_errors:
    print(f"\n>>> 검증 완료: 총 {len(detailed_errors)}개의 오류가 발견되었습니다.")
else:
    print("\n>>> 검증 완료: 모든 검증을 통과했습니다.")