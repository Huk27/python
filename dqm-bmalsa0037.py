import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import argparse

# dqmlib.py와 datalabQuery.py가 있는 경로를 sys.path에 추가하거나,
# 같은 디렉토리에 위치시켜야 합니다.
from dqmlib import run_data_validation
from datalabQuery import QueryProcessor

# ====================================================================
parser = argparse.ArgumentParser(description="월별 테이블에 대한 데이터 검증을 실행합니다.")
parser.add_argument(
    'date',
    nargs='?',
    help="기준일자 (YYYYMMDD 형식). 지정하지 않으면 오늘 날짜를 기준으로 전월 데이터를 검증합니다."
)
args = parser.parse_args()

base_date_obj = None
try:
    if args.date:
        # 인자가 주어진 경우, YYYYMMDD 형식인지 확인하고 datetime 객체로 변환
        if len(args.date) != 8:
            raise ValueError("날짜 길이는 8자리여야 합니다.")
        base_date_obj = datetime.strptime(args.date, '%Y%m%d')
        print(f"INFO: 인자로 받은 기준일자 '{args.date}'를 사용합니다.")
    else:
        # 인자가 없는 경우, 오늘 날짜를 사용
        base_date_obj = datetime.now()
        print(f"INFO: 인자가 없어 오늘 날짜를 기준으로 합니다.")

    # 기준 날짜(base_date_obj)에서 한 달을 빼서 최종 검증 대상 년월을 계산
    target_month_date = base_date_obj - relativedelta(months=1)
    current_validation_month = target_month_date.strftime("%Y%m")
    print(f"INFO: 기준일자에 따라 '{current_validation_month}'월 데이터를 검증합니다.")

except ValueError as e:
    print(f"오류: 기준일자 형식이 잘못되었습니다. YYYYMMDD 형식으로 입력해주세요. (입력값: {args.date}, {e})")
    exit(1)
# ====================================================================

# 실제 DB 연결을 위한 QueryProcessor 인스턴스 생성
real_q_processor = QueryProcessor(clear_auth_tf=False)

# --- 1. 현재 검증 대상 데이터 로드 ---
print("-" * 30)

table_name = 'mdb.bmalsa0037'
partition_key_column = "bgda_plf_pti_id"

current_data_query = f"SELECT * from {table_name} where {partition_key_column} = {current_validation_month}"
current_data_df = real_q_processor.fetch_to_pandas(query=current_data_query, engine="hive", limit=None)
current_data_df.attrs['name'] = table_name

# --- 2. 검증 규칙 설정 ---
rules_config = {
    "columns": {
        "ta_ym": [
            {"name": "기준년월_필수값", "type": "not_null"},
            {"name": "기준년월_형식(YYYYMM)", "type": "regex_pattern", "params": {"pattern": r"^\d{6}$"}},
        ],
        "mct_ue_cln_tcd_vl": [
             {"name": "고객유형(국가명)_필수값", "type": "not_null"},
        ],
        "tmt_vl": [
            {"name": "시간대값_필수값", "type": "not_null"},
            {"name": "시간대값_허용값", "type": "allowed_values", "params": {"values": ["오전", "오후", "저녁", "심야"]}, "message": "시간대값은 '오전', '오후', '저녁', '심야' 중 하나여야 합니다."}
        ],
        "aso_saa": [
            {"name": "추정매출금액_필수값", "type": "not_null"},
            {"name": "추정매출금액_범위(0이상)", "type": "numeric_range", "params": {"min": 0}}
        ],
    },
    "table_level_rules": [
        {
            "name": "스키마_자동_변경_감지",
            "type": "schema_change_check",
            "params": {
                "table_name_in_db": table_name,
                "engine": "hive",
                "auto_manage_baseline": True,
            }
        },
        {
            "name": "월별_총온라인매출_전년동월대비_추이",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "current_period_value": current_validation_month,
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMM",
                "comparison_periods": {"type": "previous_n_months", "n": 12}, # 전년 동월과 비교
                "threshold_ratio_decrease": 0.3, # 전년 동월 대비 30% 이상 감소 시 경고
                "threshold_ratio_increase": 0.5  # 전년 동월 대비 50% 이상 증가 시 경고
            }
        },
        {
            "name": "해외국가별_온라인매출_전월대비_추이",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "group_by_columns": ["mct_ue_cln_tcd_vl"], # 고객유형(국가명)으로 그룹핑
                "current_period_value": current_validation_month,
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMM",
                "comparison_periods": {"type": "previous_n_months", "n": 1}, # 전월과 비교
                "threshold_ratio_decrease": 0.2, # 전월 대비 20% 이상 변동 시 경고
                "threshold_ratio_increase": 0.2,
                "min_value_threshold": 50_000_000 # 월 매출 5천만원 이상인 국가만 대상으로 함
            }
        },
        {
            "name": "시간대별_온라인매출_전년동월대비_추이",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "group_by_columns": ["tmt_vl"], # 시간대 값으로 그룹핑
                "current_period_value": current_validation_month,
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMM",
                "comparison_periods": {"type": "previous_n_months", "n": 12}, # 전년 동월과 비교
                "threshold_ratio_decrease": 0.15,
                "threshold_ratio_increase": 0.15,
                "message": "주의: {group_key} 시간대 온라인 매출이 전년 동월 대비 {change_ratio:.2%} 변동했습니다."
            }
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
    hive_partition_value=current_validation_month,
    hive_partition_column_name="dt",
    hive_save_mode_is_overwrite=False,
    max_errors_to_log=100
)

# --- 4. 결과 출력 ---
if detailed_errors:
    print(f"\n>>> 검증 완료: 총 {len(detailed_errors)}개의 오류가 발견되었습니다.")
else:
    print("\n>>> 검증 완료: 모든 검증을 통과했습니다.")
