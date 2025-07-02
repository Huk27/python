# dqm-bmalsa0025.py 파일의 검증 실행 전에 아래 코드를 추가합니다.

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
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼ 인자 처리 로직 (월별) ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
# ====================================================================
parser = argparse.ArgumentParser(description="mdb.bmalsa0025 테이블에 대한 데이터 검증을 실행합니다.")
parser.add_argument(
    'yyyymm',
    nargs='?',
    help="검증을 수행할 년월 (YYYYMM 형식). 지정하지 않으면 현재 시스템 날짜의 년월로 실행됩니다."
)
args = parser.parse_args()

if args.yyyymm:
    try:
        datetime.strptime(args.yyyymm, '%Y%m')
        if len(args.yyyymm) != 6:
            raise ValueError("날짜 길이는 6자리여야 합니다.")
        current_validation_month = args.yyyymm
        print(f"INFO: 인자로 받은 년월 '{current_validation_month}'를 기준으로 검증을 시작합니다.")
    except ValueError as e:
        print(f"오류: 년월 형식이 잘못되었습니다. YYYYMM 형식으로 입력해주세요. (입력값: {args.yyyymm}, {e})")
        exit(1)
else:
    current_validation_month = datetime.now().strftime("%Y%m")
    print(f"INFO: 인자가 없어 현재 년월 '{current_validation_month}'를 기준으로 검증을 시작합니다.")
# ====================================================================

# 실제 DB 연결을 위한 QueryProcessor 인스턴스 생성
real_q_processor = QueryProcessor(clear_auth_tf=False)

# --- 1. 현재 검증 대상 데이터 로드 ---
print("-" * 30)

table_name = 'mdb.bmalsa0025'
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
            {"name": "기준년월_파티션키_일치", "type": "allowed_values", "params": {"values": [current_validation_month]},
             "message": f"기준년월(ta_ym)의 값이 파티션키({current_validation_month})와 일치해야 합니다."}
        ],
        "wid_cty_cd": [
            {"name": "광역도시코드_필수값", "type": "not_null"}
        ],
        "sex_ccd": [
            {"name": "성별구분코드_허용값", "type": "allowed_values", "params": {"values": ["1", "2"]}, "message": "성별 코드는 1(남성) 또는 2(여성)여야 합니다."}
        ],
        "age_ccd": [
            {"name": "연령구분코드_필수값", "type": "not_null"},
        ],
        "aso_saa": [
            {"name": "추정매출금액_필수값", "type": "not_null"},
            {"name": "추정매출금액_범위(0이상)", "type": "numeric_range", "params": {"min": 0}}
        ],
        "aso_sls_ct": [
            {"name": "추정매출건수_필수값", "type": "not_null"},
            {"name": "추정매출건수_범위(0이상)", "type": "numeric_range", "params": {"min": 0}}
        ]
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
            "name": "월별_총매출금액_전년동월대비_추이분석",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "current_period_value": current_validation_month, # YYYYMM 형식
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMM", # DB 컬럼 형식: YYYYMM
                # 비교 기준: 12개월 전 (전년 동월)
                "comparison_periods": {"type": "previous_n_months", "n": 12},
                "threshold_ratio_decrease": 0.2, # 전년 동월 대비 20% 이상 감소 시 경고
                "threshold_ratio_increase": 0.5  # 전년 동월 대비 50% 이상 증가 시 경고
            }
        },
        {
            "name": "성별_매출금액_전월대비_추이분석",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "group_by_columns": ["sex_ccd"], # 성별로 그룹지어 분석
                "current_period_value": current_validation_month,
                "historical_data_table": table_name,
                "date_column_for_period": partition_key_column,
                "date_column_format": "YYYYMM",
                # 비교 기준: 1개월 전 (전월)
                "comparison_periods": {"type": "previous_n_months", "n": 1},
                "threshold_ratio_decrease": 0.15, # 전월 대비 15% 이상 변동 시 경고
                "threshold_ratio_increase": 0.15
            }
        },
        {
            "name": "중복_데이터_검증",
            "type": "duplicate_rows",
            "params": {
                # 월별 데이터는 모든 분석 단위 컬럼이 동일하면 중복으로 간주
                "subset_columns": [
                    "ta_ym", "wid_cty_cd", "hpsn_bzn_cd", "mct_adm_gds_apb_cd",
                    "kto_mct_ccd_vl", "mct_ue_cln_tcd_vl", "hm_wid_cty_cd",
                    "hm_gds_dsr_cd", "sex_ccd", "age_ccd", "lif_stg_cd", "tmt_vl"
                ]
            }
        }
    ]
}


def add_seasonal_rules(rules_config, validation_month_str, table, partition_col):
    """
    검증 년월을 기반으로 시즌성 이벤트 규칙을 동적으로 추가합니다.
    """
    month = validation_month_str[4:] # "202505" -> "05"

    # --- 1. 가정의 달 (5월) 이벤트 규칙 ---
    if month == '05':
        family_month_rule = {
            "name": "시즌감지(가정의달)_가족소비업종_매출증가",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                # 실제 운영중인 업종 코드로 변경해야 합니다.
                "current_data_filter": "kto_mct_ccd_vl in ['701100', '517400']", # 예시: 테마파크, 장난감 업종 코드
                "current_period_value": validation_month_str,
                "historical_data_table": table,
                "date_column_for_period": partition_col,
                "date_column_format": "YYYYMM",
                "comparison_periods": {"type": "previous_n_months", "n": 1}, # 전월(4월) 대비
                "threshold_ratio_increase": 0.20, # 4월 대비 20% 이상 증가했는지 확인
                "threshold_ratio_decrease": None, # 감소는 체크하지 않음
                "message": "시즌감지(5월): 가족소비업종 매출({current_value:,.0f})이 전월({historical_value:,.0f}) 대비 충분히 증가하지 않았거나 감소(변화율: {change_ratio:.2%})했습니다."
            }
        }
        print(f"INFO: 5월(가정의 달) 감지. 관련 시즌성 검증 규칙을 추가합니다.")
        rules_config["table_level_rules"].append(family_month_rule)

    # --- 2. 여름 휴가철 (7~8월) 이벤트 규칙 ---
    if month in ['07', '08']:
        vacation_rule = {
            "name": "시즌감지(여름휴가철)_주요휴양지_매출증가",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                 # 실제 운영중인 광역도시 코드로 변경해야 합니다.
                "current_data_filter": "wid_cty_cd in ['32', '39']", # 예시: 강원도, 제주도
                "current_period_value": validation_month_str,
                "historical_data_table": table,
                "date_column_for_period": partition_col,
                "date_column_format": "YYYYMM",
                "comparison_periods": {"type": "previous_n_months", "n": 2}, # 비수기인 2달 전(5월/6월)과 비교
                "threshold_ratio_increase": 0.30, # 비수기 대비 30% 이상 증가했는지 확인
                "threshold_ratio_decrease": None,
                "message": "시즌감지(7,8월): 주요휴양지 매출({current_value:,.0f})이 비수기({historical_value:,.0f}) 대비 충분히 증가하지 않았거나 감소(변화율: {change_ratio:.2%})했습니다."
            }
        }
        print(f"INFO: {month}월(여름 휴가철) 감지. 관련 시즌성 검증 규칙을 추가합니다.")
        rules_config["table_level_rules"].append(vacation_rule)

add_seasonal_rules(rules_config, current_validation_month, table_name, partition_key_column)
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
    hive_partition_column_name="dt", # Hive 결과 테이블의 파티션 컬럼
    hive_save_mode_is_overwrite=False,
    max_errors_to_log=100
)

# --- 4. 결과 출력 ---
if detailed_errors:
    print(f"\n>>> 검증 완료: 총 {len(detailed_errors)}개의 오류가 발견되었습니다.")
else:
    print("\n>>> 검증 완료: 모든 검증을 통과했습니다.")