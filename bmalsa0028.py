import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from datalabQuery import QueryProcessor


# 이전에 제공된 검증 라이브러리 코드가 'dqmlib.py'로 저장되어 있다고 가정하고,
# 필요한 함수들을 임포트합니다. 실제 파일명에 맞게 수정해주세요.
# from dqmlib import DatalabQueryProcessor, run_data_validation, _get_offset_date_str
# 여기서는 이전 답변의 최종 코드가 현재 컨텍스트에 있다고 가정합니다.

# --- 0. 설정값 (사용자 환경에 맞게 수정) ---
class MockQueryProcessor:  # 테스트용 Mock QueryProcessor (이전과 동일)
    def __init__(self, *args, **kwargs):
        print("INFO: Mock DatalabQueryProcessor가 초기화되었습니다.")

    def fetch_to_pandas(self, query, engine=None, limit=None):
        print(f"Mock fetch_to_pandas: query='{query}', engine='{engine}', limit={limit}")
        current_month_for_mock = "202410"  # current_validation_month와 동기화 필요 시 로직 추가
        # ... (이전 Mock 데이터 생성 로직과 유사하게, 월별 데이터에 맞게 조정) ...
        # 아래는 단순화된 예시입니다. 실제 테스트 시에는 검증 대상 월과 과거 월 데이터를 구분하여 반환해야 합니다.
        data = {
            'bgda_plf_pti_id': [current_month_for_mock] * 7,
            'ta_ym': [current_month_for_mock] * 7,
            'wid_cty_cd': ['11', '11', '26', '41', '28', '30', '31'],
            'hpsn_bzn_cd': ['11650', '11110', '26350', '41110', '28110', '30110', '31110'],
            'kto_mct_ccd_vl': [f'C01300{i}' for i in range(1, 8)],
            'mct_ue_cln_tcd_vl': ['외지인', '내지인', '외지인', '내지인', '외지인', '내지인', '외지인'],
            'bgda_plf_ls_ld_dt':
                [datetime(int(current_month_for_mock[:4]), int(current_month_for_mock[4:]), 15, 12, 57, 43) - timedelta(
                    days=i) for i in range(7)],
            'aso_saa': [7169100, 4000830, 5500000, 8200000, 6100000, 7300000, 4500000],
            'aso_sls_ct': [643, 314, 450, 720, 580, 690, 350]
        }
        # 과거 데이터 모킹
        if "where" in query.lower() and partition_key_column_monthly in query:
            match = re.search(r"bgda_plf_pti_id\s*BETWEEN\s*'(\d{6})'\s*AND\s*'(\d{6})'", query, re.IGNORECASE)
            if match:  # consecutive_trend_check의 과거 데이터 조회로 간주
                start_m, end_m = match.group(1), match.group(2)
                # 간단한 Mock: 요청된 기간 내의 여러 월 데이터 생성
                mock_hist_data = []
                current_m_dt = datetime.strptime(start_m, "%Y%m")
                end_m_dt = datetime.strptime(end_m, "%Y%m")
                group_counter = 0
                while current_m_dt <= end_m_dt:
                    month_str = current_m_dt.strftime("%Y%m")
                    for _ in range(2):  # 각 월별로 몇 개의 그룹 데이터 생성
                        mock_hist_data.append({
                            'bgda_plf_pti_id': month_str,  # 실제로는 date_column_for_trend 컬럼명 사용
                            'ta_ym': month_str,  # 실제로는 date_column_for_trend 컬럼명 사용
                            'wid_cty_cd': ['11', '26'][group_counter % 2],  # 그룹 예시
                            'hpsn_bzn_cd': ['11650', '26350'][group_counter % 2],
                            'kto_mct_ccd_vl': f'C000{group_counter % 100:03d}',
                            'mct_ue_cln_tcd_vl': '내지인',
                            'bgda_plf_ls_ld_dt': datetime(current_m_dt.year, current_m_dt.month, 1),
                            'aso_saa': np.random.randint(100000, 200000),  # agg_value로 사용될 값
                            'aso_sls_ct': np.random.randint(10, 50)  # agg_value로 사용될 값
                        })
                        group_counter += 1
                    current_m_dt += relativedelta(months=1)
                if mock_hist_data:
                    df_hist = pd.DataFrame(mock_hist_data)
                    # 쿼리의 SELECT 절에서 실제 집계 대상 컬럼과 그룹핑 컬럼을 파싱해야 하지만, Mock에서는 단순화
                    # 예시로 'aso_saa' 또는 'aso_sls_ct'를 'agg_value'로, 'ta_ym'을 날짜컬럼으로 가정
                    # 실제로는 쿼리 파싱하여 SELECT된 컬럼명으로 agg_value를 반환해야 함.
                    # 여기서는 요청된 컬럼이름으로 'agg_value'를 생성하도록 함.
                    agg_col_in_query = "aso_saa"  # 기본값
                    if "aso_sls_ct" in query:
                        agg_col_in_query = "aso_sls_ct"
                    elif "COUNT" in query.upper():
                        df_hist['agg_value'] = 1;
                        agg_col_in_query = "agg_value"

                    df_hist.rename(
                        columns={agg_col_in_query: 'agg_value', 'ta_ym': partition_key_column_monthly_for_trend_rules},
                        inplace=True)  # date_column_for_trend 사용
                    # 필요한 컬럼만 선택 (그룹핑 컬럼 + 날짜 컬럼 + agg_value)
                    gb_cols_in_query = [gc.strip() for gc_list in
                                        re.findall(r"SELECT\s*(.*?),\s*SUM|AVG|COUNT", query, re.IGNORECASE) for gc in
                                        gc_list.split(',') if partition_key_column_monthly_for_trend_rules not in gc]

                    cols_to_select = [partition_key_column_monthly_for_trend_rules, 'agg_value'] + gb_cols_in_query
                    cols_present_in_df = [col for col in cols_to_select if col in df_hist.columns]

                    return df_hist[cols_present_in_df]

        df = pd.DataFrame(data)
        for col_dt in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col_dt] = df[col_dt].dt.to_pydatetime()
        if "where" in query.lower() and partition_key_column_monthly in query and current_validation_month in query:
            df = df[df[partition_key_column_monthly] == current_validation_month].reset_index(drop=True)
        return df

    def save_pandas_to_datalake(self, df, db_name, table_name, partition_column, overwrite_tf=False):
        print(
            f"Mock save_pandas_to_datalake: DataFrame to {db_name}.{table_name} (partition: {partition_column}, overwrite: {overwrite_tf})")
        return True

    def describe_table(self, table_name, engine=None):
        print(f"Mock describe_table for {table_name} engine {engine}")
        if table_name == new_monthly_table_name and engine == "hive":
            return pd.DataFrame([
                {'col_name': 'bgda_plf_pti_id', 'data_type': 'string', 'comment': '빅데이터플랫폼파티션ID_TA_YM'},
                {'col_name': 'ta_ym', 'data_type': 'string', 'comment': '기준년월'},
                {'col_name': 'wid_cty_cd', 'data_type': 'string', 'comment': '광역도시코드'},
                {'col_name': 'hpsn_bzn_cd', 'data_type': 'string', 'comment': '초개인화상권코드'},
                {'col_name': 'kto_mct_ccd_vl', 'data_type': 'string', 'comment': '한국관광공사가맹점구분코드값'},
                {'col_name': 'mct_ue_cln_tcd_vl', 'data_type': 'string', 'comment': '가맹점이용고객유형코드값'},
                {'col_name': 'bgda_plf_ls_ld_dt', 'data_type': 'timestamp', 'comment': '빅데이터플랫폼적재일시'},
                {'col_name': 'aso_saa', 'data_type': 'decimal(15,0)', 'comment': '추정매출금액'},
                {'col_name': 'aso_sls_ct', 'data_type': 'decimal(10,0)', 'comment': '추정매출건수'},
            ])
        return pd.DataFrame()


real_q_processor = QueryProcessor(clear_auth_tf=False)

current_validation_month = (datetime.now().replace(day=1) - relativedelta(months=2)).strftime("%Y%m")
new_monthly_table_name = 'mdb.bmalsa0028'
partition_key_column_monthly = "bgda_plf_pti_id"
date_ref_column_monthly = "ta_ym"
# consecutive_trend_check 와 aggregate_value_trend 규칙에서 사용할 날짜 컬럼명 통일
# 이 컬럼은 DB와 DF 모두에 존재하며 YYYYMM 또는 YYYYMMDD 형식의 값을 가짐
# 월별 테이블의 경우, bgda_plf_pti_id 또는 ta_ym 이 될 수 있음. 여기서는 ta_ym을 기준으로 함.
partition_key_column_monthly_for_trend_rules = "ta_ym"

print(f"검증 대상 테이블: {new_monthly_table_name}, 검증 기준년월(파티션 값): {current_validation_month}")

current_data_df_monthly = real_q_processor.fetch_to_pandas(
    query=f"SELECT * from {new_monthly_table_name} where {partition_key_column_monthly} = '{current_validation_month}'",
    engine="hive",
    limit=None
)
if current_data_df_monthly.empty and not (
        datetime.strptime(current_validation_month, "%Y%m") > datetime.now() - relativedelta(months=1)):
    print(f"경고: {current_validation_month}에 대한 데이터가 없습니다. 빈 DataFrame으로 검증을 진행합니다.")
current_data_df_monthly.attrs['name'] = new_monthly_table_name

prev_month_ym_str = _get_offset_date_str(current_validation_month, months_offset=-1, current_format_str="%Y%m",
                                         output_format_str="%Y%m")
prev_year_same_month_ym_str = _get_offset_date_str(current_validation_month, months_offset=-12,
                                                   current_format_str="%Y%m", output_format_str="%Y%m")

rules_config_monthly = {
    "columns": {
        "bgda_plf_pti_id": [
            {"name": "월별_파티션ID_필수값", "type": "not_null"},
            {"name": "월별_파티션ID_형식검증_YYYYMM", "type": "regex_pattern", "params": {"pattern": r"^\d{6}$"},
             "message": f"파티션ID({partition_key_column_monthly})는 YYYYMM 형식이어야 합니다."},
            {"name": "월별_파티션ID_검증월_일치", "type": "allowed_values", "params": {"values": [current_validation_month]},
             "message": f"파티션ID({partition_key_column_monthly})는 검증 기준월인 {current_validation_month}와 일치해야 합니다."}
        ],
        "ta_ym": [
            {"name": "기준년월_필수값", "type": "not_null"},
            {"name": "기준년월_형식검증_YYYYMM", "type": "regex_pattern", "params": {"pattern": r"^\d{6}$"},
             "message": f"기준년월({date_ref_column_monthly})은 YYYYMM 형식이어야 합니다."},
            {"name": "기준년월_검증월_일치", "type": "allowed_values", "params": {"values": [current_validation_month]},
             "message": f"기준년월({date_ref_column_monthly})은 검증 기준월인 {current_validation_month}와 일치해야 합니다."}
        ],
        "wid_cty_cd": [
            {"name": "월별_광역도시코드_필수값", "type": "not_null"},
            {"name": "월별_광역도시코드_형식검증_숫자혹은*2자리", "type": "regex_pattern", "params": {"pattern": r"^[0-9*]{2}$"}}
        ],
        "hpsn_bzn_cd": [
            {"name": "월별_상권코드_필수값", "type": "not_null"},
            {"name": "월별_상권코드_형식검증_숫자혹은*5자리", "type": "regex_pattern", "params": {"pattern": r"^[0-9*]{5}$"}}
        ],
        "kto_mct_ccd_vl": [
            {"name": "월별_관광공사코드_필수값", "type": "not_null"},
            {"name": "월별_관광공사코드_형식검증_C시작7자리", "type": "regex_pattern", "params": {"pattern": r"^[A-Z]\d{6}$"}},
            {"name": "월별_관광공사코드_분포변경_vs_전월",
             "type": "distribution_change",
             "params": {
                 "historical_data_table": new_monthly_table_name,
                 "historical_data_column": "kto_mct_ccd_vl",
                 "historical_data_filter": f"{partition_key_column_monthly} = '{{hist_period}}'",  # format()을 위해 중괄호 유지
                 "historical_data_query_params": {"hist_period": prev_month_ym_str},
                 "thresholds": {"new_code_max_ratio": 0.05, "freq_change_tolerance_abs": 0.1,
                                "unique_count_tolerance_ratio": 0.1}
             }}
        ],
        "mct_ue_cln_tcd_vl": [
            {"name": "월별_고객유형_필수값", "type": "not_null"},
            {"name": "월별_고객유형_허용값", "type": "allowed_values", "params": {"values": ["내지인", "외지인"]}}
        ],
        "bgda_plf_ls_ld_dt": [
            {"name": "월별_플랫폼적재일시_필수값", "type": "not_null"}
        ],
        "aso_saa": [
            {"name": "월별_추정매출금액_필수값", "type": "not_null"},
            {"name": "월별_추정매출금액_범위검증", "type": "numeric_range", "params": {"min": 0}}
        ],
        "aso_sls_ct": [
            {"name": "월별_추정매출건수_필수값", "type": "not_null"},
            {"name": "월별_추정매출건수_범위검증", "type": "numeric_range", "params": {"min": 0}}
        ]
    },
    "table_level_rules": [
        {
            "name": "월별_스키마_변경_감지",
            "type": "schema_change_check",
            "params": {
                "table_name_in_db": new_monthly_table_name,
                "engine": "hive",
                "auto_manage_baseline": True,
                "check_options": {"detect_new_columns": True, "detect_missing_columns": True,
                                  "detect_type_changes": True, "detect_nullable_changes": True},
            }
        },
        {
            "name": "월별_전체행수_추이_vs_전월",
            "type": "total_row_count_trend",
            "params": {
                "current_period_value": current_validation_month,
                "historical_data_table": new_monthly_table_name,
                "date_column_for_period": partition_key_column_monthly,
                "date_column_format": "YYYYMM",
                "comparison_periods": {"type": "previous_n_months", "n": 1},
                "threshold_ratio_decrease": 0.10,
                "threshold_ratio_increase": 0.10
            }
        },
        {
            "name": "월별_전체매출액_추이_vs_전년동월",
            "type": "aggregate_value_trend",
            "params": {
                "column_to_aggregate": "aso_saa",
                "aggregate_function": "SUM",
                "current_period_value": current_validation_month,
                "historical_data_table": new_monthly_table_name,
                "date_column_for_period": partition_key_column_monthly,
                "date_column_format": "YYYYMM",
                "comparison_periods": {"type": "previous_n_months", "n": 12},
                "threshold_ratio_decrease": 0.20,
                "threshold_ratio_increase": 0.20
            }
        },
        {  # 월별 연속 추세 검증 추가
            "name": "도시별_매출건수_3개월_연속감소_감지",
            "type": "consecutive_trend_check",
            "params": {
                "column_to_aggregate": "aso_sls_ct",
                "aggregate_function": "SUM",
                "group_by_columns": ["wid_cty_cd"],
                "date_column_for_trend": partition_key_column_monthly_for_trend_rules,  # DB 및 DF의 YYYYMM 형식 컬럼
                "date_column_format": "YYYYMM",  # 명시적으로 YYYYMM 사용
                "trend_type": "down",
                "consecutive_periods": 3,  # 3개월 연속
                "period_unit": "months",  # 월 단위 추세
                "historical_data_table": new_monthly_table_name,
                "historical_lookback_periods": 5,  # 최소 consecutive_periods + 1 (3+1=4, 넉넉히 5)
                "engine": "hive"
            }
        },
        {
            "name": "월별_주요키_중복행_검증",
            "type": "duplicate_rows",
            "params": {
                "subset_columns": [date_ref_column_monthly, "hpsn_bzn_cd", "kto_mct_ccd_vl", "mct_ue_cln_tcd_vl"]}
        }
    ]
}

# --- 검증 실행 ---
print(f"\n>>> '{new_monthly_table_name}' 테이블 대상 검증 시작 (기준월: {current_validation_month}) <<<\n")

current_data_df_monthly.info()
print(current_data_df_monthly.head(10))
# 실제 라이브러리 함수 호출 (dqmlib.py 또는 검증_최종.py 에 정의된 함수 사용)
detailed_errors, summary_report = run_data_validation(
    dataframe=current_data_df_monthly,
    rules_config=rules_config_monthly,
    query_processor_instance=real_q_processor,
    save_to_hive=True,
    hive_db_name="tdb",
    hive_validation_runs_table_name="dqm_validation_runs",
    hive_summary_table_name="dqm_rule_execution_summary",
    hive_errors_table_name="dqm_detailed_errors",
    hive_partition_value=current_validation_month,
    hive_partition_column_name="bgda_plf_pti_id",  # 월별 파티션 컬럼명 예시
    hive_save_mode_is_overwrite=False,
    max_errors_to_log=50,
    frst_rgr_id='MONTHLY_VALIDATOR_V2',
    last_updtr_id='MONTHLY_VALIDATOR_V2'
)

if detailed_errors:
    print(f"\n>>> 검증 완료: 오류가 {len(detailed_errors)}건 발견되었습니다.")
else:
    print("\n>>> 검증 완료: 모든 검증을 통과했습니다.")
if detailed_errors:
    print(f"\n>>> 검증 완료: 오류가 {len(detailed_errors)}건 발견되었습니다.")
else:
    print("\n>>> 검증 완료: 모든 검증을 통과했습니다.")