import pandas as pd
import re
import json
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
from tqdm import tqdm
import os
import time
import logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter("[%(levelname)s] %(message)s")
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# --- 스키마 기준 파일 저장 디렉토리 (사용자 환경에 맞게 설정 가능) ---
SCHEMA_BASELINE_DIR = "./schema_baselines/"

try:
    from datalabQuery import QueryProcessor as DatalabQueryProcessor
except ImportError:
    class DatalabQueryProcessor:
        def __init__(self, *args, **kwargs):
            logger.info("INFO: Mock DatalabQueryProcessor가 초기화되었습니다.")

        def fetch_to_pandas(self, query, engine=None, limit=None):
            query_preview = query[:150].replace('\n', ' ') + "..."
            logger.info(f"INFO: Mock DatalabQueryProcessor - fetch_to_pandas 호출됨 (Query: {query_preview})")
            if "COUNT(1) AS agg_value" in query or "COUNT(*) AS agg_value" in query:
                if "GROUP BY" in query and re.search(r"SELECT .*?, COUNT", query, re.IGNORECASE):
                    mock_dates = [(datetime.now() - timedelta(days=i)).strftime("%Y%m%d") for i in range(5)]
                    mock_groups = ['A', 'B'];
                    mock_data = []
                    for dt in mock_dates:
                        for grp in mock_groups: mock_data.append \
                            ({'date_col': dt, 'grp_col1': grp, 'agg_value': np.random.randint(5, 20)})
                    return pd.DataFrame(mock_data) if mock_data else pd.DataFrame()
                return pd.DataFrame({'agg_value': [np.random.randint(50, 150)]})
            if "AVG(" in query and "STDDEV_SAMP(" in query:
                if "GROUP BY" not in query:
                    return pd.DataFrame({'mean_val': [np.random.rand() * 1000], 'std_val': [np.random.rand() * 100],
                                         'min_val': [np.random.rand() * 10], 'max_val': [np.random.rand() * 2000]
                                            , 'median_val': [np.random.rand() * 900], 'q1_val': [np.random.rand() * 400]
                                            , 'q3_val': [np.random.rand() * 1500],
                                         'count_val': [np.random.randint(500, 1000)]})
                else:
                    group_cols = [gc.strip() for gc_list in re.findall(r"SELECT (.*?) AVG\(", query, re.IGNORECASE) for
                                  gc in gc_list.split(',') if gc.strip() and "AVG" not in gc.upper()] or ["grp_col1"]
                    mock_list = []
                    for i in range(np.random.randint(1, 3)):
                        row = {'mean_val': np.random.rand() * 1000, 'std_val': np.random.rand() * 100
                            , 'min_val': np.random.rand() * 10, 'max_val': np.random.rand() * 2000
                            , 'median_val': np.random.rand() * 900, 'q1_val': np.random.rand() * 400
                            , 'q3_val': np.random.rand() * 1500, 'count_val': np.random.randint(10, 100)}
                        for idx, gc_name in enumerate(group_cols): row[gc_name] = f"GroupVal{i + 1}_{idx + 1}"
                        mock_list.append(row)
                    return pd.DataFrame(mock_list) if mock_list else pd.DataFrame(
                        {**{gc: [] for gc in group_cols}, **{sc: [] for sc in ['mean_val', 'std_val', 'min_val'
                            , 'max_val', 'median_val'
                            , 'q1_val', 'q3_val'
                            , 'count_val']}})
            if "CAST(" in query and "AS STRING) AS code" in query: return pd.DataFrame \
                ({'code': [f'CODE{i}' for i in range(np.random.randint(3, 7))],
                  'frequency': np.random.randint(10, 100, size=np.random.randint(3, 7))})
            if "COUNT(" in query and "AS total_count" in query and "DISTINCT" in query: return pd.DataFrame \
                ({'total_count': [np.random.randint(200, 500)], 'total_unique_count': [np.random.randint(3, 7)]})
            if "GROUP BY" in query and ("SUM(" in query or "AVG(" in query):
                group_cols_agg = [gc.strip() for gc_list in re.findall(r"SELECT (.*?),.*\(", query, re.IGNORECASE) for
                                  gc in gc_list.split(',') if
                                  gc.strip() and not any(f in gc.upper() for f in ["SUM", "AVG", "COUNT"])] or \
                                 ["grp_col1"]
                mock_list_agg = []
                for i in range(np.random.randint(1, 4)):
                    row_agg = {'agg_value': np.random.randint(500, 2000)}
                    for idx, gc_name in enumerate(group_cols_agg):
                        row_agg[gc_name] = f"GroupVal{i + 1}_{idx + 1}"
                    mock_list_agg.append(row_agg)
                return pd.DataFrame(mock_list_agg) if mock_list_agg else pd.DataFrame(
                    {**{gc: [] for gc in group_cols_agg}, 'agg_value': []})
            return pd.DataFrame()

        def describe_table(self, table_name, engine=None):
            logger.info(f"INFO: Mock DatalabQueryProcessor - describe_table 호출됨 (Table: {table_name}, Engine: {engine})")
            if table_name in ["mdb.bmalsa0026", "MOCK_TABLE_FOR_SCHEMA_TEST", "edb.hmcbsi0015",
                              "mdb.monthly_tourism_sales"]:  # 테스트용 테이블명 추가
                if engine and engine.lower() == "hive":
                    return pd.DataFrame([{'col_name': 'ced', 'data_type': 'string', 'comment': '기준일자'},
                                         {'col_name': 'wid_cty_cd', 'data_type': 'string', 'comment': '광역도시코드'},
                                         {'col_name': 'hpsn_bzn_cd', 'data_type': 'string', 'comment': '초개인화상권코드'},
                                         {'col_name': 'mct_ue_cln_tcd_vl', 'data_type': 'varchar(20)',
                                          'comment': '가맹점이용고객유형코드값'},
                                         {'col_name': 'hm_wid_cty_cd', 'data_type': 'string', 'comment': '자택광역도시코드'},
                                         {'col_name': 'newly_added_column_in_db', 'data_type': 'int',
                                          'comment': 'DB에만 새로 추가된 컬럼'},
                                         {'col_name': 'bgda_plf_ls_ld_dt', 'data_type': 'timestamp',
                                          'comment': '빅데이터플랫폼적재일시'},
                                         {'col_name': 'aso_saa', 'data_type': 'decimal(15,0)', 'comment': '추정매출금액'},
                                         {'col_name': 'aso_sls_ct', 'data_type': 'decimal(10,0)', 'comment': '추정매출건수'},
                                         {'col_name': 'bgda_plf_pti_id', 'data_type': 'string',
                                          'comment': '빅데이터플랫폼파티션ID_CED'},
                                         {'col_name': 'ta_ym', 'data_type': 'string', 'comment': '기준년월'},
                                         {'col_name': '# Partition Information', 'data_type': None, 'comment': None},
                                         {'col_name': '# col_name            ', 'data_type': 'data_type          ',
                                          'comment': 'comment            '},
                                         {'col_name': 'bgda_plf_pti_id', 'data_type': 'string',
                                          'comment': '빅데이터플랫폼파티션ID_CED'}])
                elif engine and engine.lower() == "edw":
                    return pd.DataFrame([{'COLUMN_ID': 1, 'OWNER': 'EDWADM', 'TABLE_NAME': 'MCBSI0015',
                                          'COLUMN_NAME': 'CED', 'COMMENTS': '기준일자', 'DATA_TYPE': 'VARCHAR2(8)',
                                          'NULLABLE': 'N'},
                                         {'COLUMN_ID': 2, 'OWNER': 'EDWADM', 'TABLE_NAME': 'MCBSI0015',
                                          'COLUMN_NAME': 'WID_CTY_CD', 'COMMENTS': '광역도시코드', 'DATA_TYPE': 'VARCHAR2(2)',
                                          'NULLABLE': 'N'},
                                         {'COLUMN_ID': 3, 'OWNER': 'EDWADM', 'TABLE_NAME': 'MCBSI0015',
                                          'COLUMN_NAME': 'HPSN_BZN_CD', 'COMMENTS': '초개인화상권코드',
                                          'DATA_TYPE': 'VARCHAR2(10)', 'NULLABLE': 'Y'},
                                         {'COLUMN_ID': 5, 'OWNER': 'EDWADM', 'TABLE_NAME': 'MCBSI0015',
                                          'COLUMN_NAME': 'MCT_UE_CLN_TCD_VL', 'COMMENTS': '가맹점이용고객유형코드값',
                                          'DATA_TYPE': 'VARCHAR2(5)', 'NULLABLE': 'Y'},
                                         {'COLUMN_ID': 42, 'OWNER': 'EDWADM', 'TABLE_NAME': 'MCBSI0015',
                                          'COLUMN_NAME': 'NEW_DB_ONLY_COL_EDW', 'COMMENTS': 'DB에만있는EDW컬럼',
                                          'DATA_TYPE': 'DATE', 'NULLABLE': 'Y'}])
            return pd.DataFrame()

        def save_pandas_to_datalake(self, df, db_name, table_name, partition_column, overwrite_tf=False):
            mode = "overwrite" if overwrite_tf else "append";
            logger.info(
                f"INFO: Mock DatalabQueryProcessor - save_pandas_to_datalake 호출됨.\n      (df ({len(df)} rows), db_name='{db_name}', table_name='{table_name}', partition_column='{partition_column}', mode='{mode}')")
            logger.info(
                f"INFO: Mock - DataFrame을 Hive 테이블 '{db_name}.{table_name}' (파티션: {partition_column})에 '{mode}' 모드로 저장 시뮬레이션 완료.");
            return True


def _get_offset_date_str(base_date_str, days_offset=0, months_offset=0, current_format_str="%Y%m%d",
                         output_format_str="%Y%m%d"):
    try:
        base_date = datetime.strptime(base_date_str, current_format_str);
        target_date = base_date + relativedelta(
            months=months_offset) + timedelta(days=days_offset);
        return target_date.strftime(output_format_str)
    except ValueError as e:
        if current_format_str == "%Y%m" and len(base_date_str) == 6:
            try:
                base_date = datetime.strptime(base_date_str + "01", "%Y%m%d");
                target_date = base_date + relativedelta(
                    months=months_offset) + timedelta(days=days_offset);
                return target_date.strftime(output_format_str)
            except ValueError as e_inner:
                raise ValueError(
                    f"날짜 변환 오류 (월 형식 처리 중): base_date_str='{base_date_str}', format='{current_format_str}'. 상세: {e_inner}")
        raise ValueError(f"날짜 변환 오류: base_date_str='{base_date_str}', format='{current_format_str}'. 상세: {e}")


def get_month_start_end_dates(yyyymm_str):
    if not (isinstance(yyyymm_str, str) and len(yyyymm_str) == 6 and yyyymm_str.isdigit()): raise ValueError(
        f"get_month_start_end_dates: 입력값은 'YYYYMM' 형식의 문자열이어야 합니다: {yyyymm_str}")
    year, month = int(yyyymm_str[:4]), int(yyyymm_str[4:]);
    start_date = f"{year:04d}{month:02d}01";
    _, last_day = calendar.monthrange(year, month);
    end_date = f"{year:04d}{month:02d}{last_day:02d}";
    return start_date, end_date


def _get_historical_aggregate_value(q_processor, table, agg_column, agg_func, date_col_in_db, date_col_format_in_db,
                                    is_partitioned_by_date_col, target_historical_period, engine, base_filter="1=1"):
    if not q_processor: logger.info("경고: QueryProcessor가 없어 과거 집계값을 조회할 수 없습니다."); return 0.0
    where_clause_for_date = "1=1"
    try:
        if date_col_format_in_db == "YYYYMM":
            if not (len(target_historical_period) == 6 and target_historical_period.isdigit()): logger.info(
                f"경고: date_col_format_in_db 'YYYYMM', target_historical_period ('{target_historical_period}') 형식 오류."); return 0.0
            where_clause_for_date = f"{date_col_in_db} = '{target_historical_period}'"
        elif date_col_format_in_db == "YYYYMMDD":
            if len(target_historical_period) == 8 and target_historical_period.isdigit():
                where_clause_for_date = f"{date_col_in_db} = '{target_historical_period}'"
            elif len(target_historical_period) == 6 and target_historical_period.isdigit():
                start_day, end_day = get_month_start_end_dates(
                    target_historical_period);
                where_clause_for_date = f"{date_col_in_db} BETWEEN '{start_day}' AND '{end_day}'" if is_partitioned_by_date_col else f"SUBSTRING(CAST({date_col_in_db} AS STRING), 1, 6) = '{target_historical_period}'"
            else:
                logger.info(
                    f"경고: date_col_format_in_db 'YYYYMMDD', target_historical_period ('{target_historical_period}') 형식 오류.");
                return 0.0
        else:
            logger.info(f"경고: 지원하지 않는 date_column_format '{date_col_format_in_db}'.");
            return 0.0
    except ValueError as ve:
        logger.info(f"경고: 날짜 변환 또는 처리 오류로 과거 집계값 조회 불가 - {ve}");
        return 0.0
    actual_agg_column = '*' if agg_func.upper() == 'COUNT' and agg_column in ['*', '1'] else agg_column
    query = f"SELECT {agg_func}({actual_agg_column}) AS agg_value FROM {table} WHERE {where_clause_for_date} AND {base_filter}"
    try:
        result_df = q_processor.fetch_to_pandas(query=query, engine=engine, limit=1)
        return float(result_df.iloc[0]['agg_value']) if not result_df.empty and pd.notna(
            result_df.iloc[0]['agg_value']) else 0.0
    except Exception as e:
        logger.info(f"경고: 과거 집계값 조회 중 오류 ({table}, {agg_column}, {target_historical_period}): {e}");
        return 0.0


def _get_historical_grouped_aggregates(q_processor, table, agg_column, agg_func, group_by_columns, date_col_in_db,
                                       date_col_format_in_db, is_partitioned_by_date_col, target_historical_period,
                                       engine, base_filter="1=1"):
    if not q_processor: logger.info("경고: QueryProcessor가 없어 과거 그룹별 집계값을 조회할 수 없습니다."); return {}
    where_clause_for_date = "1=1"
    try:
        if date_col_format_in_db == "YYYYMM":
            if not (len(target_historical_period) == 6 and target_historical_period.isdigit()): logger.info(
                f"경고: 그룹별 과거 집계 - date_col_format_in_db 'YYYYMM', target_historical_period ('{target_historical_period}') 형식 오류."); return {}
            where_clause_for_date = f"{date_col_in_db} = '{target_historical_period}'"
        elif date_col_format_in_db == "YYYYMMDD":
            if len(target_historical_period) == 8 and target_historical_period.isdigit():
                where_clause_for_date = f"{date_col_in_db} = '{target_historical_period}'"
            elif len(target_historical_period) == 6 and target_historical_period.isdigit():
                start_day, end_day = get_month_start_end_dates(
                    target_historical_period);
                where_clause_for_date = f"{date_col_in_db} BETWEEN '{start_day}' AND '{end_day}'" if is_partitioned_by_date_col else f"SUBSTRING(CAST({date_col_in_db} AS STRING), 1, 6) = '{target_historical_period}'"
            else:
                logger.info(
                    f"경고: 그룹별 과거 집계 - date_col_format_in_db 'YYYYMMDD', target_historical_period ('{target_historical_period}') 형식 오류.");
                return {}
        else:
            logger.info(f"경고: 그룹별 과거 집계 - 지원하지 않는 date_column_format '{date_col_format_in_db}'.");
            return {}
    except ValueError as ve:
        logger.info(f"경고: 그룹별 과거 집계 - 날짜 변환 오류: {ve}");
        return {}
    gb_cols_str = ", ".join(group_by_columns);
    actual_agg_column = '*' if agg_func.upper() == 'COUNT' and agg_column in ['*', '1'] else agg_column
    agg_col_filter = f"AND {actual_agg_column} IS NOT NULL" if agg_func.upper() != 'COUNT' or (
            agg_func.upper() == 'COUNT' and actual_agg_column not in ['*', '1']) else ""
    query = f"""SELECT {gb_cols_str}, {agg_func}({actual_agg_column}) AS agg_value FROM {table} WHERE {where_clause_for_date} AND {base_filter} {agg_col_filter} GROUP BY {gb_cols_str}"""
    try:
        result_df = q_processor.fetch_to_pandas(query=query, engine=engine, limit=None);
        output_map = {}
        if not result_df.empty:
            for _, row in result_df.iterrows(): output_map[tuple(
                str(row[col]) if pd.notna(row[col]) else '__NONE_GROUP_KEY__' for col in group_by_columns)] = float(
                row['agg_value']) if pd.notna(row['agg_value']) else 0.0
        return output_map
    except Exception as e:
        logger.info(
            f"경고: 과거 그룹별 집계값 조회 중 오류 ({table}, {agg_column}, 그룹: {group_by_columns}, 기간: {target_historical_period}): {e}");
        return {}


# --- 1. 개별 검증 로직을 담당하는 함수들 ---
def check_not_null(series, column_name, params=None, disable_tqdm=True):
    errors = [];
    params = params or {};
    msg_template = params.get('message', "컬럼 '{column_name}'은(는) 필수 값입니다.")
    if series is None: return [
        {'column': column_name, 'error_type': 'SERIES_IS_NONE', 'message': f"컬럼 '{column_name}'의 시리즈(데이터)가 None입니다."}]
    for idx in series[series.isnull()].index: errors.append(
        {'column': column_name, 'row_index': idx, 'error_type': 'NOT_NULL',
         'message': msg_template.format(column_name=column_name)})
    return errors


def check_regex_pattern(series, column_name, params, disable_tqdm=True):
    errors = [];
    pattern = params.get('pattern')
    if series is None: return [
        {'column': column_name, 'error_type': 'SERIES_IS_NONE', 'message': f"컬럼 '{column_name}'의 시리즈(데이터)가 None입니다."}]
    if not pattern: errors.append(
        {'column': column_name, 'error_type': 'CONFIG_ERROR', 'message': "정규식 패턴 필요"}); return errors
    msg_template = params.get('message', "컬럼 '{column_name}'의 값 '{value}'이(가) 패턴 '{pattern}'과(와) 불일치.")
    items_to_iterate = series.dropna()
    for idx, val in tqdm(items_to_iterate.items(), total=len(items_to_iterate), desc=f"REGEX '{column_name}'",
                         disable=disable_tqdm, leave=False, unit="행"):
        if not isinstance(val, str) or not re.match(pattern, val): errors.append(
            {'column': column_name, 'row_index': idx, 'value': val, 'error_type': 'REGEX_MISMATCH',
             'message': msg_template.format(column_name=column_name, value=val, pattern=pattern)})
    return errors


def check_allowed_values(series, column_name, params, disable_tqdm=True):
    errors = [];
    allowed = set(params.get('values', []))
    if series is None: return [
        {'column': column_name, 'error_type': 'SERIES_IS_NONE', 'message': f"컬럼 '{column_name}'의 시리즈(데이터)가 None입니다."}]
    if not allowed: errors.append(
        {'column': column_name, 'error_type': 'CONFIG_ERROR', 'message': "허용 값 목록 필요"}); return errors
    msg_template = params.get('message', "컬럼 '{column_name}'의 값 '{value}'은(는) 허용 목록 {allowed_values}에 없음.")
    items_to_iterate = series.dropna()
    for idx, val in tqdm(items_to_iterate.items(), total=len(items_to_iterate), desc=f"ALLOWED '{column_name}'",
                         disable=disable_tqdm, leave=False, unit="행"):
        if val not in allowed: errors.append(
            {'column': column_name, 'row_index': idx, 'value': val, 'error_type': 'INVALID_VALUE',
             'message': msg_template.format(column_name=column_name, value=val, allowed_values=list(allowed))})
    return errors


def check_numeric_range(series, column_name, params, disable_tqdm=True):
    errors = [];
    min_v, max_v = params.get('min'), params.get('max')
    if series is None: return [
        {'column': column_name, 'error_type': 'SERIES_IS_NONE', 'message': f"컬럼 '{column_name}'의 시리즈(데이터)가 None입니다."}]
    min_s, max_s = str(min_v) if min_v is not None else '-inf', str(max_v) if max_v is not None else 'inf'
    msg_template = params.get('message', "컬럼 '{column_name}'의 값 '{value}'이(가) 범위 [{min_val_str}, {max_val_str}] 벗어남.")
    num_s = pd.to_numeric(series, errors='coerce')
    for idx, val_numeric in tqdm(num_s.items(), total=len(num_s), desc=f"RANGE '{column_name}'", disable=disable_tqdm,
                                 leave=False, unit="행"):
        original_value = series.loc[idx]
        if pd.isna(val_numeric):
            if not series.isnull().loc[idx]: errors.append(
                {'column': column_name, 'row_index': idx, 'value': original_value, 'error_type': 'NOT_NUMERIC',
                 'message': f"컬럼 '{column_name}'의 값 '{original_value}'은(는) 숫자로 변환할 수 없습니다."})
            continue
        if (min_v is not None and val_numeric < min_v) or (max_v is not None and val_numeric > max_v):
            errors.append(
                {'column': column_name, 'row_index': idx, 'value': original_value, 'error_type': 'OUT_OF_RANGE',
                 'message': msg_template.format(column_name=column_name, value=original_value, min_val_str=min_s,
                                                max_val_str=max_s)})
    return errors


def check_distribution_change(series, column_name, params, q_processor=None):
    errors = [];
    historical_profile = None;
    current_profile_data = {'unique_codes': [], 'frequencies': {}, 'total_unique_count': 0, 'count': 0, 'null_count': 0}
    if series is None or series.empty:
        logger.info(f"정보: 컬럼 '{column_name}' 현재 데이터 비어 분포 변경 검사 일부 수행/건너뜀.")
    else:
        current_s = series.astype(str);
        vc = current_s.value_counts(dropna=False);
        tc = len(
            current_s);
        null_count_current = series.isnull().sum();
        current_profile_data = {
            'unique_codes': [str(code) for code in vc.index],
            'frequencies': {str(k): (v / tc if tc > 0 else 0) for k, v in vc.items()},
            'total_unique_count': current_s.nunique(dropna=False), 'count': tc, 'null_count': int(null_count_current)}
    if 'historical_data_table' in params and 'historical_data_column' in params:
        if not q_processor: errors.append(
            {'column': column_name, 'error_type': 'CONFIG_ERROR', 'message': "DB용 QueryProcessor 필요"}); return errors
        table, hist_col = params['historical_data_table'], params['historical_data_column'];
        filter_condition = params.get('historical_data_filter', '1=1');
        query_params = params.get('historical_data_query_params', {});
        db_engine = params.get('db_engine', 'hive')
        if query_params: filter_condition = filter_condition.format(**query_params)
        freq_query = f"SELECT CAST({hist_col} AS STRING) AS code, COUNT(*) AS frequency FROM {table} WHERE {filter_condition} AND {hist_col} IS NOT NULL GROUP BY CAST({hist_col} AS STRING)"
        summary_query = f"SELECT COUNT(1) AS total_count_with_null, COUNT({hist_col}) AS total_count_not_null, COUNT(DISTINCT {hist_col}) AS total_unique_count_not_null FROM {table} WHERE {filter_condition}"
        try:
            hist_freq_df = q_processor.fetch_to_pandas(query=freq_query, engine=db_engine, limit=None);
            hist_summary_df = q_processor.fetch_to_pandas(query=summary_query, engine=db_engine, limit=None)
            if hist_summary_df.empty or pd.isna(hist_summary_df.iloc[0]['total_count_with_null']) or \
                    hist_summary_df.iloc[0]['total_count_with_null'] == 0:
                historical_profile = {'unique_codes': [], 'frequencies': {}, 'total_unique_count': 0, 'count': 0,
                                      'null_count': 0}
            else:
                tc_hist_wn = int(hist_summary_df.iloc[0]['total_count_with_null']);
                tc_hist_nn = int(hist_summary_df.iloc[0]['total_count_not_null']);
                nc_hist = tc_hist_wn - tc_hist_nn;
                tuc_hist_nn = int(hist_summary_df.iloc[0]['total_unique_count_not_null']) if pd.notna(
                    hist_summary_df.iloc[0]['total_unique_count_not_null']) else 0
                uc_hist_nn = hist_freq_df['code'].astype(str).tolist() if not hist_freq_df.empty else [];
                freqs_hist = {str(r['code']): r['frequency'] / tc_hist_wn if tc_hist_wn > 0 else 0 for _, r in
                              hist_freq_df.iterrows()} if not hist_freq_df.empty else {}
                if tc_hist_wn > 0: freqs_hist[str(np.nan)] = nc_hist / tc_hist_wn
                historical_profile = {'unique_codes': uc_hist_nn + ([str(np.nan)] if nc_hist > 0 else []),
                                      'frequencies': freqs_hist,
                                      'total_unique_count': tuc_hist_nn + (1 if nc_hist > 0 else 0),
                                      'count': tc_hist_wn, 'null_count': nc_hist}
        except Exception as e:
            errors.append({'column': column_name, 'error_type': 'DB_AGGREGATE_PROFILE_ERROR',
                           'message': f"DB 과거 분포 프로파일 생성 실패 ({column_name}): {e}"});
            return errors
    elif 'historical_profile_path' in params:
        try:
            with open(params['historical_profile_path'], 'r', encoding='utf-8') as f:
                historical_profile = json.load(f)
            historical_profile.setdefault('null_count', 0);
            historical_profile.setdefault('count', 0)
            if 'frequencies' in historical_profile and str(np.nan) not in historical_profile['frequencies'] and \
                    historical_profile['null_count'] > 0 and historical_profile['count'] > 0:
                historical_profile['frequencies'][str(np.nan)] = historical_profile['null_count'] / historical_profile[
                    'count']
        except FileNotFoundError:
            errors.append({'column': column_name, 'error_type': 'PROFILE_FILE_NOT_FOUND',
                           'message': f"프로파일 파일을 찾을 수 없습니다: {params['historical_profile_path']}"});
            historical_profile = None
        except json.JSONDecodeError:
            errors.append({'column': column_name, 'error_type': 'PROFILE_JSON_DECODE_ERROR',
                           'message': f"프로파일 파일 JSON 디코딩 오류: {params['historical_profile_path']}"});
            historical_profile = None
        except Exception as e:
            errors.append({'column': column_name, 'error_type': 'PROFILE_FILE_ERROR',
                           'message': f"프로파일 파일 처리 중 알 수 없는 오류 ({params['historical_profile_path']}): {e}"});
            historical_profile = None
    elif 'historical_profile' in params:
        historical_profile = params['historical_profile'];
        historical_profile.setdefault('null_count', 0);
        historical_profile.setdefault('count', 0)
        if 'frequencies' in historical_profile and str(np.nan) not in historical_profile['frequencies'] and \
                historical_profile['null_count'] > 0 and historical_profile['count'] > 0:
            historical_profile['frequencies'][str(np.nan)] = historical_profile['null_count'] / historical_profile[
                'count']
    else:
        errors.append(
            {'column': column_name, 'error_type': 'CONFIG_ERROR', 'message': "과거 분포 프로파일 설정 누락"});
        return errors
    if not historical_profile:
        if not errors: errors.append(
            {'column': column_name, 'error_type': 'PROFILE_LOAD_ERROR', 'message': "과거 분포 프로파일을 가져올 수 없습니다."})
        return errors
    if historical_profile.get('count', 0) == 0 and current_profile_data.get('count', 0) > 0: logger.info(
        f"정보: '{column_name}' 과거 분포 프로파일 비어있으나 현재 데이터 있어 신규 코드 위주 검사."); historical_profile = {'unique_codes': [],
                                                                                               'frequencies': {},
                                                                                               'total_unique_count': 0,
                                                                                               'count': 0,
                                                                                               'null_count': 0}
    thresholds = params.get('thresholds', {});
    h_codes = set(str(c) for c in historical_profile.get('unique_codes', []));
    h_freqs = {str(k): v for k, v in historical_profile.get('frequencies', {}).items()};
    h_uc = historical_profile.get('total_unique_count', 0);
    h_tc = historical_profile.get('count', 0);
    h_nc = historical_profile.get('null_count', 0)
    c_codes = set(str(c) for c in current_profile_data.get('unique_codes', []));
    c_freqs = {str(k): v for k, v in current_profile_data.get('frequencies', {}).items()};
    c_uc = current_profile_data.get('total_unique_count', 0);
    c_tc = current_profile_data.get('count', 0);
    c_nc = current_profile_data.get('null_count', 0)
    if c_tc == 0 and h_tc == 0: return errors
    if c_tc == 0 and h_tc > 0: errors.append({'column': column_name, 'error_type': 'ALL_DATA_DISAPPEARED',
                                              'message': f"'{column_name}' 현재 데이터 없음 (과거엔 존재)"}); return errors
    null_tol = thresholds.get('null_ratio_change_tolerance_abs', 0.05)
    if h_tc > 0 and c_tc > 0 and abs((c_nc / c_tc) - (h_nc / h_tc)) > null_tol: errors.append(
        {'column': column_name, 'error_type': 'NULL_RATIO_CHANGED', 'current_null_ratio': (c_nc / c_tc),
         'historical_null_ratio': (h_nc / h_tc),
         'message': f"NULL 값 비율 변화 ({c_nc / c_tc:.2%} vs {h_nc / h_tc:.2%}), 임계치({null_tol:.0%}) 초과"})
    new_nn_codes = {c for c in (c_codes - h_codes) if c != str(np.nan)}
    if new_nn_codes and c_tc > 0:
        new_freq_sum = sum(c_freqs.get(c, 0) for c in new_nn_codes)
        if new_freq_sum > thresholds.get('new_code_max_ratio', 0.05): errors.append(
            {'column': column_name, 'error_type': 'HIGH_NEW_CODE_RATIO', 'ratio': new_freq_sum,
             'new_codes_sample': list(new_nn_codes)[:5],
             'message': f"신규 코드(NULL 제외) 빈도 합({new_freq_sum:.2%}) 임계치({thresholds.get('new_code_max_ratio', 0.05):.0%}) 초과. 예: {list(new_nn_codes)[:5]}"})
    for code in h_codes:
        if code == str(np.nan): continue
        if abs(c_freqs.get(code, 0.0) - h_freqs.get(code, 0.0)) > thresholds.get('freq_change_tolerance_abs',
                                                                                 0.1): errors.append(
            {'column': column_name, 'error_type': 'CODE_FREQUENCY_CHANGED', 'code': code,
             'current_freq': c_freqs.get(code, 0.0), 'historical_freq': h_freqs.get(code, 0.0),
             'message': f"코드 '{code}' 빈도 변화 ({c_freqs.get(code, 0.0):.2%} vs {h_freqs.get(code, 0.0):.2%}), 임계치 초과"})
    h_uc_nn = h_uc - (1 if h_nc > 0 and str(np.nan) in h_codes else 0);
    c_uc_nn = c_uc - (1 if c_nc > 0 and str(np.nan) in c_codes else 0)
    if h_uc_nn > 0 and abs(c_uc_nn - h_uc_nn) / h_uc_nn > thresholds.get('unique_count_tolerance_ratio', 0.2):
        errors.append({'column': column_name, 'error_type': 'UNIQUE_CODE_COUNT_CHANGED', 'current_count': c_uc_nn,
                       'historical_count': h_uc_nn, 'change_ratio': abs(c_uc_nn - h_uc_nn) / h_uc_nn,
                       'message': f"고유 코드 수(NULL 제외) 변화율({abs(c_uc_nn - h_uc_nn) / h_uc_nn:.2%}) 임계치 초과"})
    elif c_uc_nn > 0 and h_uc_nn == 0:
        errors.append(
            {'column': column_name, 'error_type': 'NEW_CODES_APPEARED_HISTORICAL_EMPTY', 'current_count': c_uc_nn,
             'message': f"과거 고유 코드(NULL 제외) 없었으나 현재 {c_uc_nn}개 발견"})
    return errors


def check_numeric_volatility(current_df_series, column_name, params, q_processor=None, full_current_df=None,
                             disable_tqdm=True):
    errors = [];
    historical_profile_map = {};
    group_by_columns = params.get('group_by_columns')
    if current_df_series is None or current_df_series.empty: logger.info(
        f"정보: 컬럼 '{column_name}' 현재 데이터 비어 변동성 검사 건너뜀."); return errors
    if 'historical_data_table' in params and 'historical_data_column' in params:
        if not q_processor: errors.append(
            {'column': column_name, 'error_type': 'CONFIG_ERROR', 'message': "DB용 QueryProcessor 필요"}); return errors
        table, hist_col = params['historical_data_table'], params['historical_data_column'];
        filter_condition = params.get('historical_data_filter', '1=1');
        query_params = params.get('historical_data_query_params', {});
        db_engine = params.get('db_engine', 'hive')
        if query_params: filter_condition = filter_condition.format(**query_params)
        gb_select_str = (", ".join(group_by_columns) + ", ") if group_by_columns else "";
        gb_clause_str = ("GROUP BY " + ", ".join(group_by_columns)) if group_by_columns else ""
        query = f"""SELECT {gb_select_str} AVG({hist_col}) AS mean_val, STDDEV_SAMP({hist_col}) AS std_val, MIN({hist_col}) AS min_val, MAX({hist_col}) AS max_val, PERCENTILE_APPROX({hist_col}, 0.5) AS median_val, PERCENTILE_APPROX({hist_col}, 0.25) AS q1_val, PERCENTILE_APPROX({hist_col}, 0.75) AS q3_val, COUNT({hist_col}) AS count_val FROM {table} WHERE {filter_condition} AND {hist_col} IS NOT NULL {gb_clause_str}"""
        try:
            profile_df_db = q_processor.fetch_to_pandas(query=query, engine=db_engine, limit=None)
            if profile_df_db.empty:
                logger.info(f"경고: DB에서 '{hist_col}' 과거 숫자 데이터 없음 (컬럼:{column_name}, 그룹:{group_by_columns}).")
            else:
                for _, row in profile_df_db.iterrows():
                    profile = {'mean': float(row['mean_val']) if pd.notna(row['mean_val']) else 0.0,
                               'std': float(row['std_val']) if pd.notna(row['std_val']) else 0.0,
                               'min': float(row['min_val']) if pd.notna(row['min_val']) else 0.0,
                               'max': float(row['max_val']) if pd.notna(row['max_val']) else 0.0,
                               'median': float(row['median_val']) if pd.notna(row['median_val']) else 0.0,
                               'q1': float(row['q1_val']) if pd.notna(row['q1_val']) else 0.0,
                               'q3': float(row['q3_val']) if pd.notna(row['q3_val']) else 0.0,
                               'count': int(row['count_val']) if pd.notna(row['count_val']) else 0}
                    if group_by_columns:
                        key_tuple = tuple(
                            str(row[gb_col]) if pd.notna(row[gb_col]) else '__NONE_GROUP_KEY__' for gb_col in
                            group_by_columns);
                        historical_profile_map[key_tuple] = profile
                    else:
                        historical_profile_map['__overall__'] = profile;
                        break
        except Exception as e:
            errors.append({'column': column_name, 'error_type': 'DB_AGGREGATE_PROFILE_ERROR',
                           'message': f"DB 과거 숫자 프로파일 생성 실패 ({column_name}, 그룹:{group_by_columns}): {e}"});
            return errors
    elif 'historical_profile_path' in params or 'historical_profile' in params:
        if group_by_columns: errors.append(
            {'column': column_name, 'error_type': 'CONFIG_ERROR', 'message': '파일/객체 프로파일은 그룹별 검사 미지원'}); return errors
        profile_source = params.get('historical_profile')
        if 'historical_profile_path' in params:
            try:
                with open(params['historical_profile_path'], 'r', encoding='utf-8') as f:
                    profile_source = json.load(f)
            except FileNotFoundError:
                errors.append({'column': column_name, 'error_type': 'PROFILE_FILE_NOT_FOUND',
                               'message': f"프로파일 파일을 찾을 수 없습니다: {params['historical_profile_path']}"});
                return errors
            except json.JSONDecodeError:
                errors.append({'column': column_name, 'error_type': 'PROFILE_JSON_DECODE_ERROR',
                               'message': f"프로파일 파일 JSON 디코딩 오류: {params['historical_profile_path']}"});
                return errors
            except Exception as e:
                errors.append({'column': column_name, 'error_type': 'PROFILE_FILE_ERROR',
                               'message': f"프로파일 파일 오류: {e}"});
                return errors
        if profile_source:
            cleaned_profile = {
                key: (int(value) if key == 'count' and pd.notna(value) else (float(value) if pd.notna(value) else 0.0))
                for key, value in profile_source.items()};
            historical_profile_map['__overall__'] = {
                **{'mean': 0.0, 'std': 0.0, 'min': 0.0, 'max': 0.0, 'median': 0.0, 'q1': 0.0, 'q3': 0.0, 'count': 0},
                **cleaned_profile}
        else:
            errors.append(
                {'column': column_name, 'error_type': 'CONFIG_ERROR', 'message': "과거 숫자 프로파일 설정 누락"});
            return errors
    if not historical_profile_map: logger.info(f"경고: '{column_name}' 과거 프로파일 없음. 변동성 검사 불가."); return errors
    method, thresholds = params.get("method", "z_score").lower(), params.get("thresholds", {})
    num_series_curr = pd.to_numeric(current_df_series, errors='coerce').fillna(0.0)
    for idx, curr_val in tqdm(num_series_curr.items(), total=len(num_series_curr), desc=f"VOLATILITY '{column_name}'",
                              disable=disable_tqdm, leave=False, unit="행"):
        orig_val = current_df_series.loc[idx];
        hist_prof, grp_key_str = None, "전체"
        if group_by_columns:
            if full_current_df is None or full_current_df.empty: err_detail = {'column': column_name, 'row_index': idx,
                                                                               'error_type': 'CONFIG_ERROR',
                                                                               'message': "그룹별 검사 시 전체 DataFrame 필요"}; [
                errors.append(err_detail) if err_detail not in errors else None]; continue
            try:
                curr_grp_key = tuple(str(full_current_df.loc[idx, col]) if pd.notna(
                    full_current_df.loc[idx, col]) else '__NONE_GROUP_KEY__' for col in group_by_columns)
            except KeyError:
                logger.info(f"경고: 그룹 키 생성 중 오류 (인덱스 {idx}). 건너뜀.");
                continue
            grp_key_str = str(curr_grp_key);
            hist_prof = historical_profile_map.get(curr_grp_key)
            if not hist_prof: continue
        else:
            hist_prof = historical_profile_map.get('__overall__')
        if not hist_prof or hist_prof.get('count', 0) == 0: continue
        is_outlier_flag = False;
        detail_message_suffix = "";
        msg_template_str = params.get('message', "컬럼 '{column_name}'(그룹: {group_key_str}) 값 '{value}' 과거 대비 비정상 변동.")
        try:
            formatted_msg_template = msg_template_str.format(column_name=column_name, value=orig_val,
                                                             group_key_str=grp_key_str)
        except KeyError:
            formatted_msg_template = params.get('message', "컬럼 '{column_name}' 값 '{value}' 과거 대비 비정상 변동.").format(
                column_name=column_name, value=orig_val)
        if method == "z_score":
            m, s, z_thresh = hist_prof.get("mean", 0.0), hist_prof.get("std", 0.0), thresholds.get("z_score_threshold",
                                                                                                   3.0)
            if s == 0:
                if curr_val != m: is_outlier_flag = True; detail_message_suffix = f"과거 std 0, 현재값({curr_val}) != 평균({m})"
            elif abs((curr_val - m) / s) > z_thresh:
                is_outlier_flag = True;
                detail_message_suffix = f"Z-점수 {(curr_val - m) / s:.2f} (임계값: {z_thresh})"
        elif method == "iqr":
            q1_h, q3_h, iqr_mult = hist_prof.get("q1", 0.0), hist_prof.get("q3", 0.0), thresholds.get("iqr_multiplier",
                                                                                                      1.5);
            iqr_h = max(0, q3_h - q1_h);
            low_b, upp_b = q1_h - iqr_mult * iqr_h, q3_h + iqr_mult * iqr_h
            if not (
                    low_b <= curr_val <= upp_b): is_outlier_flag = True; detail_message_suffix = f"IQR 범위 [{low_b:.2f}, {upp_b:.2f}] 벗어남 (Q1:{q1_h:.2f}, Q3:{q3_h:.2f}, IQR:{iqr_h:.2f})"
        else:
            errors.append({'column': column_name, 'row_index': idx, 'value': orig_val,
                           'group_key': grp_key_str if group_by_columns else '', 'error_type': 'CONFIG_ERROR',
                           'message': f"알 수 없는 변동성 검증 방법: {method}"});
            continue
        if is_outlier_flag: errors.append({'column': column_name, 'row_index': idx, 'value': orig_val,
                                           'group_key': grp_key_str if group_by_columns else '', 'error_type': (
                                                                                                                   'GROUP_' if group_by_columns else 'OVERALL_') + 'NUMERIC_VOLATILITY_DETECTED',
                                           'message': f"{formatted_msg_template} ({detail_message_suffix})"})
    return errors


def check_column_equality(df, params, disable_tqdm=True):
    errors = [];
    col1, col2 = params.get('column1'), params.get('column2')
    if df is None or df.empty: return []
    if not all([col1, col2, col1 in df.columns, col2 in df.columns]): errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': f'컬럼 설정 오류: {col1}, {col2}'}); return errors
    msg_template = params.get('message', "행 {row_index}: 컬럼 '{col1_name}'(값: {val1})과 '{col2_name}'(값: {val2}) 불일치.")
    df_compare = df[[col1, col2]].astype(str).fillna('__NONE_PLACEHOLDER__')
    for idx, row_series in tqdm(df.iterrows(), total=len(df), desc=f"EQUALITY '{col1}' vs '{col2}'",
                                disable=disable_tqdm, leave=False, unit="행"):
        if df_compare.loc[idx, col1] != df_compare.loc[idx, col2]: errors.append(
            {'columns': [col1, col2], 'row_index': idx, 'error_type': 'COLUMN_MISMATCH',
             'message': msg_template.format(row_index=idx, col1_name=col1, val1=df.loc[idx, col1], col2_name=col2,
                                            val2=df.loc[idx, col2]), 'error_row_data': df.loc[idx].to_dict()})
    return errors


def check_duplicate_rows(df, params, disable_tqdm=True):
    errors = [];
    subset = params.get('subset_columns')
    if df is None or df.empty: return []
    if subset and not all(col in df.columns for col in subset): subset = None
    msg_template = params.get('message', "중복 행 발견 (행 인덱스: {row_index}). 검사 대상: {checked_columns}")
    df_for_dup_check = df[subset].astype(str).fillna('__NAN_PLACEHOLDER__') if subset else df.astype(str).fillna(
        '__NAN_PLACEHOLDER__')
    duplicates_series = df_for_dup_check.duplicated(keep=False)
    for idx in tqdm(df[duplicates_series].index, total=duplicates_series.sum(),
                    desc=f"DUPLICATES (subset: {subset or 'all'})", disable=disable_tqdm or not duplicates_series.any(),
                    leave=False, unit="행"):
        errors.append({'columns': subset or 'all', 'row_index': idx, 'error_type': 'DUPLICATE_ROW',
                       'message': msg_template.format(row_index=idx, checked_columns=subset or '모든 컬럼'),
                       'error_row_data': df.loc[idx].to_dict()})
    return errors


def check_aggregate_value_trend(df, params, q_processor=None):
    errors = []
    agg_col = params.get('column_to_aggregate')
    agg_func = params.get('aggregate_function', 'SUM').upper()
    group_by_columns = params.get('group_by_columns')
    date_col_db = params.get('date_column_for_period')
    date_fmt_db = params.get('date_column_format', 'YYYYMM')
    is_part = params.get('date_column_is_partition_key', False)
    current_period_value = str(params.get('current_period_value', ''))
    hist_periods_def = params.get('comparison_periods', {})
    table = params.get('historical_data_table')
    engine = params.get('db_engine', 'hive')
    inc_thresh = params.get('threshold_ratio_increase')
    dec_thresh = params.get('threshold_ratio_decrease')
    hist_base_filter = params.get('historical_base_filter', '1=1')

    # --- 필수 파라미터 검증 ---
    if not all([agg_col, current_period_value, q_processor, table, date_col_db, date_fmt_db]):
        return [{'rule_type': 'aggregate_value_trend', 'error_type': 'CONFIG_ERROR', 'message': "필수 파라미터 누락"}]
    if agg_func not in ['SUM', 'AVG', 'COUNT']:
        return [{'rule_type': 'aggregate_value_trend', 'error_type': 'CONFIG_ERROR',
                 'message': "지원 함수: 'SUM', 'AVG', 'COUNT'"}]
    if not hist_periods_def or 'type' not in hist_periods_def or 'n' not in hist_periods_def:
        return [
            {'rule_type': 'aggregate_value_trend', 'error_type': 'CONFIG_ERROR', 'message': "comparison_periods 형식 오류"}]
    if group_by_columns and not isinstance(group_by_columns, list):
        return [{'rule_type': 'aggregate_value_trend', 'error_type': 'CONFIG_ERROR',
                 'message': "group_by_columns는 리스트여야 함"}]

    # --- 현재 값 계산 ---
    current_aggregates_map = {}
    if not df.empty:
        if group_by_columns:
            if not all(col in df.columns for col in group_by_columns):
                return [{'rule_type': 'aggregate_value_trend', 'error_type': 'CONFIG_ERROR',
                         'message': f"group_by_columns 일부 컬럼 없음: {group_by_columns}"}]
            df_grouped = df.copy()
            for gb_col in group_by_columns:
                if gb_col in df_grouped.columns:
                    df_grouped[gb_col] = df_grouped[gb_col].fillna('__NONE_GROUP_KEY__').astype(str)

            if agg_func == 'COUNT':
                agg_series = df_grouped.groupby(group_by_columns, observed=True, dropna=False).size()
            else:
                if agg_col not in df.columns:
                    return [{'rule_type': 'aggregate_value_trend', 'error_type': 'COLUMN_NOT_FOUND',
                             'message': f"컬럼 '{agg_col}' 없음"}]
                numeric_col = pd.to_numeric(df_grouped[agg_col], errors='coerce').fillna(0)
                df_grouped['__agg_target__'] = numeric_col
                grouped_obj_agg = df_grouped.groupby(group_by_columns, observed=True, dropna=False)
                agg_series = grouped_obj_agg['__agg_target__'].sum() if agg_func == 'SUM' else grouped_obj_agg[
                    '__agg_target__'].mean().fillna(0.0)

            if agg_series is not None:
                current_aggregates_map = {(k if isinstance(k, tuple) else (k,)): float(v) for k, v in
                                          agg_series.items()}
        else:
            if agg_func == 'COUNT':
                val = float(len(df) if agg_col in ['*', '1'] else df[agg_col].count())
            else:
                if agg_col not in df.columns:
                    return [{'rule_type': 'aggregate_value_trend', 'error_type': 'COLUMN_NOT_FOUND',
                             'message': f"컬럼 '{agg_col}' 없음"}]
                num_col = pd.to_numeric(df[agg_col], errors='coerce').fillna(0)
                val = float(num_col.sum()) if agg_func == 'SUM' else (
                    float(num_col.mean()) if not num_col.empty else 0.0)
            current_aggregates_map['__overall__'] = val

    final_hist_map = {}
    final_comp_label = "과거 기간 (조회 불가)"
    comp_type = hist_periods_def.get("type")
    n_p = hist_periods_def.get("n", 1)
    base_offset = current_period_value
    fmt_offset_in = "%Y%m%d" if len(base_offset) == 8 else ("%Y%m" if len(base_offset) == 6 else "")

    if not fmt_offset_in:
        return [{'rule_type': 'aggregate_value_trend', 'error_type': 'CONFIG_ERROR',
                 'message': f"current_period_value 형식 오류"}]

    # 'previous_n_months' 또는 'previous_n_days' 타입이고, 'average'가 아닌 경우 -> n번째 과거만 직접 조회
    if comp_type in ["previous_n_months", "previous_n_days"] and "average" not in comp_type:
        offset_kwargs = {'months_offset': -n_p} if 'months' in comp_type else {'days_offset': -n_p}
        output_fmt = '%Y%m' if 'months' in comp_type else '%Y%m%d'

        try:
            target_hist_p_str = _get_offset_date_str(base_offset, current_format_str=fmt_offset_in,
                                                     output_format_str=output_fmt, **offset_kwargs)

            if group_by_columns:
                final_hist_map = _get_historical_grouped_aggregates(q_processor, table, agg_col, agg_func,
                                                                    group_by_columns, date_col_db, date_fmt_db, is_part,
                                                                    target_hist_p_str, engine, hist_base_filter)
            else:
                final_hist_map = {
                    '__overall__': _get_historical_aggregate_value(q_processor, table, agg_col, agg_func, date_col_db,
                                                                   date_fmt_db, is_part, target_hist_p_str, engine,
                                                                   hist_base_filter)}

            final_comp_label = f"이전 {n_p}{'개월' if 'months' in comp_type else '일'}차 ({target_hist_p_str})"
        except ValueError as ve_off:
            logger.info(f"경고: 과거 기간 문자열 생성 오류 ({ve_off}).")

    # 'average' 타입인 경우 -> 기존의 for 루프 로직 사용
    elif "average" in comp_type:
        tmp_hist_vals = {}
        actual_hist_labels = []
        for i in range(1, n_p + 1):
            target_hist_p_str = ""
            try:
                if "months" in comp_type:
                    target_hist_p_str = _get_offset_date_str(base_offset, months_offset=-i,
                                                             current_format_str=fmt_offset_in, output_format_str="%Y%m")
                elif "days" in comp_type:
                    target_hist_p_str = _get_offset_date_str(base_offset, days_offset=-i, current_format_str="%Y%m%d",
                                                             output_format_str="%Y%m%d")
            except ValueError as ve_off:
                logger.info(f"경고: 과거 기간 문자열 생성 오류 ({ve_off}). 건너뜀.")
                continue

            period_hist_data = _get_historical_grouped_aggregates(q_processor, table, agg_col, agg_func,
                                                                  group_by_columns, date_col_db, date_fmt_db, is_part,
                                                                  target_hist_p_str, engine,
                                                                  hist_base_filter) if group_by_columns else {
                '__overall__': _get_historical_aggregate_value(q_processor, table, agg_col, agg_func, date_col_db,
                                                               date_fmt_db, is_part, target_hist_p_str, engine,
                                                               hist_base_filter)}

            if period_hist_data:
                if target_hist_p_str not in actual_hist_labels:
                    actual_hist_labels.append(target_hist_p_str)
                for grp_k, v_val in period_hist_data.items():
                    tmp_hist_vals.setdefault(grp_k, []).append(float(v_val))

        if tmp_hist_vals:
            for grp_k, v_list in tmp_hist_vals.items():
                final_hist_map[grp_k] = np.mean(v_list) if v_list else 0.0
            final_comp_label = f"이전 {len(set(actual_hist_labels))}{'개월' if 'months' in comp_type else '일'}({','.join(sorted(list(set(actual_hist_labels))))}) 평균"
        else:
            final_comp_label = f"이전 {n_p}{'개월' if 'months' in comp_type else '일'} 평균 (조회 불가)"
            logger.info(f"정보: '{agg_col}' 과거 평균 집계값 조회 불가. 과거 평균 0으로 간주.")

    # --- 비교 및 오류 생성 ---
    all_comp_keys = set(current_aggregates_map.keys()) | set(final_hist_map.keys())
    if not all_comp_keys and not group_by_columns: all_comp_keys.add('__overall__')

    for grp_k_or_all in all_comp_keys:
        curr_agg_v = float(current_aggregates_map.get(grp_k_or_all, 0.0))
        hist_ref_v = float(final_hist_map.get(grp_k_or_all, 0.0))

        min_value_threshold = params.get('min_value_threshold')
        if min_value_threshold is not None:
            if curr_agg_v < min_value_threshold and hist_ref_v < min_value_threshold:
                continue

        chg_info = {'current_value': curr_agg_v, 'historical_value': hist_ref_v,
                    'historical_period_label': final_comp_label}
        if group_by_columns and grp_k_or_all != '__overall__':
            chg_info['group_key'] = str(grp_k_or_all)

        if hist_ref_v == 0:
            if curr_agg_v != 0 and inc_thresh is not None and curr_agg_v > 0:
                errors.append(
                    {'rule_type': 'aggregate_value_trend', 'column': agg_col, 'function': agg_func, **chg_info,
                     'error_type': 'AGG_INCREASED_FROM_ZERO',
                     'message': f"'{agg_col}' {agg_func} ({curr_agg_v:,.2f}) 과거({final_comp_label}) 0 대비 급증."})
        else:
            chg_ratio = (curr_agg_v - hist_ref_v) / abs(hist_ref_v)
            chg_info['change_ratio'] = float(chg_ratio)
            chg_str = f"(현재 {curr_agg_v:,.2f} vs {final_comp_label} {hist_ref_v:,.2f}, 변화율: {chg_ratio:.2%})"
            msg_prefix = f"'{agg_col}' {agg_func} "
            if group_by_columns and grp_k_or_all != '__overall__':
                msg_prefix = f"그룹 '{str(grp_k_or_all)}'의 " + msg_prefix
            if inc_thresh is not None and chg_ratio > inc_thresh:
                errors.append(
                    {'rule_type': 'aggregate_value_trend', 'column': agg_col, 'function': agg_func, **chg_info,
                     'error_type': 'AGG_VALUE_INCREASED',
                     'message': f"{msg_prefix}임계치({inc_thresh:.0%}) 초과 증가. {chg_str}"})
            elif dec_thresh is not None and chg_ratio < -dec_thresh:
                errors.append(
                    {'rule_type': 'aggregate_value_trend', 'column': agg_col, 'function': agg_func, **chg_info,
                     'error_type': 'AGG_VALUE_DECREASED',
                     'message': f"{msg_prefix}임계치({dec_thresh:.0%}) 초과 감소. {chg_str}"})

    return errors


def check_total_row_count_trend(df, params, q_processor=None):
    params_for_rc = params.copy()
    params_for_rc['column_to_aggregate'] = params.get('count_aggregate_column', '1')
    params_for_rc['aggregate_function'] = 'COUNT'
    return check_aggregate_value_trend(df, params_for_rc, q_processor)


def check_schema_change(df_placeholder, params, q_processor=None):
    errors = [];
    table_name = params.get("table_name_in_db");
    engine_name = params.get("engine")
    baseline_schema_dir = params.get("baseline_schema_dir", SCHEMA_BASELINE_DIR);
    auto_manage_baseline = params.get("auto_manage_baseline", True);
    force_update_baseline = params.get("force_update_baseline", False);
    update_baseline_if_no_change = params.get("update_baseline_if_no_change", False)
    expected_schema_list_param = params.get("expected_schema");
    expected_schema_path_param = params.get("expected_schema_path")
    check_options = params.get("check_options",
                               {"detect_new_columns": True, "detect_missing_columns": True, "detect_type_changes": True,
                                "detect_order_changes": False, "detect_nullable_changes": True,
                                "detect_comment_changes": False})
    if not table_name or not engine_name: errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "'table_name_in_db'와 'engine' 파라미터는 필수입니다."}); return errors
    if not q_processor or not hasattr(q_processor, 'describe_table'): errors.append({'error_type': 'CONFIG_ERROR',
                                                                                     'message': "스키마 조회를 위한 'describe_table' 메소드를 가진 QueryProcessor가 필요합니다."}); return errors
    current_schema_list_standardized = [];
    raw_current_schema_df = None
    try:
        raw_current_schema_df = q_processor.describe_table(engine=engine_name, table_name=table_name)
        if raw_current_schema_df is None: errors.append(
            {'error_type': 'DB_SCHEMA_FETCH_ERROR', 'table_name': table_name, 'engine': engine_name,
             'message': f"테이블 '{table_name}' 스키마 조회 결과 None."}); return errors
        if raw_current_schema_df.empty:
            logger.info(f"경고: 테이블 '{table_name}' 스키마 정보 비어있음.")
        else:
            if engine_name.lower() == "edw":
                expected_oracle_cols = ['COLUMN_NAME', 'DATA_TYPE', 'NULLABLE', 'COLUMN_ID'];
                required_cols_present = all(col in raw_current_schema_df.columns for col in expected_oracle_cols)
                if not required_cols_present: errors.append(
                    {'error_type': 'INTERNAL_ERROR', 'table_name': table_name, 'engine': engine_name,
                     'message': f"EDW 스키마 필수 컬럼 부족"}); return errors
                for order_idx, (_, row) in enumerate(
                        raw_current_schema_df.iterrows()): current_schema_list_standardized.append(
                    {'name': str(row['COLUMN_NAME']).lower(), 'type': str(row['DATA_TYPE']).lower(),
                     'comment': str(row.get('COMMENTS', '')).strip(),
                     'is_nullable': str(row.get('NULLABLE', 'Y')).upper() == 'Y',
                     'order': int(row.get('COLUMN_ID', order_idx))})
            elif engine_name.lower() == "hive":
                name_col, type_col, com_col = 'COL_NAME', 'DATA_TYPE', 'COMMENT'
                if name_col not in raw_current_schema_df.columns or type_col not in raw_current_schema_df.columns: errors.append(
                    {'error_type': 'INTERNAL_ERROR', 'table_name': table_name, 'engine': engine_name,
                     'message': "Hive 스키마 필수 컬럼 부족"}); return errors
                part_idx = next((i for i, r in raw_current_schema_df.iterrows() if
                                 isinstance(r.get(name_col), str) and r.get(name_col, "").strip().startswith(
                                     "# Partition Information")), len(raw_current_schema_df))
                for order_idx, (_, row) in enumerate(raw_current_schema_df.iloc[:part_idx].iterrows()):
                    cn, ct = row.get(name_col), row.get(type_col)
                    if isinstance(cn, str) and cn.strip() and not cn.strip().startswith("#") and isinstance(ct,
                                                                                                            str) and ct.strip(): current_schema_list_standardized.append(
                        {'name': cn.lower().strip(), 'type': ct.lower().strip(),
                         'comment': str(row.get(com_col, '')).strip(), 'is_nullable': True, 'order': order_idx})
            else:
                errors.append({'error_type': 'CONFIG_ERROR', 'table_name': table_name, 'engine': engine_name,
                               'message': f"알 수 없는 engine: {engine_name}."});
                return errors
            if not current_schema_list_standardized and not raw_current_schema_df.empty: errors.append(
                {'error_type': 'INTERNAL_ERROR', 'table_name': table_name, 'engine': engine_name,
                 'message': "스키마 표준 형식 변환 실패."}); return errors
            current_schema_list_standardized.sort(key=lambda x: x.get('order', -1))
    except Exception as e:
        errors.append({'error_type': 'DB_SCHEMA_FETCH_ERROR', 'table_name': table_name, 'engine': engine_name,
                       'message': f"스키마 조회/변환 중 예외: {e}"});
        return errors

    expected_schema_list = [];
    baseline_schema_file_path = ""
    if auto_manage_baseline:
        if not os.path.exists(baseline_schema_dir):
            try:
                os.makedirs(baseline_schema_dir)
            except OSError as e_mkdir:
                errors.append({'error_type': 'BASELINE_SCHEMA_ERROR',
                               'message': f"기준 스키마 디렉토리 생성 실패 '{baseline_schema_dir}': {e_mkdir}"});
                return errors
        baseline_schema_file_path = os.path.join(baseline_schema_dir,
                                                 f"{table_name.replace('.', '_')}_{engine_name}_baseline.json")
        if force_update_baseline:
            if not current_schema_list_standardized and (
                    raw_current_schema_df is None or raw_current_schema_df.empty): errors.append(
                {'error_type': 'DB_SCHEMA_FETCH_ERROR',
                 'message': f"'{table_name}' 현재 스키마 비어있어 기준 저장 불가."}); return errors
            try:
                with open(baseline_schema_file_path, 'w', encoding='utf-8') as f:
                    json.dump(current_schema_list_standardized, f, ensure_ascii=False, indent=4)
                logger.info(f"정보: '{table_name}' 기준 스키마 강제 업데이트됨: {baseline_schema_file_path}");
                return []
            except Exception as e_write:
                errors.append({'error_type': 'BASELINE_SCHEMA_WRITE_ERROR',
                               'message': f"강제 업데이트 중 기준 파일 저장 실패: {e_write}"});
                return errors
        if os.path.exists(baseline_schema_file_path):
            try:
                with open(baseline_schema_file_path, 'r', encoding='utf-8') as f:
                    expected_schema_list = json.load(f)
                if not isinstance(expected_schema_list, list) or not all(
                        'name' in x and 'type' in x for x in expected_schema_list): logger.info(
                    f"경고: 기준 파일 형식 오류. 새 기준으로 사용."); expected_schema_list = []
            except Exception as e_read:
                errors.append({'error_type': 'BASELINE_SCHEMA_READ_ERROR',
                               'message': f"기준 파일 로드 실패: {e_read}"});
                expected_schema_list = []
        if not expected_schema_list:
            if not current_schema_list_standardized and (raw_current_schema_df is None or raw_current_schema_df.empty):
                if not errors: errors.append({'error_type': 'SCHEMA_UNAVAILABLE',
                                              'message': f"'{table_name}' 현재 스키마 비어있어 기준 생성 불가."}); return errors
            try:
                with open(baseline_schema_file_path, 'w', encoding='utf-8') as f:
                    json.dump(current_schema_list_standardized, f, ensure_ascii=False, indent=4)
                logger.info(f"정보: '{table_name}' 현재 스키마를 기준으로 저장. 다음 실행부터 비교.");
                return []
            except Exception as e_write_new:
                errors.append({'error_type': 'BASELINE_SCHEMA_WRITE_ERROR',
                               'message': f"새 기준 파일 저장 실패: {e_write_new}"});
                return errors
    elif expected_schema_path_param:
        try:
            with open(expected_schema_path_param, 'r', encoding='utf-8') as f:
                expected_schema_list = json.load(f)
        except FileNotFoundError:
            errors.append({'error_type': 'PROFILE_FILE_NOT_FOUND',
                           'message': f"기대 스키마 파일을 찾을 수 없습니다: {expected_schema_path_param}"});
            return errors
        except json.JSONDecodeError:
            errors.append({'error_type': 'PROFILE_JSON_DECODE_ERROR',
                           'message': f"기대 스키마 파일 JSON 디코딩 오류: {expected_schema_path_param}"});
            return errors
        except Exception as e:
            errors.append({'error_type': 'PROFILE_FILE_ERROR',
                           'message': f"기대 스키마 파일({expected_schema_path_param}) 로드 실패: {e}"});
            return errors
    elif expected_schema_list_param:
        expected_schema_list = expected_schema_list_param
    else:
        errors.append({'error_type': 'CONFIG_ERROR', 'message': "기대 스키마 정보 누락 및 자동 기준 관리 비활성화."});
        return errors
    if not isinstance(expected_schema_list, list) or not all(
            isinstance(item, dict) and 'name' in item and 'type' in item for item in
            expected_schema_list): errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "기대 스키마는 'name', 'type' 키를 가진 딕셔너리 리스트여야 함."}); return errors
    exp_map = {col['name'].lower(): {**col, 'name_original': col['name']} for col in expected_schema_list};
    cur_map = {col['name'].lower(): {**col, 'name_original': col['name']} for col in current_schema_list_standardized}
    exp_names = set(exp_map.keys());
    cur_names = set(cur_map.keys());
    common_names = sorted(list(exp_names & cur_names))
    if check_options.get("detect_missing_columns", True):
        missing = sorted(list(exp_names - cur_names))
        if missing: errors.append(
            {'error_type': 'SCHEMA_MISSING_COLUMNS', 'table_name': table_name, 'engine': engine_name,
             'details': {'missing_columns': [exp_map[mc]['name_original'] for mc in missing]},
             'message': f"기대 컬럼 누락: {[exp_map[mc]['name_original'] for mc in missing]}"})
    if check_options.get("detect_new_columns", True):
        new = sorted(list(cur_names - exp_names))
        if new: errors.append({'error_type': 'SCHEMA_NEW_COLUMNS', 'table_name': table_name, 'engine': engine_name,
                               'details': {'new_columns': [
                                   {'name': cur_map[nc]['name_original'], 'type': cur_map[nc].get('type')} for nc in new
                                   if nc in cur_map]},
                               'message': f"새로운 컬럼 발견: {[cur_map[nc]['name_original'] for nc in new if nc in cur_map]}"})
    if check_options.get("detect_order_changes", False) and common_names:
        exp_order = [c['name'].lower() for c in expected_schema_list if c['name'].lower() in common_names];
        cur_order = [c['name'] for c in current_schema_list_standardized if c['name'] in common_names]
        if exp_order != cur_order: errors.append(
            {'error_type': 'SCHEMA_COLUMN_ORDER_CHANGED', 'table_name': table_name, 'engine': engine_name,
             'details': {'expected_common_order': [exp_map.get(cn, {}).get('name_original', cn) for cn in exp_order],
                         'current_common_order': [cur_map.get(cn, {}).get('name_original', cn) for cn in cur_order]},
             'message': "공통 컬럼 순서 변경됨."})
    for col_l in common_names:
        exp_c, cur_c, orig_exp_n = exp_map[col_l], cur_map[col_l], exp_map[col_l].get('name_original',
                                                                                      exp_map[col_l]['name'])

        def norm_type(t, eng):
            if not isinstance(t, str): t = str(t)
            t = t.lower().replace(" ", "");
            t = re.sub(r'\(\s*', '(', t);
            t = re.sub(r'\s*,\s*', ',', t);
            t = re.sub(r'\s*\)', ')', t)
            if eng and eng.lower() == "hive":
                t = "string" if t.startswith(("varchar", "char")) else ("decimal" if "decimal(" in t else t)
            elif eng and eng.lower() == "edw":
                t = "varchar2" if t.startswith("varchar2") else (
                    "number" if (t.startswith("number(") or (t == "number" and '(' not in t)) else t)
            return t

        if check_options.get("detect_type_changes", True) and norm_type(exp_c.get('type', ''),
                                                                        engine_name) != norm_type(cur_c.get('type', ''),
                                                                                                  engine_name): errors.append(
            {'error_type': 'SCHEMA_TYPE_CHANGED', 'table_name': table_name, 'engine': engine_name,
             'column_name': orig_exp_n, 'expected_type': exp_c.get('type'), 'current_type': cur_c.get('type'),
             'message': f"컬럼 '{orig_exp_n}' 타입 변경."})
        if check_options.get("detect_nullable_changes", True) and 'is_nullable' in exp_c and 'is_nullable' in cur_c and \
                exp_c['is_nullable'] != cur_c['is_nullable']: errors.append(
            {'error_type': 'SCHEMA_NULLABLE_CHANGED', 'table_name': table_name, 'engine': engine_name,
             'column_name': orig_exp_n, 'expected_nullable': exp_c['is_nullable'],
             'current_nullable': cur_c['is_nullable'], 'message': f"컬럼 '{orig_exp_n}' Null 허용 여부 변경."})
        if check_options.get("detect_comment_changes",
                             False) and 'comment' in exp_c and 'comment' in cur_c and exp_c.get('comment',
                                                                                                '').strip() != cur_c.get(
            'comment', '').strip(): errors.append(
            {'error_type': 'SCHEMA_COMMENT_CHANGED', 'table_name': table_name, 'engine': engine_name,
             'column_name': orig_exp_n, 'expected_comment': exp_c.get('comment'),
             'current_comment': cur_c.get('comment'), 'message': f"컬럼 '{orig_exp_n}' 코멘트 변경."})
    if not errors and auto_manage_baseline and update_baseline_if_no_change and baseline_schema_file_path and os.path.exists(
            baseline_schema_file_path):
        try:
            with open(baseline_schema_file_path, 'w', encoding='utf-8') as f:
                json.dump(current_schema_list_standardized, f, ensure_ascii=False, indent=4)
            logger.info(f"정보: 스키마 변경 없음. 기준 스키마 업데이트됨: {baseline_schema_file_path}")
        except Exception as e_upd:
            logger.info(f"경고: 기준 스키마 파일 자동 업데이트 실패: {e_upd}")
    return errors


def check_consecutive_trend(df, params, q_processor=None):
    errors = [];
    column_to_aggregate = params.get('column_to_aggregate');
    aggregate_function = params.get('aggregate_function', 'SUM').upper();
    group_by_columns = params.get('group_by_columns')
    date_column_for_trend = params.get('date_column_for_trend');
    trend_type = params.get('trend_type', 'down').lower();
    consecutive_periods = params.get('consecutive_periods', 7)

    # MODIFIED: period_unit 및 date_column_format 파라미터 추가 및 기본값 설정
    period_unit = params.get('period_unit', 'days').lower()
    date_column_format = params.get('date_column_format', 'YYYYMMDD').upper()  # YYYYMM 또는 YYYYMMDD

    historical_data_table = params.get('historical_data_table');
    historical_lookback_periods = params.get('historical_lookback_periods', consecutive_periods + 5);
    historical_base_filter = params.get('historical_base_filter', '1=1');
    engine = params.get('db_engine', 'hive')

    # 파라미터 검증
    if not all([column_to_aggregate, date_column_for_trend, historical_data_table, q_processor]): errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "consecutive_trend_check 필수 파라미터 누락"}); return errors
    if trend_type not in ['down', 'up']: errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "trend_type은 'down' 또는 'up'이어야 합니다."}); return errors
    if not isinstance(consecutive_periods, int) or consecutive_periods < 2: errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "consecutive_periods는 2 이상의 정수여야 합니다."}); return errors
    if period_unit not in ['days', 'months']: errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "period_unit은 'days' 또는 'months'만 지원합니다."}); return errors
    if date_column_format not in ['YYYYMMDD', 'YYYYMM']: errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "date_column_format은 'YYYYMMDD' 또는 'YYYYMM'이어야 합니다."}); return errors
    if group_by_columns and not isinstance(group_by_columns, list): errors.append(
        {'error_type': 'CONFIG_ERROR', 'message': "group_by_columns는 리스트 형태여야 합니다."}); return errors
    if date_column_for_trend not in df.columns: errors.append({'error_type': 'COLUMN_NOT_FOUND',
                                                               'message': f"현재 DataFrame에 날짜 컬럼 '{date_column_for_trend}' 없음"}); return errors

    # 날짜 형식 설정
    pd_datetime_format = '%Y%m%d' if date_column_format == 'YYYYMMDD' else '%Y%m'
    strftime_format_for_output = '%Y%m%d' if date_column_format == 'YYYYMMDD' else '%Y%m'
    strftime_format_for_filter = '%Y%m%d' if date_column_format == 'YYYYMMDD' else '%Y%m'

    try:
        df_copy_for_date = df.copy();
        if date_column_format == 'YYYYMM':  # YYYYMM이면 월의 1일로 변환하여 datetime 객체 생성
            df_copy_for_date[date_column_for_trend] = pd.to_datetime(
                df_copy_for_date[date_column_for_trend].astype(str) + '01', format='%Y%m%d', errors='coerce')
        else:  # YYYYMMDD
            df_copy_for_date[date_column_for_trend] = pd.to_datetime(
                df_copy_for_date[date_column_for_trend].astype(str), format='%Y%m%d', errors='coerce')

        df_original_dates = df_copy_for_date.dropna(subset=[date_column_for_trend])
        if df_original_dates.empty: logger.info(
            f"정보: 규칙 '{params.get('rule_name', 'N/A')}' - 현재 데이터 비어 추세 분석 불가."); return errors
        latest_date_in_current_df = df_original_dates[date_column_for_trend].max()  # datetime 객체
    except Exception as e_date_conv:
        errors.append(
            {'error_type': 'DATA_PROCESSING_ERROR', 'message': f"현재 데이터 날짜 컬럼 변환 중 오류: {e_date_conv}"});
        return errors

    # 현재 DF의 가장 최신 날짜/월에 대한 집계값 계산
    current_period_aggregates_map = {};
    current_day_or_month_df_filtered = df_original_dates[
        df_original_dates[date_column_for_trend] == latest_date_in_current_df]

    if current_day_or_month_df_filtered.empty and aggregate_function == 'COUNT':
        if not group_by_columns: current_period_aggregates_map['__overall__'] = 0.0
    elif not current_day_or_month_df_filtered.empty:
        if group_by_columns:
            if not all(col in current_day_or_month_df_filtered.columns for col in group_by_columns): errors.append(
                {'error_type': 'CONFIG_ERROR', 'message': f"group_by_columns 일부 컬럼 없음"}); return errors
            df_grouped_curr = current_day_or_month_df_filtered.copy()
            for gb_col in group_by_columns:
                if gb_col in df_grouped_curr.columns: df_grouped_curr[gb_col] = df_grouped_curr[gb_col].fillna(
                    '__NONE_GROUP_KEY__').astype(str)
            grouped_obj_curr = df_grouped_curr.groupby(group_by_columns, observed=True, dropna=False);
            agg_series_curr = None
            if aggregate_function == 'COUNT':
                agg_series_curr = grouped_obj_curr.size() if column_to_aggregate in ['*', '1'] else grouped_obj_curr[
                    column_to_aggregate].count()
            elif column_to_aggregate not in df_grouped_curr.columns:
                errors.append({'error_type': 'COLUMN_NOT_FOUND',
                               'message': f"집계 대상 컬럼 '{column_to_aggregate}' 없음"});
                return errors
            else:
                numeric_col_curr = pd.to_numeric(df_grouped_curr[column_to_aggregate], errors='coerce').fillna(0);
                df_grouped_curr['__agg_target__'] = numeric_col_curr;
                grouped_obj_curr_agg = df_grouped_curr.groupby(
                    group_by_columns, observed=True, dropna=False);
                agg_series_curr = grouped_obj_curr_agg[
                    '__agg_target__'].sum() if aggregate_function == 'SUM' else grouped_obj_curr_agg[
                    '__agg_target__'].mean().fillna(0.0)
            if agg_series_curr is not None: current_period_aggregates_map = {
                (k if isinstance(k, tuple) else (k,)): float(v) for k, v in agg_series_curr.items()}
        else:
            val_curr = 0.0
            if aggregate_function == 'COUNT':
                val_curr = float(len(current_day_or_month_df_filtered) if column_to_aggregate in ['*', '1'] else
                                 current_day_or_month_df_filtered[column_to_aggregate].count())
            elif column_to_aggregate not in current_day_or_month_df_filtered.columns:
                errors.append({'error_type': 'COLUMN_NOT_FOUND',
                               'message': f"집계 대상 컬럼 '{column_to_aggregate}' 없음"});
                return errors
            else:
                num_col_curr_no_grp = pd.to_numeric(current_day_or_month_df_filtered[column_to_aggregate],
                                                    errors='coerce').fillna(0);
                val_curr = float(
                    num_col_curr_no_grp.sum()) if aggregate_function == 'SUM' else (
                    float(num_col_curr_no_grp.mean()) if not num_col_curr_no_grp.empty else 0.0)
            current_period_aggregates_map['__overall__'] = val_curr
    elif not group_by_columns:
        current_period_aggregates_map['__overall__'] = 0.0

    # 과거 데이터 조회 기간 설정
    if period_unit == 'months':
        end_date_for_history = (latest_date_in_current_df.replace(day=1) - relativedelta(months=1))  # 전월의 1일
        start_date_for_history = (end_date_for_history - relativedelta(
            months=historical_lookback_periods - 1))  # lookback 기간만큼 이전 월의 1일
    else:  # days
        end_date_for_history = latest_date_in_current_df - timedelta(days=1)
        start_date_for_history = end_date_for_history - timedelta(days=historical_lookback_periods - 1)

    historical_filter_with_date = f"{date_column_for_trend} BETWEEN '{start_date_for_history.strftime(strftime_format_for_filter)}' AND '{end_date_for_history.strftime(strftime_format_for_filter)}'"
    historical_filter_with_date = f"({historical_filter_with_date}) AND ({historical_base_filter})" if historical_base_filter and historical_base_filter != "1=1" else historical_filter_with_date

    gb_cols_for_hist_query_list = [date_column_for_trend] + (group_by_columns if group_by_columns else []);
    actual_agg_column_hist = '*' if aggregate_function.upper() == 'COUNT' and column_to_aggregate in ['*',
                                                                                                      '1'] else column_to_aggregate;
    agg_col_not_null_filter = f"AND {actual_agg_column_hist} IS NOT NULL" if aggregate_function.upper() != 'COUNT' or (
            aggregate_function.upper() == 'COUNT' and actual_agg_column_hist not in ['*', '1']) else ""
    historical_query = f"SELECT {', '.join(gb_cols_for_hist_query_list)}, {aggregate_function}({actual_agg_column_hist}) as agg_value FROM {historical_data_table} WHERE {historical_filter_with_date} {agg_col_not_null_filter} GROUP BY {', '.join(gb_cols_for_hist_query_list)} ORDER BY {', '.join(gb_cols_for_hist_query_list)}"
    try:
        historical_trend_data_df = q_processor.fetch_to_pandas(query=historical_query,
                                                               engine=engine);
        historical_trend_data_df = pd.DataFrame() if historical_trend_data_df is None or historical_trend_data_df.empty else historical_trend_data_df;
        [
            logger.info(
                f"정보: 규칙 '{params.get('rule_name', 'N/A')}' - 과거 DB 데이터 없음.")] if historical_trend_data_df.empty else None
    except Exception as e_hist_fetch:
        errors.append({'rule_type': 'consecutive_trend_check', 'error_type': 'DB_HISTORY_FETCH_ERROR',
                       'message': f"과거 추세 데이터 조회 실패: {e_hist_fetch}"});
        return errors

    if not historical_trend_data_df.empty:
        try:
            if date_column_format == 'YYYYMM':  # YYYYMM이면 월의 1일로 변환
                historical_trend_data_df[date_column_for_trend] = pd.to_datetime(
                    historical_trend_data_df[date_column_for_trend].astype(str) + '01', format='%Y%m%d',
                    errors='coerce')
            else:  # YYYYMMDD
                historical_trend_data_df[date_column_for_trend] = pd.to_datetime(
                    historical_trend_data_df[date_column_for_trend].astype(str), format='%Y%m%d', errors='coerce')
            historical_trend_data_df.dropna(subset=[date_column_for_trend], inplace=True)
            if group_by_columns:
                for gb_col in group_by_columns:
                    if gb_col in historical_trend_data_df.columns:
                        historical_trend_data_df[gb_col] = historical_trend_data_df[gb_col].fillna(
                            '__NONE_GROUP_KEY__').astype(str)
            historical_trend_data_df['agg_value'] = pd.to_numeric(historical_trend_data_df['agg_value'],
                                                                  errors='coerce').fillna(0.0)
        except Exception as e_hist_conv:
            errors.append({'rule_type': 'consecutive_trend_check', 'error_type': 'DATA_PROCESSING_ERROR',
                           'message': f"과거 데이터 변환 중 오류: {e_hist_conv}"});
            return errors

    all_relevant_groups = set(current_period_aggregates_map.keys())
    if group_by_columns and not historical_trend_data_df.empty:
        all_relevant_groups.update(set(tuple(row[g] for g in group_by_columns) for _, row in
                                       historical_trend_data_df[group_by_columns].drop_duplicates().iterrows()))
    elif not group_by_columns:
        all_relevant_groups.add('__overall__')

    for group_key_tuple_or_overall in all_relevant_groups:
        latest_agg_value = current_period_aggregates_map.get(group_key_tuple_or_overall, 0.0);
        group_hist_df = pd.DataFrame()
        if not historical_trend_data_df.empty:
            if group_by_columns and group_key_tuple_or_overall != '__overall__':
                if not (isinstance(group_key_tuple_or_overall, tuple) and len(group_key_tuple_or_overall) == len(
                        group_by_columns) and all(
                    g in historical_trend_data_df.columns for g in group_by_columns)): continue
                group_filter = pd.Series([True] * len(historical_trend_data_df))
                for i, col_n_g in enumerate(group_by_columns): group_filter = group_filter & (
                        historical_trend_data_df[col_n_g] == group_key_tuple_or_overall[i])
                group_hist_df = historical_trend_data_df[group_filter]
            elif not group_by_columns and group_key_tuple_or_overall == '__overall__':
                group_hist_df = historical_trend_data_df

        latest_row_dict = {date_column_for_trend: latest_date_in_current_df, 'agg_value': latest_agg_value}
        if group_by_columns and group_key_tuple_or_overall != '__overall__':
            for i, col_n_g in enumerate(group_by_columns): latest_row_dict[col_n_g] = group_key_tuple_or_overall[i]

        combined_series_df = pd.concat([group_hist_df, pd.DataFrame([latest_row_dict])],
                                       ignore_index=True) if not group_hist_df.empty else pd.DataFrame(
            [latest_row_dict]);
        time_series = combined_series_df.sort_values(by=date_column_for_trend)['agg_value'].tolist()
        if len(time_series) < consecutive_periods: continue

        consecutive_count = 0;
        actual_trend_start_idx_in_ts = -1
        for k in range(len(time_series) - 1, 0, -1):
            current_val_ts, previous_val_ts = time_series[k], time_series[k - 1];
            trend_match = (trend_type == 'down' and current_val_ts < previous_val_ts) or (
                    trend_type == 'up' and current_val_ts > previous_val_ts)
            if trend_match:
                if consecutive_count == 0: actual_trend_start_idx_in_ts = k
                consecutive_count += 1
                if consecutive_count >= (consecutive_periods - 1):
                    trend_start_idx_slice = actual_trend_start_idx_in_ts - consecutive_count;
                    trend_vals_slice = time_series[trend_start_idx_slice: actual_trend_start_idx_in_ts + 1];
                    trend_dates_dt_obj = combined_series_df.sort_values(by=date_column_for_trend).iloc[
                                         trend_start_idx_slice: actual_trend_start_idx_in_ts + 1][date_column_for_trend]
                    trend_dates_slice = trend_dates_dt_obj.dt.strftime(
                        strftime_format_for_output).tolist()  # MODIFIED: 출력 형식 사용
                    msg_fmt = {'column_to_aggregate': column_to_aggregate, 'aggregate_function': aggregate_function,
                               'consecutive_periods_detected': consecutive_count + 1, 'period_unit': period_unit,
                               'trend_type': trend_type, 'trend_values': str(trend_vals_slice),
                               'trend_dates_first': trend_dates_slice[0] if trend_dates_slice else '',
                               'trend_dates_last': trend_dates_slice[-1] if trend_dates_slice else '',
                               'latest_value': trend_vals_slice[-1] if trend_vals_slice else 0.0}
                    base_msg_tmpl = params.get('message',
                                               "컬럼 '{column_to_aggregate}'의 {aggregate_function} 값이 {consecutive_periods_detected}{period_unit_label} 연속 {trend_type} 추세. 최근값: {latest_value}, 기간: {trend_dates_first} ~ {trend_dates_last}, 값: {trend_values}")
                    msg_fmt['period_unit_label'] = "개월" if period_unit == "months" else "일"  # 메시지용 레이블 추가

                    err_dtl = {'column': column_to_aggregate, 'function': aggregate_function, 'trend_type': trend_type,
                               'consecutive_periods_detected': consecutive_count + 1, 'trend_values': trend_vals_slice,
                               'trend_dates': trend_dates_slice, 'error_type': 'CONSECUTIVE_TREND_DETECTED'}
                    if group_by_columns and group_key_tuple_or_overall != '__overall__':
                        err_dtl['group_key'] = str(group_key_tuple_or_overall);
                        msg_fmt['group_key_str'] = str(
                            group_key_tuple_or_overall);
                        err_dtl['message'] = (
                                f"그룹 {str(group_key_tuple_or_overall)}의 " + base_msg_tmpl.format(
                            **msg_fmt)) if "{group_key_str}" not in base_msg_tmpl else base_msg_tmpl.format(
                            **msg_fmt)
                    else:
                        err_dtl['message'] = base_msg_tmpl.format(**msg_fmt)
                    errors.append(err_dtl);
                    break
            else:
                consecutive_count = 0;
                actual_trend_start_idx_in_ts = -1
    return errors


def check_conditional(df, params, disable_tqdm=True):
    """
    조건부 규칙을 검증합니다. if_condition이 True일 때, then_condition도 True여야 합니다.
    """
    errors = []
    if_condition = params.get('if_condition')
    then_condition = params.get('then_condition')
    expected_outcome = params.get('expected_outcome', True)

    if not all([if_condition, then_condition]):
        errors.append({'error_type': 'CONFIG_ERROR', 'message': "'if_condition'과 'then_condition' 파라미터는 필수입니다."})
        return errors

    msg_template = params.get('message', "조건부 규칙 위반: '{if_cond}' 조건은 만족하지만, '{then_cond}' 조건은 만족하지 않습니다.")

    try:
        # if_condition을 만족하는 모든 행을 찾습니다.
        if_true_df = df.query(if_condition)

        if not if_true_df.empty:
            # 그 중에서 then_condition을 만족하지 못하는 행을 찾습니다.
            violating_df = if_true_df.query(f"not ({then_condition})")

            # expected_outcome이 False라면, then_condition을 만족하는 행이 오류입니다.
            if not expected_outcome:
                violating_df = if_true_df.query(then_condition)

            for idx, row_series in violating_df.iterrows():
                errors.append({
                    'row_index': idx,
                    'error_type': 'CONDITIONAL_CHECK_VIOLATION',
                    'rule_type': 'conditional_check',
                    'message': msg_template.format(if_cond=if_condition, then_cond=then_condition),
                    'error_row_data': row_series.to_dict()
                })

    except Exception as e:
        errors.append({'error_type': 'QUERY_EXECUTION_ERROR', 'message': f"조건부 규칙 실행 중 오류 발생: {e}"})

    return errors


class DataValidator:
    def __init__(self, rules_config, q_processor=None):
        self.rules_config = rules_config;
        self.q_processor = q_processor
        self.validation_functions = {
            'not_null': check_not_null, 'regex_pattern': check_regex_pattern,
            'allowed_values': check_allowed_values, 'numeric_range': check_numeric_range,
            'distribution_change': check_distribution_change,
            'numeric_volatility': check_numeric_volatility,
            'column_equality': check_column_equality, 'duplicate_rows': check_duplicate_rows,
            'aggregate_value_trend': check_aggregate_value_trend,
            'total_row_count_trend': check_total_row_count_trend,
            'schema_change_check': check_schema_change,
            'consecutive_trend_check': check_consecutive_trend,
            'conditional_check': check_conditional
        }
        self.table_level_rule_types = ['column_equality', 'duplicate_rows', 'aggregate_value_trend',
                                       'total_row_count_trend', 'schema_change_check', 'consecutive_trend_check',
                                       'conditional_check']

    def validate(self, df, disable_outer_tqdm=False, disable_inner_tqdm=True):
        all_errors = []
        rule_execution_summary = []
        df_name_for_summary = getattr(df, 'attrs', {}).get('name', '')

        column_rules = self.rules_config.get('columns', {})
        for col_name, rules in tqdm(column_rules.items(), desc="컬럼별 검증 진행", unit="컬럼",
                                    disable=disable_outer_tqdm or not column_rules):

            if col_name not in df.columns:
                for rule_idx, rule_def in enumerate(rules):
                    rule_type_def = rule_def.get('type', 'N/A')
                    rule_name_def = rule_def.get('name', f'{col_name}_{rule_type_def}_{rule_idx}')
                    severity = rule_def.get('severity', 'minor').lower()
                    all_errors.append(
                        {'column': col_name, 'rule_name': rule_name_def, 'rule_type': rule_type_def,
                         'error_type': 'COLUMN_NOT_FOUND',
                         'message': f"컬럼 '{col_name}' 없음", 'severity': severity})
                    rule_execution_summary.append(
                        {'rule_name': rule_name_def, 'rule_type': rule_type_def, 'target_column': col_name,
                         'target_table': df_name_for_summary,
                         'current_data_filter_applied': rule_def.get('params', {}).get('current_data_filter', ''),
                         'items_checked': 0, 'items_passed': 0, 'items_failed': 1, 'status': 'Error (Config)',
                         'rule_severity': severity}
                    )
                continue

            for rule_idx, rule in enumerate(rules):
                severity = rule.get('severity', 'minor').lower()
                rule_type, params = rule['type'], rule.get('params', {}).copy()
                params['rule_name'] = rule.get('name', f'{col_name}_{rule_type}_{rule_idx}')
                current_filter_applied_str = params.get('current_data_filter', '')
                df_for_this_rule = df
                if current_filter_applied_str:
                    try:
                        df_for_this_rule = df.query(current_filter_applied_str)
                        if df_for_this_rule.empty and not df.empty:
                            logger.info(f"경고: 규칙 '{params['rule_name']}' 필터 적용 결과 데이터 없음.")
                    except Exception as e:
                        all_errors.append(
                            {'column': col_name, 'rule_type': rule_type, 'rule_name': params['rule_name'],
                             'error_type': 'CONFIG_ERROR',
                             'message': f"필터 실행 오류: {e}", 'severity': severity}
                        )
                        rule_execution_summary.append(
                            {'rule_name': params['rule_name'], 'rule_type': rule_type, 'target_column': col_name,
                             'target_table': df_name_for_summary,
                             'current_data_filter_applied': current_filter_applied_str, 'items_checked': 0,
                             'items_passed': 0, 'items_failed': 1, 'status': 'Error (Filter)',
                             'rule_severity': severity}
                        )
                        continue
                items_checked = 0
                series_for_check = df_for_this_rule.get(col_name)
                status_for_summary = 'Passed'
                if series_for_check is None and rule_type not in self.table_level_rule_types:
                    all_errors.append({'column': col_name, 'rule_name': params['rule_name'], 'rule_type': rule_type,
                                       'error_type': 'COLUMN_NOT_FOUND_AFTER_FILTER',
                                       'message': f"필터 후 컬럼 '{col_name}' 없음", 'severity': severity})
                    status_for_summary = 'Error (Config)'
                elif series_for_check is not None:
                    if rule_type == 'not_null':
                        items_checked = len(series_for_check)
                    elif rule_type in ['regex_pattern', 'allowed_values']:
                        items_checked = len(series_for_check.dropna())
                    elif rule_type == 'numeric_range':
                        items_checked = len(series_for_check)
                    elif rule_type == 'numeric_volatility':
                        items_checked = len(pd.to_numeric(series_for_check, errors='coerce').dropna())
                    elif rule_type == 'distribution_change':
                        items_checked = 1
                current_rule_errors = []
                if rule_type in self.validation_functions and rule_type not in self.table_level_rule_types:
                    if series_for_check is not None:
                        func_args = [series_for_check, col_name, params];
                        func_kwargs = {'disable_tqdm': disable_inner_tqdm} if rule_type in ['not_null', 'regex_pattern',
                                                                                            'allowed_values',
                                                                                            'numeric_range'] else {}
                        if rule_type == 'numeric_volatility':
                            func_kwargs.update({'q_processor': self.q_processor, 'full_current_df': df_for_this_rule})
                        elif rule_type == 'distribution_change':
                            func_kwargs['q_processor'] = self.q_processor
                        current_rule_errors = self.validation_functions[rule_type](*func_args, **func_kwargs)
                elif rule_type not in self.table_level_rule_types:
                    all_errors.append({'column': col_name, 'rule_type': rule_type, 'rule_name': params['rule_name'],
                                       'error_type': 'UNKNOWN_RULE_TYPE',
                                       'message': f"알 수 없는 컬럼 규칙: {rule_type}", 'severity': severity});
                    status_for_summary = 'Error (Unknown Rule)'
                items_failed = (1 if current_rule_errors else 0) if rule_type == 'distribution_change' else (
                    len(current_rule_errors) if current_rule_errors is not None else 1)
                if current_rule_errors is None: current_rule_errors = [
                    {'column': col_name, 'rule_name': params['rule_name'], 'rule_type': rule_type,
                     'error_type': 'INTERNAL_ERROR',
                     'message': f"검증 함수 {rule_type}가 None 반환"}]; status_for_summary = 'Error (Internal)'
                items_passed = items_checked - items_failed if items_checked >= items_failed else 0
                if status_for_summary == 'Passed' and items_failed > 0: status_for_summary = 'Failed'
                if items_checked == 0 and status_for_summary == 'Passed': status_for_summary = 'Skipped (Filter Empty)' if current_filter_applied_str and df_for_this_rule.empty and not df.empty else (
                    'Skipped (No Data in Series)' if series_for_check is not None and series_for_check.empty else status_for_summary)
                rule_execution_summary.append(
                    {'rule_name': params['rule_name'], 'rule_type': rule_type, 'target_column': col_name,
                     'target_table': df_name_for_summary, 'current_data_filter_applied': current_filter_applied_str,
                     'items_checked': items_checked, 'items_passed': items_passed, 'items_failed': items_failed,
                     'status': status_for_summary, 'rule_severity': severity})
                if current_rule_errors:
                    for err in current_rule_errors:
                        err.update({'rule_type': rule_type, 'rule_name': params['rule_name'], 'column': col_name,
                                    'severity': severity})
                        if current_filter_applied_str: err['current_data_filter_applied'] = current_filter_applied_str
                        if 'row_index' in err and err[
                            'row_index'] in df_for_this_rule.index and 'error_row_data' not in err:
                            try:
                                err['error_row_data'] = df_for_this_rule.loc[err['row_index']].to_dict()
                            except KeyError:
                                logger.info(f"경고: 오류 행 데이터 조회 실패 (필터: {current_filter_applied_str}, 인덱스: {err['row_index']})")
                    all_errors.extend(current_rule_errors)

        table_rules = self.rules_config.get('table_level_rules', [])
        for rule_idx, rule in tqdm(enumerate(table_rules), desc="테이블 레벨 검증 진행", unit="룰",
                                   disable=disable_outer_tqdm or not table_rules):
            rule_type, params = rule['type'], rule.get('params', {}).copy()
            params['rule_name'] = rule.get('name', f'table_{rule_type}_{rule_idx}')
            severity = rule.get('severity', 'minor').lower()

            current_filter_applied_table_str = params.get('current_data_filter', '')
            df_for_this_rule_table = df
            status_for_summary_table = 'Passed'
            target_table_for_this_rule = params.get('table_name_in_db', df_name_for_summary)
            if current_filter_applied_table_str and rule_type not in ['schema_change_check']:
                try:
                    df_for_this_rule_table = df.query(current_filter_applied_table_str)
                    if df_for_this_rule_table.empty and not df.empty:
                        logger.info(f"경고: 규칙 '{params['rule_name']}' 필터 적용 결과 데이터 없음.")
                except Exception as e:
                    all_errors.append(
                        {'rule_type': rule_type, 'rule_name': params['rule_name'], 'error_type': 'CONFIG_ERROR',
                         'message': f"필터 실행 오류: {e}", 'severity': severity})
                    rule_execution_summary.append(
                        {'rule_name': params['rule_name'], 'rule_type': rule_type, 'target_column': None,
                         'target_table': target_table_for_this_rule,
                         'current_data_filter_applied': current_filter_applied_table_str, 'items_checked': 0,
                         'items_passed': 0, 'items_failed': 1, 'status': 'Error (Filter)', 'rule_severity': severity}
                    )
                    continue
            items_checked_table = 0
            if rule_type == 'schema_change_check':
                items_checked_table = 1
            elif rule_type == 'consecutive_trend_check':
                gb_cols = params.get('group_by_columns')
                items_checked_table = (
                    df_for_this_rule_table.groupby(gb_cols, observed=True,
                                                   dropna=False).ngroups if not df_for_this_rule_table.empty and gb_cols and all(
                        col in df_for_this_rule_table.columns for col in gb_cols) else 0
                ) if gb_cols else 1
            elif rule_type == 'conditional_check':
                if_condition = params.get('if_condition', '1==0')
                try:
                    items_checked_table = len(df_for_this_rule_table.query(if_condition))
                except Exception:
                    items_checked_table = 0
            elif not df_for_this_rule_table.empty or rule_type in ['total_row_count_trend', 'aggregate_value_trend']:
                items_checked_table = len(df_for_this_rule_table) if rule_type in ['column_equality',
                                                                                   'duplicate_rows'] else (
                    1 if rule_type in ['aggregate_value_trend', 'total_row_count_trend'] else 0)

            current_rule_errors = []
            if rule_type in self.validation_functions and rule_type in self.table_level_rule_types:
                func_args_table = [df_for_this_rule_table, params] if rule_type not in ['schema_change_check'] else [
                    None, params]
                func_kwargs_table = {'q_processor': self.q_processor} if rule_type in ['aggregate_value_trend',
                                                                                       'total_row_count_trend',
                                                                                       'consecutive_trend_check',
                                                                                       'schema_change_check'] else (
                    {'disable_tqdm': disable_inner_tqdm} if rule_type in ['column_equality', 'duplicate_rows',
                                                                          'conditional_check'] else {})

                # df가 비어있어도 실행해야 하는 규칙들 예외 처리
                if not (df_for_this_rule_table.empty and rule_type not in ['total_row_count_trend',
                                                                           'aggregate_value_trend',
                                                                           'consecutive_trend_check',
                                                                           'schema_change_check']):
                    current_rule_errors = self.validation_functions[rule_type](*func_args_table, **func_kwargs_table)
                else:
                    current_rule_errors = []

            elif rule_type in self.table_level_rule_types:
                all_errors.append(
                    {'rule_type': rule_type, 'rule_name': params['rule_name'], 'error_type': 'UNKNOWN_RULE_TYPE',
                     'message': f"알 수 없는 테이블 규칙: {rule_type}", 'severity': severity})
                status_for_summary_table = 'Error (Unknown Rule)'

            items_failed_table = len(current_rule_errors) if current_rule_errors is not None else 1
            if current_rule_errors is None:
                current_rule_errors = [
                    {'rule_name': params['rule_name'], 'rule_type': rule_type, 'error_type': 'INTERNAL_ERROR',
                     'message': f"검증 함수 {rule_type}가 None 반환"}]
                status_for_summary_table = 'Error (Internal)'

            items_passed_table = items_checked_table - items_failed_table if items_checked_table >= items_failed_table else 0
            if status_for_summary_table == 'Passed' and items_failed_table > 0: status_for_summary_table = 'Failed'
            if items_checked_table == 0 and status_for_summary_table == 'Passed': status_for_summary_table = 'Skipped (Filter Empty)' if current_filter_applied_table_str and df_for_this_rule_table.empty and not df.empty else (
                'Skipped (No Data)' if df_for_this_rule_table.empty and not current_filter_applied_table_str else status_for_summary_table)

            summary_item_details = {'rule_name': params['rule_name'], 'rule_type': rule_type, 'target_column': None,
                                    'target_table': target_table_for_this_rule,
                                    'current_data_filter_applied': current_filter_applied_table_str if rule_type != 'schema_change_check' else '',
                                    'items_checked': items_checked_table, 'items_passed': items_passed_table,
                                    'items_failed': items_failed_table, 'status': status_for_summary_table,
                                    'rule_severity': severity}

            if rule_type == 'schema_change_check' and 'table_name_in_db' in params:
                summary_item_details['target_table'] = params['table_name_in_db']

            rule_execution_summary.append(summary_item_details)
            if current_rule_errors:
                for err in current_rule_errors:
                    err.update({'rule_type': rule_type, 'rule_name': params['rule_name'], 'severity': severity})
                    if current_filter_applied_table_str and rule_type != 'schema_change_check':
                        err['current_data_filter_applied'] = current_filter_applied_table_str
                all_errors.extend(current_rule_errors)
        return all_errors, rule_execution_summary

def run_data_validation(dataframe, rules_config, query_processor_instance=None,
                            log_to_console=True,
                            max_errors_to_log=100,
                            disable_outer_tqdm=False, disable_inner_tqdm=True,
                            save_to_hive=False,
                            hive_db_name=None,
                            hive_validation_runs_table_name=None,
                            hive_summary_table_name=None,
                            hive_errors_table_name=None,
                            hive_partition_value=None,
                            hive_partition_column_name='dt',
                            hive_save_mode_is_overwrite=True,
                            validation_run_info_extra=None,
                            output_report_path_if_db_fail=None,
                            frst_rgr_id='data_validator_script',
                            last_updtr_id='data_validator_script'
                            ):
        if dataframe is None:
            logger.info("오류: 검증할 DataFrame이 제공되지 않았습니다.")
            return [{"error_type": "CONFIG_ERROR", "message": "검증 대상 DataFrame이 누락되었습니다."}], []

        start_time_total_run = time.time()

        if query_processor_instance is None:
            query_processor_instance = DatalabQueryProcessor()

        validator = DataValidator(rules_config, query_processor_instance)

        logger.info(f"\n--- 데이터 검증 실행 (DataFrame 크기: {dataframe.shape}) ---")
        all_errors, rule_execution_summary = validator.validate(dataframe, disable_outer_tqdm=disable_outer_tqdm,
                                                                disable_inner_tqdm=disable_inner_tqdm)
        actual_execution_time_seconds = time.time() - start_time_total_run

        all_errors = all_errors if all_errors is not None else []
        rule_execution_summary = rule_execution_summary if rule_execution_summary is not None else []

        logger.info(f"--- 데이터 검증 실행 완료 (오류 수: {len(all_errors)}) ---")

        severity_counts = {'critical': {'failed_rules': 0, 'errors': 0},
                           'major': {'failed_rules': 0, 'errors': 0},
                           'minor': {'failed_rules': 0, 'errors': 0}}

        for summary_item in rule_execution_summary:
            if summary_item.get('status') == 'Failed':
                severity = summary_item.get('rule_severity', 'minor')
                if severity in severity_counts:
                    severity_counts[severity]['failed_rules'] += 1

        for error_item in all_errors:
            severity = error_item.get('severity', 'minor')
            if severity in severity_counts:
                severity_counts[severity]['errors'] += 1
        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★

        if log_to_console and rule_execution_summary:
            logger.info("\n--- 검증 실행 요약 보고서 ---")
            try:
                summary_df_log = pd.DataFrame(rule_execution_summary)
                # severity 컬럼 추가
                summary_cols_log = ['rule_name', 'rule_type', 'rule_severity', 'target_column', 'target_table',
                                    'current_data_filter_applied', 'status', 'items_checked', 'items_passed',
                                    'items_failed']
                display_cols_log = [col for col in summary_cols_log if col in summary_df_log.columns]
                if not summary_df_log.empty and display_cols_log:
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', 1000)
                    logger.info(summary_df_log[display_cols_log].to_string(index=False))
                else:
                    logger.info("실행된 규칙에 대한 요약 정보가 없습니다.")
            except Exception as e_summary_log:
                logger.info(f"요약 보고서 콘솔 출력 중 오류 발생: {e_summary_log}")

        total_errors_found = len(all_errors)
        errors_to_process = all_errors
        log_summary_message_detailed = f"\n🚨 총 {total_errors_found}개의 상세 검증 오류 발견"

        if total_errors_found == 0 and log_to_console:
            logger.info("\n✅ 모든 검증 규칙을 통과했습니다!")
        elif total_errors_found > 0:
            if max_errors_to_log is not None and 0 <= max_errors_to_log < total_errors_found:
                errors_to_process = all_errors[:max_errors_to_log]
                log_summary_message_detailed += f" (상위 {max_errors_to_log}개만 표시 및 저장 대상):"
            else:
                log_summary_message_detailed += ":"
            if log_to_console:
                logger.info(log_summary_message_detailed)
                for i, error in enumerate(errors_to_process):
                    logger.info(f"\n--- 오류 {i + 1} ---")
                    for k, v in error.items():
                        logger.info(
                            f"  {k}: {json.dumps(v, ensure_ascii=False, indent=2, default=str) if isinstance(v, (dict, list)) else v}")

        db_save_successful = False
        if save_to_hive:
            if not all([hive_db_name, hive_validation_runs_table_name, hive_summary_table_name, hive_errors_table_name,
                        hive_partition_value]):
                logger.info("경고: Hive 저장 필수 파라미터 누락. DB 저장 건너뜁니다.")
            elif not hasattr(query_processor_instance, 'save_pandas_to_datalake'):
                logger.info("경고: QueryProcessor에 save_pandas_to_datalake 없음. DB 저장 건너뜁니다.")
            else:
                try:
                    current_run_id = f"run_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                    df_name_attr = getattr(dataframe, 'attrs', {}).get('name', '')
                    table_name_from_rules = next((r.get('params', {}).get('table_name_in_db', '') for r in
                                                  rules_config.get('table_level_rules', []) if
                                                  r.get('params', {}).get('table_name_in_db')), '')
                    target_data_source_name_val = df_name_attr or table_name_from_rules or "UnknownTable"

                    run_info_data = {
                        'validation_run_id': current_run_id,
                        'validation_timestamp': datetime.now(),
                        'target_data_source_name': str(target_data_source_name_val or ''),
                        'target_data_reference_date': str(hive_partition_value or ''),
                        'rules_config_identifier': str(rules_config.get('version', '')),
                        'overall_status': 'Failed' if total_errors_found > 0 else 'Passed',
                        'total_rules_executed': len(rule_execution_summary),
                        'total_rules_failed': sum(1 for r in rule_execution_summary if r['status'] == 'Failed'),
                        'total_errors_found': total_errors_found,
                        'critical_rules_failed': severity_counts['critical']['failed_rules'],
                        'major_rules_failed': severity_counts['major']['failed_rules'],
                        'minor_rules_failed': severity_counts['minor']['failed_rules'],
                        'critical_errors_found': severity_counts['critical']['errors'],
                        'major_errors_found': severity_counts['major']['errors'],
                        'minor_errors_found': severity_counts['minor']['errors'],
                        'execution_time_seconds': actual_execution_time_seconds,
                        'run_log_path': f'log_dir/{current_run_id}.log',
                        **(validation_run_info_extra or {})
                    }

                    run_info_df_to_save = pd.DataFrame([run_info_data])
                    run_info_df_to_save[hive_partition_column_name] = str(hive_partition_value or '')
                    for col in run_info_df_to_save.select_dtypes(include=['object']).columns:
                        run_info_df_to_save[col] = run_info_df_to_save[col].fillna('').astype(str)
                    if 'execution_time_seconds' in run_info_df_to_save.columns:
                        run_info_df_to_save['execution_time_seconds'] = run_info_df_to_save[
                            'execution_time_seconds'].astype(float)
                    for col in ['total_rules_executed', 'total_rules_failed', 'total_errors_found',
                                'critical_rules_failed', 'major_rules_failed', 'minor_rules_failed',
                                'critical_errors_found', 'major_errors_found', 'minor_errors_found']:
                        if col in run_info_df_to_save.columns:
                            run_info_df_to_save[col] = run_info_df_to_save[col].astype(np.int64)

                    query_processor_instance.save_pandas_to_datalake(
                        run_info_df_to_save, db_name=hive_db_name,
                        table_name=hive_validation_runs_table_name,
                        partition_column=hive_partition_column_name,
                        overwrite_tf=hive_save_mode_is_overwrite
                    )

                    if rule_execution_summary:
                        summary_df_to_save = pd.DataFrame(rule_execution_summary)
                        summary_df_to_save['validation_run_id'] = current_run_id
                        summary_df_to_save[hive_partition_column_name] = str(hive_partition_value or '')
                        summary_df_to_save['summary_entry_id'] = [f"sum_{current_run_id}_{i}" for i in
                                                                  range(len(summary_df_to_save))]

                        summary_cols_map = {
                            'summary_entry_id': ('string', ''), 'validation_run_id': ('string', ''),
                            'rule_name': ('string', ''), 'rule_type': ('string', ''),
                            'rule_severity': ('string', 'minor'),
                            'target_column': ('string', ''), 'target_table': ('string', ''),
                            'current_data_filter_applied': ('string', ''), 'status': ('string', ''),
                            'items_checked': ('int64', 0), 'items_passed': ('int64', 0),
                            'items_failed': ('int64', 0), hive_partition_column_name: ('string', '')
                        }

                        for col, (dtype, default) in summary_cols_map.items():
                            if col not in summary_df_to_save.columns: summary_df_to_save[col] = default
                            current_col_s = summary_df_to_save[col]
                            if dtype == 'string':
                                summary_df_to_save[col] = current_col_s.fillna(default).astype(str)
                            elif dtype == 'int64':
                                summary_df_to_save[col] = pd.to_numeric(current_col_s, errors='coerce').fillna(
                                    default).astype(np.int64)
                        summary_df_to_save = summary_df_to_save[list(summary_cols_map.keys())]
                        query_processor_instance.save_pandas_to_datalake(summary_df_to_save, db_name=hive_db_name,
                                                                         table_name=hive_summary_table_name,
                                                                         partition_column=hive_partition_column_name,
                                                                         overwrite_tf=hive_save_mode_is_overwrite)

                    if errors_to_process:
                        processed_errors_for_df = []
                        for i, err_obj in enumerate(errors_to_process):
                            rec = {
                                'error_id': f"err_{current_run_id}_{i}",
                                'validation_run_id': current_run_id,
                                'rule_name': str(err_obj.get('rule_name', '')),
                                'rule_severity': str(err_obj.get('severity', 'minor')),
                                'rule_type': str(err_obj.get('rule_type', '')),
                                'error_type': str(err_obj.get('error_type', '')),
                                'target_table': str(
                                    err_obj.get('table_name', getattr(dataframe, 'attrs', {}).get('name', ''))),
                                # 명세서에 ERR_TBL_NM
                                'target_column': str(err_obj.get('column', '')),  # 명세서에 ERR_CLMN_NM
                                'row_identifier': str(err_obj.get('row_index', '')) if pd.notna(
                                    err_obj.get('row_index')) else '',  # 명세서에 ERR_ROW_IDFR_VL
                                'error_message': str(err_obj.get('message', '')),  # 명세서에 ERR_MSG_CTT
                            }
                            # error_value_details: 나머지 상세 정보를 JSON으로 저장 (텍스트 명세서 기준)
                            err_value_content_payload = {}
                            keys_for_details = ['value', 'group_key', 'details', 'ratio', 'new_codes_sample',
                                                'code', 'current_freq', 'historical_freq', 'current_count',
                                                'historical_count', 'change_ratio', 'current_value',
                                                'historical_value', 'historical_period_label', 'change_type',
                                                'function', 'consecutive_periods_detected', 'trend_values',
                                                'trend_dates',
                                                'columns', 'engine_name']  # engine_name도 여기에 포함 가능

                            for k_detail in keys_for_details:
                                if k_detail in err_obj and pd.notna(err_obj[k_detail]):
                                    err_value_content_payload[k_detail] = err_obj[k_detail]

                            rec['error_value_details'] = json.dumps(err_value_content_payload, ensure_ascii=False,
                                                                    default=str)

                            # error_row_data_json 컬럼
                            rec['error_row_data_json'] = json.dumps(err_obj.get('error_row_data', {}),
                                                                    ensure_ascii=False,
                                                                    default=str)

                            # current_data_filter_applied 컬럼
                            rec['current_data_filter_applied'] = str(err_obj.get('current_data_filter_applied', ''))

                            processed_errors_for_df.append(rec)

                        if processed_errors_for_df:
                            detailed_errors_df_to_save = pd.DataFrame(processed_errors_for_df)
                            detailed_errors_df_to_save[hive_partition_column_name] = str(hive_partition_value or '')

                            # 텍스트로 제공된 명세서 기준 컬럼 정의 (15개)
                            # 모든 컬럼을 STRING으로 가정하고, None은 빈 문자열로 처리
                            error_cols_final_spec = {
                                'error_id': ('string', ''), 'validation_run_id': ('string', ''),
                                'rule_name': ('string', ''), 'rule_type': ('string', ''),
                                'rule_severity': ('string', ''),
                                'error_type': ('string', ''), 'target_column': ('string', ''),
                                'target_table': ('string', ''), 'engine_name': ('string', ''),
                                # engine_name은 ERR_VL_CTT로 이동했으므로 여기선 제외해도 됨 (명세서 기준)
                                'group_key_info': ('string', '{}'), 'row_identifier': ('string', ''),
                                'error_value_details': ('string', '{}'), 'error_message': ('string', ''),
                                'error_row_data_json': ('string', '{}'),
                                'current_data_filter_applied': ('string', ''),
                                hive_partition_column_name: ('string', '')  # dt 컬럼
                            }

                            # engine_name은 ERR_VL_CTT로 보냈으므로, error_cols_final_spec 에서는 제거하거나,
                            # DB 테이블에 실제로 engine_name 컬럼이 있다면 여기에 포함. 텍스트 명세서 기준으로는 15개 컬럼만.
                            # 텍스트 명세서에 engine_name이 있으므로 다시 추가:
                            if 'engine_name' not in error_cols_final_spec:
                                error_cols_final_spec['engine_name'] = ('string', '')

                            final_err_cols_ordered = []
                            for col_name, (col_type_str, default_val) in error_cols_final_spec.items():
                                final_err_cols_ordered.append(col_name)
                                if col_name not in detailed_errors_df_to_save.columns:
                                    detailed_errors_df_to_save[col_name] = default_val

                                # 모든 컬럼을 문자열로 처리 (텍스트 명세서 기준)
                                detailed_errors_df_to_save[col_name] = detailed_errors_df_to_save[col_name].fillna(
                                    default_val).astype(str)

                            # 최종 컬럼 순서 (텍스트 명세서 순서와 일치하도록)
                            # 텍스트 명세서 순서: error_id, validation_run_id, rule_name, rule_type, error_type,
                            # target_column, target_table, engine_name, group_key_info, row_identifier,
                            # error_value_details, error_message, error_row_data_json, current_data_filter_applied, dt
                            final_ordered_cols_from_text_spec = [
                                'error_id', 'validation_run_id', 'rule_name', 'rule_type', 'rule_severity',
                                'error_type',
                                'target_column', 'target_table', 'engine_name', 'group_key_info',
                                'row_identifier', 'error_value_details', 'error_message',
                                'error_row_data_json', 'current_data_filter_applied', hive_partition_column_name
                            ]
                            # DataFrame에 존재하는 컬럼만으로 순서 재구성
                            existing_cols_for_order = [col for col in final_ordered_cols_from_text_spec if
                                                       col in detailed_errors_df_to_save.columns]
                            detailed_errors_df_to_save = detailed_errors_df_to_save[existing_cols_for_order]

                            logger.info(f"INFO: dqm_detailed_errors 테이블에 저장 시도. {len(detailed_errors_df_to_save)} 건")
                            query_processor_instance.save_pandas_to_datalake(
                                detailed_errors_df_to_save,
                                db_name=hive_db_name,
                                table_name=hive_errors_table_name,
                                partition_column=hive_partition_column_name,
                                overwrite_tf=hive_save_mode_is_overwrite
                            )
                            logger.info(f"INFO: dqm_detailed_errors 테이블에 저장 완료.")

                    logger.info(f"INFO: 모든 검증 결과를 DB 테이블에 저장 완료 (run_id: {current_run_id}).")
                    db_save_successful = True
                except Exception as e_db_save:
                    logger.info(f"\n⚠️ 검증 결과를 DB에 저장 중 오류 발생: {e_db_save}")
                    db_save_successful = False

        if not db_save_successful and output_report_path_if_db_fail:
            logger.info(f"INFO: DB 저장 실패/비활성화. 결과를 로컬 파일 '{output_report_path_if_db_fail}'에 저장합니다.")
            try:
                def safe_converter_file(o):
                    if isinstance(o, (datetime, np.datetime64, pd.Timestamp)): return o.isoformat()
                    if isinstance(o, np.integer): return int(o)
                    if isinstance(o, np.floating): return float(o) if not np.isnan(o) else None
                    if isinstance(o, np.ndarray): return o.tolist()
                    if pd.isna(o) and not isinstance(o, (dict, list, tuple, set)): return None
                    try:
                        json.dumps(o);
                        return o
                    except TypeError:
                        return str(o)

                report_content = {
                    "validation_summary_report": [{str(k): safe_converter_file(v) for k, v in item.items()} for item in
                                                  rule_execution_summary],
                    "detailed_errors": [{str(k): safe_converter_file(v) for k, v in item.items()} for item in
                                        errors_to_process]}
                if max_errors_to_log is not None and 0 <= max_errors_to_log < total_errors_found: report_content[
                    "error_log_notice"] = f"총 {total_errors_found} 오류 중 상위 {max_errors_to_log}개만 기록됨."
                with open(output_report_path_if_db_fail, 'w', encoding='utf-8') as f:
                    json.dump(report_content, f, ensure_ascii=False, indent=4, default=str)
                logger.info(f"\n📄 검증 결과가 '{output_report_path_if_db_fail}' 파일에 저장되었습니다.")
            except Exception as e_file_save:
                logger.info(f"\n⚠️ DB 저장 실패 후 파일 저장 중에도 오류 발생: {e_file_save}")

        return all_errors, rule_execution_summary