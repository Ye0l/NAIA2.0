import pandas as pd
import numpy as np
import re
import pyarrow as pa
import pyarrow.compute as pc
from typing import Dict, List, Any, Optional


def _arrow_values(series: pd.Series):
    """Underlying Arrow array of an Arrow-backed Series (``None`` for object/legacy).

    Arrow-backed columns expose their buffers directly, so tag joins and
    substring matching run on the parquet UTF-8 bytes without ever building a
    Python ``str`` per cell.
    """
    values = getattr(series, "array", None)
    return getattr(values, "_pa_array", None)


def _mask_to_numpy(result) -> np.ndarray:
    """Arrow boolean (Array or ChunkedArray) -> numpy bool mask, nulls as False."""
    return pc.fill_null(result, False).to_numpy(zero_copy_only=False)


def _read_parquet_compact(file_path: str) -> pd.DataFrame:
    """Read one archive parquet keeping UTF-8 columns in shared Arrow buffers.

    An object-dtype read allocates a Python ``str`` per cell. Across a
    multi-file archive scan (one full file per worker thread) that transient
    dominates the process peak, and glibc does not hand those small blocks back
    to the OS afterwards, so the resident set stays high long after the scan.
    Arrow strings keep one contiguous buffer per column instead. Falls back to
    the plain pandas reader if the compact path is unavailable.
    """
    try:
        import pyarrow.parquet as pq

        from core.parquet_chunk_loader import compact_string_types_mapper

        table = pq.read_table(file_path)
        # self_destruct releases each Arrow column as it is converted; the table
        # is dropped here either way, so the conversion never holds both.
        return table.to_pandas(
            types_mapper=compact_string_types_mapper(),
            split_blocks=True,
            self_destruct=True,
        )
    except Exception:
        return pd.read_parquet(file_path, engine="pyarrow")


def _iter_parquet_frames(file_path: str, batch_rows: int):
    """Yield one archive parquet as row-batch DataFrames (Arrow-backed strings).

    A worker used to hold a whole bucket file while it filtered it, so the scan's
    peak was ``workers x full file`` no matter how few rows actually matched.
    Streaming row batches makes that ``workers x one batch`` plus the survivors.
    Falls back to a single whole-file frame if the batch reader is unavailable.
    """
    try:
        import pyarrow.parquet as pq

        from core.parquet_chunk_loader import compact_string_types_mapper

        parquet_file = pq.ParquetFile(file_path)
        mapper = compact_string_types_mapper()
    except Exception:
        yield _read_parquet_compact(file_path)
        return

    for batch in parquet_file.iter_batches(batch_size=batch_rows):
        yield batch.to_pandas(types_mapper=mapper)


class SearchEngine:
    """Parquet 파일에서 태그를 검색하는 로직을 수행하는 핵심 엔진"""
    TAG_COLUMNS = ['copyright', 'character', 'artist', 'meta', 'general']
    # Rows per streamed batch. Bounds the transient a worker holds while it
    # filters; small enough that N workers stay flat, large enough that the
    # Arrow match kernels still run on a worthwhile array.
    BATCH_ROWS = 100_000

    def _build_tags_string(self, df: pd.DataFrame) -> pd.Series:
        """태그 컬럼들을 쉼표로 결합한 tags_string 을 행 단위로 생성(벡터화).

        ``Series.str.cat`` 한 번으로 5개 태그 컬럼을 결합한다. 기존 melt/groupby
        구현 대비 ~수배 빠르며(읽기+빌드 150파일 51.7s→13.9s), 검색 결과는 동치
        (동일 ``.str.contains`` 매칭; NaN→'' 치환이라 빈 필드는 어떤 키워드와도
        매칭되지 않음). groupby('index') 를 쓰지 않으므로 비유니크 인덱스에서
        서로 다른 행의 태그가 병합되던 결함도 구조적으로 발생하지 않는다.
        """
        if df.empty:
            return pd.Series(index=df.index, dtype='object')

        first = df[self.TAG_COLUMNS[0]].astype(object)
        rest = [df[col].astype(object) for col in self.TAG_COLUMNS[1:]]
        return first.str.cat(rest, sep=',', na_rep='')

    def _tags_array(self, df: pd.DataFrame):
        """행 단위 ``a,b,c`` 태그 텍스트를 **Arrow 배열로만** 만든다.

        ``binary_join_element_wise`` 한 번(C레벨)으로 5개 컬럼을 잇는다 — 셀당 파이썬
        문자열을 만들지 않으므로 아카이브 스캔의 순간 피크가 태그 텍스트 한 벌(UTF-8
        버퍼)로 끝난다. 이미 Arrow 인 컬럼은 버퍼를 그대로 쓰고, object 컬럼만 한 번
        변환한다(``filter_source_frame`` 처럼 빠진 태그 컬럼을 ``""`` 로 채워 넣는
        경로에서도 나머지 컬럼이 Arrow 면 이득을 잃지 않는다). 변환이 불가능한 프레임은
        예전 ``Series.str.cat`` 경로로 떨어진다. 결과는 어느 쪽이든 동치다
        (결측 → ``''``, 구분자 ``','``).
        """
        try:
            arrow_columns = []
            for name in self.TAG_COLUMNS:
                column = df[name]
                values = _arrow_values(column)
                if values is None:
                    values = pa.array(column, from_pandas=True)
                arrow_columns.append(values)
            # 커널은 모든 인자가 같은 문자열 타입일 때만 잡힌다. object 변환은 ``string``,
            # pandas 의 Arrow 문자열 dtype 은 ``large_string`` 이라 한쪽으로 맞춰 준다.
            target = arrow_columns[0].type
            if any(pa.types.is_large_string(values.type) for values in arrow_columns):
                target = pa.large_string()
            filled = [
                pc.fill_null(values if values.type == target else values.cast(target), "")
                for values in arrow_columns
            ]
            return pc.binary_join_element_wise(*filled, pa.scalar(",", type=target))
        except Exception:
            return pa.array(self._build_tags_string(df), from_pandas=True)

    def _parse_query(self, query: str) -> Dict[str, List[Any]]:
        query = query.strip().replace("_", " ")
        
        # OR 그룹 추출 ({tag1|tag2|tag3} 형태)
        or_groups_raw = re.findall(r'\{([^}]+)\}', query)
        query = re.sub(r'\{[^}]+\}', '', query)

        or_groups = []
        for group in or_groups_raw:
            # | 로 분리된 태그들을 개별 키워드로 처리 (수정됨)
            or_parts = [part.strip() for part in group.split('|') if part.strip()]
            if or_parts:
                or_groups.append(or_parts)

        # 나머지 키워드를 쉼표로 분리
        keywords = [k.strip() for k in query.split(',') if k.strip()]

        # 연산자별로 분리
        parsed = {
            'normal': [k for k in keywords if not k.startswith(('*', '~'))],
            'exact': [k.lstrip('*') for k in keywords if k.startswith('*')],
            'not_exact': [k.lstrip('~') for k in keywords if k.startswith('~')],
        }
        if or_groups:
            parsed['or'] = or_groups
            
        return parsed

    def _apply_filters(self, df: pd.DataFrame, query: str, exclude_query: str) -> pd.DataFrame:
        """파싱된 쿼리에 따라 필터를 적용합니다.

        매칭은 PyArrow compute(`match_substring` / `match_substring_regex`, C레벨·
        **GIL 해제**)로 수행 → ThreadPool 아카이브 스캔에서 실질 병렬화. 모든 마스크를
        하나의 numpy 불리언 배열로 누적해 **DataFrame 슬라이싱은 마지막 1회**만(다열
        복사 반복 제거). 결과는 기존 pandas `.str.contains` 순차 narrowing 과 **동치**
        (정확매칭 lookbehind 정규식은 RE2 의 '경계 소비형' 패턴으로 존재여부 동치 변환;
        OR 그룹의 기존 동작-빈 그룹 덮어쓰기 포함-도 그대로 보존)."""
        if df.empty:
            return df

        search_params = self._parse_query(query)
        exclude_params = self._parse_query(exclude_query)

        # 태그 텍스트는 Arrow 배열로만 만든다. 예전에는 'tags_string' 컬럼을 **호출자의
        # 프레임에 직접 붙였는데**(입력 변형), 그 한 컬럼이 풀 전체 태그 텍스트를 파이썬
        # 문자열로 한 벌 더 들고 있었다(그리고 호출부마다 drop 으로 뒤치다꺼리를 했다).
        # 호출자가 미리 만들어 둔 컬럼이 있으면 그대로 쓴다(기존 계약 보존).
        if 'tags_string' in df.columns:
            tags = _arrow_values(df['tags_string'])
            if tags is None:
                tags = pa.array(df['tags_string'], from_pandas=True)  # NaN -> null (na=False 동치)
        else:
            tags = self._tags_array(df)

        def contains(keyword: str) -> np.ndarray:
            # 부분일치. pandas str.contains(re.escape(kw), regex=True) 와 동치.
            return _mask_to_numpy(pc.match_substring(tags, keyword))

        def contains_exact(keyword: str) -> np.ndarray:
            # 퍼펙트 매칭 = **태그 전체 일치**. 경계는 쉼표뿐이다.
            #
            # ⚠️ 예전 경계는 `[, ]`(쉼표 **또는 공백**)였다. 태그 자체가 공백을 품으므로
            #    (`dog ears`·`hot dog`) `*dog` 이 그것들에 전부 걸렸다 - 이름은 퍼펙트
            #    매칭인데 실제로는 '단어 일치'였다. 실측(196,974행): `*dog` 2,883건 중
            #    맞는 것은 701건뿐이고 나머지 2,182건이 `dog ears`(1,617)·`dog girl`
            #    (1,019)·`dog tail`(889) 류였다(사용자 제보 2026-08-25).
            #
            # 쉼표 주변 공백은 데이터에 섞여 있다(`a, b,c` 둘 다 나온다) - `\s*` 로 흡수한다.
            pattern = r'(^|,)\s*' + re.escape(keyword) + r'\s*(,|$)'
            return _mask_to_numpy(pc.match_substring_regex(tags, pattern))

        def or_term(keyword: str) -> np.ndarray:
            """OR 그룹의 항 하나. `*tag` 는 **그룹 밖과 똑같이** 태그 전체일치로 읽는다.

            ⚠️ 예전엔 리터럴이었다 - `{*dog|*cat}` 이 문자 그대로 `*dog` 을 찾아
               **조용히 0건**을 돌려줬다. `*` 가 정확 검색의 표준 표기가 된 뒤로는
               자연스럽게 칠 법한 질의라 더 위험하다(2026-08-25).
            """
            keyword = keyword.strip()
            if keyword.startswith('*'):
                stripped = keyword.lstrip('*').strip()
                if stripped:
                    return contains_exact(stripped)
            return contains(keyword)

        def or_group_mask(or_group) -> np.ndarray:
            mask = np.zeros(n, dtype=bool)
            for keyword in or_group:
                mask |= or_term(keyword)
            return mask

        n = len(df)
        survivors = np.ones(n, dtype=bool)

        # 1. Normal (AND) - 각 키워드가 모두 포함되어야 함
        for keyword in search_params['normal']:
            survivors &= contains(keyword)
        if not survivors.any():
            return df.iloc[:0]

        # 2. OR - {a|b}, {c|d} → (a OR b) AND (c OR d).
        #
        # ⚠️ 초기화 판정은 **'첫 바퀴인가'** 여야 한다. 예전엔 '누적 마스크에 매치가
        #    있나' 로 물어서, 어떤 그룹이 **정당하게 0건**이면 그 그룹이 다음 그룹으로
        #    덮여 통째로 사라졌다 - 그래서 같은 뜻인데 **그룹 순서만 바꾸면 답이 달라졌다**:
        #      `{zzz|yyy}, {solo|monochrome}` -> 5건 (비어야 맞다)
        #      `{solo|monochrome}, {zzz|yyy}` -> 0건 (맞다)
        #    원본(C:/VNR/NAIA2.0 search_engine.py:92)도 같은 모양이었다 - 물려받은 결함이다.
        if search_params.get('or'):
            final_or: Optional[np.ndarray] = None
            for or_group in search_params['or']:
                group_mask = or_group_mask(or_group)
                final_or = group_mask if final_or is None else (final_or & group_mask)
            survivors &= final_or
            if not survivors.any():
                return df.iloc[:0]

        # 3. Exact (*) - 태그 전체 일치
        for keyword in search_params['exact']:
            survivors &= contains_exact(keyword)

        # 3-b. 포함칸에 쓴 `~` = 제외. 예전엔 파싱만 하고 **아무도 안 써서** 그 낱말이
        #      통째로 사라졌다 - 걸러지기는커녕 검색이 조용히 넓어졌다(실측: `~dog ears`
        #      를 포함칸에 넣으면 `cat` 행까지 돌아왔다). 자리를 잘못 썼더라도 뜻은
        #      명백하므로 제외로 읽는다(사용자 결정 2026-08-25).
        for keyword in search_params['not_exact']:
            survivors &= ~contains_exact(keyword)

        # 4. Normal Exclude - 해당 키워드를 포함하지 않아야 함
        for keyword in exclude_params['normal']:
            survivors &= ~contains(keyword)

        # 5. Exact Exclude - 태그 전체가 일치하면 뺀다. `~` 와 `*` **둘 다** 받는다.
        #    예전엔 `*` 쪽을 아무도 안 써서 `제외: *dog ears` 가 한 줄도 못 줄였다.
        for keyword in exclude_params['not_exact'] + exclude_params['exact']:
            survivors &= ~contains_exact(keyword)

        # 6. OR Exclude - `{a|b}` = 그 중 하나라도 들면 뺀다.
        #    예전엔 파싱만 하고 아무도 안 썼다. 게다가 정규식이 중괄호째 지우므로
        #    낱말이 통째로 사라져 `제외: {dog|cat}` 이 한 줄도 못 줄였다.
        for or_group in (exclude_params.get('or') or []):
            survivors &= ~or_group_mask(or_group)

        return df[survivors]

    def search_in_file(self, file_path: str, search_params: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """단일 Parquet 파일 내에서 검색을 수행합니다.

        파일을 행 배치로 흘려 읽으며 배치마다 걸러 낸다. 모든 마스크가 행 단위라
        배치 경계는 결과에 영향을 주지 않는다(통짜 읽기와 동치) — 대신 스캔이 잡고
        있는 양이 '워커당 파일 한 통'에서 '워커당 배치 하나 + 살아남은 행'으로 준다.
        """
        # 등급 필터링 - 최적화: 모든 등급이 선택된 경우 건너뛰기
        enabled_ratings = set()
        if search_params.get('rating_e'): enabled_ratings.add('e')
        if search_params.get('rating_q'): enabled_ratings.add('q')
        if search_params.get('rating_s'): enabled_ratings.add('s')
        if search_params.get('rating_g'): enabled_ratings.add('g')
        rating_filtered = len(enabled_ratings) < 4

        query = search_params.get('query')
        exclude_query = search_params.get('exclude_query')

        survivors: List[pd.DataFrame] = []
        try:
            for df in _iter_parquet_frames(file_path, self.BATCH_ROWS):
                # 인덱스 정규화: 원격 태그 아카이브 parquet 일부는 뒤섞인(때로 비유니크)
                # int64 인덱스를 갖는다(실제 식별자는 'id' 컬럼). parquet 자체는 수정
                # 불가(원격 로드)이므로 읽는 단계에서 0..N-1 RangeIndex 로 리셋해 태그
                # 텍스트가 행 단위로만 만들어지게 한다 — 비유니크 인덱스에서 서로 다른
                # 행의 태그가 병합되던 결함(예: '2girls' 행이 '1girl' 검색에 매칭)을
                # 읽는 단계에서 차단.
                if not isinstance(df.index, pd.RangeIndex):
                    df = df.reset_index(drop=True)

                # 모든 등급이 선택되지 않은 경우만 필터링
                if rating_filtered:
                    df = df[df['rating'].isin(enabled_ratings)]
                    if df.empty:
                        continue

                # 검색어가 있을 때만 태그 텍스트 생성 (성능 최적화)
                if query or exclude_query:
                    # _apply_filters 가 태그 텍스트를 Arrow 배열로만 들고 있으므로 프레임에
                    # 컬럼을 붙이지 않는다 — 붙이기 위한 방어적 copy 도, 뒤이은 drop 도 없다.
                    df = self._apply_filters(df, query, exclude_query)
                    if df.empty:
                        continue
                    if 'tags_string' in df.columns:
                        df = df.drop(columns=['tags_string'])

                survivors.append(df)
        except Exception:
            return None # 파일 읽기 실패 시 건너뛰기

        if not survivors:
            return None
        if len(survivors) == 1:
            return survivors[0].reset_index(drop=True)
        return pd.concat(survivors, ignore_index=True)
