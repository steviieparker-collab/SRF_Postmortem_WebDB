# MCP 파형·보고서 Feature/Vector 작업 (포인터)

전체 기획서와 데이터 스냅샷은 `research_MCP` 프로젝트에 정리되어 있다.

- 기획서: `/home/oc-main/claude/projects/research_MCP/srf_postmortem/PLAN.md`
- 새 세션 가이드: `/home/oc-main/claude/projects/research_MCP/srf_postmortem/NEW_SESSION.md`
- 데이터 인벤토리: `/home/oc-main/claude/projects/research_MCP/srf_postmortem/data_inventory.md`
- Feature store DDL: `/home/oc-main/claude/projects/research_MCP/srf_postmortem/schema/feature_store.sql`

## 요약

현재 MCP(`src/mcp_server/server.py`)는 `events.db`와 FTS 인덱스, Archiver만 조회하며
parquet 파형 값과 HWP/PDF 본문은 읽지 않는다. 고급 quench RF 질문을 처리하려면
**배치에서 파형·보고서 feature/vector를 생성해 `feature_store.sqlite3`로 저장하고,
MCP가 이를 CPU로 검색**해야 한다.

- 배치: GPU (311 parquet, 96 HWP/PDF)
- 런타임: srf-hprf CPU, GPU PC/임베딩 API 의존 없음
- 질의 텍스트 임베딩: 배치와 동일 모델을 CPU(ONNX)로 실행
- 실시간 갱신 없음: 2026-11-18 스냅샷 기준
- 추가 도구: `analyze_waveform`, `find_similar_waveforms`, `search_reports`,
  `analyze_quench`, `compare_quench_events`, `get_pv_series_multi`, `list_rf_pvs`

구현을 시작할 때는 위 `NEW_SESSION.md`의 Task 1(parquet feature extractor)부터 진행한다.
