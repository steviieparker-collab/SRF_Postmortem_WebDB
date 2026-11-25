# MCP 실제 응답 샘플 (2026-11-25)

보고서에 인용할 수 있도록 실제 서버 응답을 그대로 저장한 파일이다.
서버: `http://141.223.105.232:8766/mcp` (stateless JSON, LAN passwordless).

| 파일 | 호출 | 설명 |
|---|---|---|
| `tools_list.json` | `tools/list` | 도구 17개 목록과 스키마 |
| `capabilities.json` | `postmortem_get_capabilities` | 데이터 커버리지, `data_as_of`, 인덱스 상태 |
| `connection.json` | `GET /connection` | 접속 URL, 인증 방식, 세션 모드 |
| `aggregate_fault_type_2026.json` | `postmortem_aggregate_events(group_by="fault_type", year=2026)` | MS 제외 2026년 fault 집계 (total 38) |

재현:

```bash
curl -sS http://141.223.105.232:8766/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

수치는 전부 서버가 SQL로 계산한 값이다. 파일을 수정하지 말고 그대로 인용한다.
