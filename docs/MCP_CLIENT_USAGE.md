# MCP 클라이언트 사용 안내 (srf-postmortem)

이 문서는 OpenClaw/Claude Code 같은 MCP 클라이언트에서 `srf-postmortem`
서버를 쓸 때의 호출 방식, 이미지 반환, 파라미터 규칙을 정리한 것이다.
서버는 stateless Streamable HTTP로 동작한다.

---

## 1. Endpoint와 헤더

| 인스턴스 | URL | 응답 | Accept |
|---|---|---|---|
| JSON | `http://141.223.105.232:8766/mcp` | JSON | `application/json` |
| SSE | `http://141.223.105.232:8765/mcp` | SSE | `application/json, text/event-stream` |

- 두 인스턴스 모두 stateless라 `Mcp-Session-Id`와 `initialize` 선행이 필요 없다.
- 8766은 단순 HTTP/JSON-RPC 클라이언트에 권장.
- `GET /mcp`는 405다. 반드시 `POST /mcp`.
- 표준 MCP 클라이언트는 `tools/list`, `tools/call`을 사용한다. curl로 직접
  호출할 때는 아래 형식을 그대로 쓴다.

```bash
curl -s -X POST http://141.223.105.232:8766/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

---

## 1.1 처음 연결할 때 장치 약어표

`postmortem_get_capabilities`를 호출하면 `abbreviations` 블록이 함께 온다.
여기에는 `CM`, `LLRF`, `HPRF`, `KSU`, `IC`, `MIS`, `PSI`, `MPS`, `BLM`,
`BPM`, `DCCT`, `TFS`, `FOFB`, `BCL01/BCL03`, `BPR01`, `BPS01` 같은 장치명과
`VAC/QUEN/ARC/HE-PR/RDY/INT` 채널 접두어, `_v`/`_d` 접미어 설명이 들어 있다.

같은 내용을 topic으로 직접 받을 수도 있다.

```json
{"name":"postmortem_get_glossary",
 "arguments":{"topic":"abbreviations"}}
```

MCP resource `postmortem://abbreviations`도 같은 표를 반환한다. 에이전트는
장치명을 추측하지 말고 이 표를 먼저 읽어야 한다.

---

## 2. 도구 호출과 응답 파싱

```bash
curl -s -X POST http://141.223.105.232:8766/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{
    "jsonrpc":"2.0","id":2,"method":"tools/call",
    "params":{"name":"postmortem_get_event",
              "arguments":{"event_id":"20260921_162300"}}
  }'
```

응답 구조:

```
result.content[0].text   ← JSON 문자열 (기존 클라이언트 호환)
result.content[1..]      ← 이미지 요청 시 ImageContent 블록
```

`content[0].text`를 `json.loads()`하면 기존 dict 응답을 그대로 얻는다.
`structuredContent`는 도구에 따라 없을 수 있으므로 `content[0].text`를
정본으로 파싱하는 것이 안전하다.

---

## 3. 파형 이미지 직접 받기

이미지 요청 시 `content[0].text`는 JSON, 그 뒤에 `ImageContent` 블록이 붙는다.
이미지 데이터는 base64이며 `mime_type`은 `image/jpeg`다. 파일 경로 제한
(`/tmp` 접근 불가)을 피하려면 이 블록을 그대로 저장하면 된다.

### 3.1 `postmortem_get_event`

```json
{
  "event_id": "20260921_162300",
  "include_images": true,
  "image_scope": "cavity"
}
```

- `waveform_image: true`도 같은 동작(하위 호환 alias). `"reference"` 같은
  truthy 문자열도 허용한다.
- `image_scope`: `all`(기본), `forward`, `cavity`, `reflect`, 또는
  `forward,cavity` 같은 comma list.

### 3.2 `postmortem_analyze_waveform`

```json
{
  "event_id": "20240907_025313",
  "generate_plot": true
}
```

- `generate_plot: true` 또는 `include_images: true`.
- 현재 구현은 웹 파이프라인이 미리 만든
  `data/waveforms/<event_id>/{forward,cavity,reflect}_narrow.jpg`를 반환한다.
  새로 그리는 것이 아니라 **이미 렌더링된 차트**를 그대로 준다.
- 이미지가 없으면 JSON에 `image_available: false`, `image_hint`가 들어오고
  이미지 블록은 생략된다.

### 3.3 `postmortem_analyze_quench`

```json
{"event_id": "20240907_025313", "include_images": true}
```

### 3.4 JSON 메타데이터

이미지가 있으면 JSON에 아래가 추가된다.

```json
{
  "image_available": true,
  "images": [
    {
      "event_id": "20260921_162300",
      "scope": "cavity",
      "mime_type": "image/jpeg",
      "source": "data/waveforms/20260921_162300/cavity_narrow.jpg",
      "url": "/api/waveforms/20260921_162300/cavity_narrow.jpg",
      "visible_channels": ["Cavity_SRF1", "QUEN_CM1_FC1", "..."],
      "caption_ko": "Cavity 패널: CM QUEN/ARC/VAC/HE-PR/RDY digital이 보인다."
    }
  ]
}
```

`url`은 웹 기준 상대 경로다. MCP 이미지 블록을 쓰면 별도 fetch가 필요 없다.
**이미지는 패널별로 보이는 채널이 다르다.** Forward 패널에 `RDY_KSU1`이
보여도 `QUEN_CM1`은 Cavity 패널에 있을 수 있다. `visible_channels`와
`caption_ko`를 먼저 읽는다.

---

## 3.5 분석용 도구

단일 이벤트의 원인 설명은 `postmortem_explain_event`를 먼저 쓴다.

```json
{"event_id": "20261013_212628", "include_images": true}
```

응답 핵심:

- `primary_interlock` — 가장 먼저 뜬 digital
- `quench.unit/source/units_with_quen_assert`
- `sequence.tau_reliable`, `per_cavity_tau_us`
- `root_cause` — note 기반
- `warnings`, `claim_checks`, `required_disclosures`
- `images[].visible_channels/caption_ko`

전체 패턴은 `postmortem_analyze_cohort`로 본다.

```json
{"fault_family": "cm_arc", "limit": 10}
```

큐레이션 예시는 `postmortem_find_examples`:

```json
{"topic": "cm_single_fault"}
```

`cm_single_fault`는 quench가 아니라 **CM ARC / flowmeter** 사례를 준다.
`quench_propagation`은 별도 topic이며 “단일 CM 교본으로 쓰지 말 것” 경고가 붙는다.

질문 라우팅:

```json
{"question": "CM 하나 fault로 빔덤프 메커니즘을 설명해줘"}
```

`postmortem_search_notes`는 운영자 notes의 유사 문구 검색이다. counts/timing에는
쓰지 않고, `postmortem_explain_event`/`postmortem_analyze_cohort`와 함께 쓴다.

---

## 4. 파라미터 규칙

### fault_type은 string 또는 list

`postmortem_search_events`, `postmortem_aggregate_events` 모두 허용한다.

```json
{"fault_type": ["Case 3: RF_Interlock", "Case 9: Cavity_quench"], "limit": 20}
```

기존 `fault_types` list도 계속 지원한다. 내부적으로 list는 `fault_types`로
정규화된다.

### 유효값 확인

- `postmortem_list_fault_types` → fault_type 목록/건수
- `postmortem_list_beam_times` → beam_time 목록
- `postmortem_get_capabilities` → 데이터 커버리지, 도구 목록, data_as_of

### 이미지 관련

- `include_images: bool` (기본 false)
- `waveform_image: bool` (`get_event` alias)
- `generate_plot: bool` (`analyze_waveform` alias)
- `image_scope: str` (`all`/`forward`/`cavity`/`reflect`/comma list)

---

## 5. 자주 나는 오류와 대응

| 증상 | 원인 | 대응 |
|---|---|---|
| `-32601 Method not found` | 도구 이름을 `method`에 넣음 | `method:"tools/call"`, `params.name` 사용 |
| `-32600 Missing session ID` | stateful 서버에 붙음 | 8765/8766 stateless 인스턴스 사용 |
| 406 | `Accept` 헤더 부족(8765) | `application/json, text/event-stream` 둘 다 |
| 403 `source_ip_not_allowed` | allowlist 밖 IP | 사내망/허용 IP에서 호출 |
| `fault_type` validation error | 문자열 자리에 list | `fault_type` list 허용 버전 사용 또는 `fault_types` |
| 이미지가 안 옴 | `include_images`/`generate_plot` 미지정 | 위 예시처럼 플래그 지정 |
| `image_available: false` | 해당 이벤트에 narrow JPEG 없음 | 웹 이벤트 페이지/URL 확인 |

---

## 6. 최소 Python 예시

```python
import base64, json, urllib.request

def call(tool, args):
    req = urllib.request.Request(
        "http://141.223.105.232:8766/mcp",
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":"tools/call",
                         "params":{"name":tool,"arguments":args}}).encode(),
        headers={"Content-Type":"application/json","Accept":"application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        res = json.load(r)["result"]

    payload = json.loads(res["content"][0]["text"])
    images = []
    for block in res["content"][1:]:
        if block.get("type") == "image":
            images.append(base64.b64decode(block["data"]))
    return payload, images

payload, images = call("postmortem_analyze_waveform",
                       {"event_id":"20240907_025313","generate_plot":True})
for i, blob in enumerate(images):
    open(f"waveform_{i}.jpg","wb").write(blob)
```

---

## 7. 참고

- 이미지 반환은 MCP Python SDK v2의 `Image` helper를 사용한다.
  클라이언트는 `ImageContent`를 지원해야 한다.
- 서버는 `data/waveforms`의 사전 렌더링 JPEG를 읽으므로 matplotlib 설치가
  필요 없다. 새 이벤트의 이미지가 없으면 웹 파이프라인에서 먼저 생성해야 한다.
- 도구 설명은 `tools/list`의 `description`에 있으며, 각 도구의 예시는
  `postmortem_get_capabilities`의 `tool_descriptions`/`examples`에서도 볼 수 있다.
