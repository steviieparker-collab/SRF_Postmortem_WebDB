# Report search (`postmortem_search_reports`)

Task 4 산출물. 96개 첨부(HWP 77, PDF 15, PNG 2, DOCX 1, HWPX 1)에서 텍스트·이미지를
추출하고, 이미지는 로컬 GPU VLM으로 캡션/OCR을 생성한 뒤, CPU에서
lexical(FTS5 trigram) + dense(multilingual-e5-small) 하이브리드 검색을 제공한다.

## 파이프라인

```bash
cd ~/SRF_Postmortem_WebDB

# 1) 텍스트/이미지 추출 (srf-hprf, venv-report: pyhwp + PyMuPDF)
venv-report/bin/python scripts/extract_report_text.py \
    --manifest /tmp/attachment_manifest.csv \
    --out-dir data/reports --workers 4

# 2) 이미지 캡션/OCR (GPU PC, vlm-env: torch + Qwen2.5-VL-3B)
#    이미지 트리와 report_images.jsonl을 GPU PC로 복사한 뒤 실행
vlm-env/bin/python scripts/caption_report_images.py \
    --images-jsonl report_images.jsonl \
    --images-root srf_report_images \
    --out image_captions.jsonl

# 3) feature store 적재 + 임베딩 (srf-hprf, venv-report)
venv-report/bin/python scripts/build_report_index.py --force
```

- 1)은 `data/reports/`에 `report_documents.jsonl`, `report_images.jsonl`,
  `report_parse_manifest.csv`, `report_parse_validation.json`, `images/`를 쓴다.
- 2)는 GPU PC에서만 돈다. 407장, RTX 3060 12GB, 평균 8.6초/장.
  `image_captions.jsonl`은 `doc_id`+`ord`로 1)의 이미지와 매칭된다.
- 3)는 `feature_store.sqlite3`를 임시 복사해 채운 뒤 원자적으로 교체한다.
  파형 테이블(`event_features`, `waveform_vectors`)은 건드리지 않는다.
- **순서 중요**: `build_feature_store.py`(Task 2)로 DB를 새로 만들면 report
  테이블이 사라지므로, 그 뒤에 `build_report_index.py`를 다시 실행해야 한다.

## 결과 (2026-11-20)

| 항목 | 값 |
|---|---:|
| 첨부 | 96 |
| 고유 문서(content hash) | 91 |
| 이벤트-문서 연결 | 96 |
| 추출 텍스트 | 113,869자 (중앙값 884자) |
| 청크 (900자/120 overlap) | 157 |
| 추출 이미지 | 407 (617 MB, BMP/JPG/PNG) |
| VLM 캡션 | **407** (빈 캡션 0) |
| OCR 텍스트 | **404** (3장은 판독 불가) |
| 캡션/OCR 문자량 | 81,290 / 108,154자 |
| text vector | 157 × 384 |
| image caption vector | 407 × 384 |
| DB 크기 | 7.9 MB |
| 상태 | ok 94 / partial 2(PNG) / empty 0 / failed 0 |

중복 첨부: 동일 PDF 5부(2026-02-26 이벤트 5건), 동일 HWP 2부(2026-10-02 이벤트 2건).
`doc_id`는 파일 내용 해시라 문서는 91개이고, 이벤트 연결은
`report_document_events(doc_id, event_id)`에 모두 보존한다.

## 파서 동작

| 형식 | 텍스트 | 이미지 |
|---|---|---|
| HWP | `hwp5html` → HTML 파서. **표 셀 텍스트 포함** | `bindata/` 출력 |
| HWPX | zip + OWPML `<hp:t>` | `BinData/*` |
| PDF | PyMuPDF 페이지별 `get_text` | xref 중복 제거 후 추출 |
| DOCX | zip + `word/document.xml` `<w:t>` | `word/media/*` |
| PNG | 없음(partial) | 파일 자체 |

- HWP를 `hwp5txt`로만 처리하면 표가 `<표>`로 축약되어 47자짜리 문서가 나온다.
  `hwp5html`로 바꾼 뒤 같은 문서가 1,565자로 늘었다. `hwp5html` 실패 시
  `hwp5txt` + `olefile`로 fallback한다.
- HWP BinData 스트림은 headerless raw deflate인 경우가 많다. fallback 경로는
  `zlib.decompress(raw, -15)` 후 magic byte로 이미지 여부를 판별한다.
- 스캔 PDF/PNG처럼 텍스트가 없으면 `partial`로 기록하고 이미지만 남긴다.
  이 이미지들은 VLM 캡션/OCR로 검색 가능해졌다.

## 이미지 캡션 (Qwen2.5-VL-3B)

- GPU PC: RTX 3060 12GB, `torch 2.15.0+cu132`, `vlm-env`.
- 모델: `Qwen/Qwen2.5-VL-3B-Instruct` (bf16, `device_map="cuda"`).
- 입력: 이미지 + 보고서 제목/이벤트 ID, `max_pixels=1024*28*28`, greedy.
- 출력: `{"caption": 한국어 1-3문장, "text": 가시 텍스트 전사}`.
  문자열 안 실제 개행 때문에 JSON이 깨지는 경우가 있어 `json-repair`로 보정한다.
- 407장 처리, 오류 0, 평균 8.6초/장.

캡션은 **검색용 보조 텍스트**다. OCR은 알람 표의 PV 이름·시각·수치를 잘 잡지만,
캡션은 가끔 표현을 틀린다(예: RF를 "고성능 레이저"로 표기). 응답 caveat에
"VLM 전사값은 원본 이미지로 확인"을 명시한다.

## 임베딩

- 모델: `Xenova/multilingual-e5-small` int8 ONNX, 384차원
  (`data/models/multilingual-e5-small/`).
- E5 규약: 문서/캡션은 `"passage: "`, 질의는 `"query: "` prefix.
- mean pooling(attention mask) → L2 normalize → float32 BLOB.
- 최대 512 token, 청크 900자(한국어 ≈ 400 token).
- `model_versions`: `text_e5small_v1` (dim 384, normalize 1).
- `feature_vector_specs`: pooling/prefix/max token/chunk 파라미터 기록.

런타임(`venv-mcp`) 의존성: `numpy`, `onnxruntime`, `tokenizers`
(`requirements-mcp.txt`). 모델은 lazy load다.

## 검색

`postmortem_search_reports(query, event_id=None, top_k=8)`:

1. 질의 확장(켄치/퀜치/쿼치→quench, 이온펌프→ion pump, 빔덤프→beam dump 등)
2. 텍스트 청크: FTS5 trigram + e5 cosine
3. 이미지 캡션/OCR: `report_image_fts` trigram + `report_image_vectors` cosine
4. 두 종류를 Reciprocal Rank Fusion (k=60)으로 통합
5. lexical hit가 전혀 없고 최고 dense score < 0.85면 저신뢰 이웃을 숨긴다
   (`suppressed=true`).

응답 item:
- 공통: `kind` (`chunk`|`image`), `doc_id`, `event_id`, `event_ids`, `title`,
  `score`(RRF), `dense_score`, `fts_score`, `citation`, `url`
- chunk: `chunk_id`, `ord`, `text`
- image: `image_id`, `ord`, `caption`, `ocr_text`, `source_path`
- citation: `[R:event/doc]` (chunk), `[R:event/doc#imgN]` (image)

`event_id`를 주면 `report_document_events`로 그 이벤트에 연결된 문서만 본다
(중복 첨부의 다른 이벤트 연결도 유지).

### 실제 결과

```text
"KLY Ion Pump 알람"
  [IMG] 20221006_061900 20221006 KLY3 Ion pump1 Current too high
        "KLY3 이온 펌프 1의 전류가 너무 높은 알림..."
  [TXT] 20250313_053300 KLY2 Ion pump Current too high

"캐비티 온도 그래프"
  [IMG] 20250603_024124 2025-08차 2A Beamline
  [TXT] 20240907_025313 / 20241111_044552 quench reports

"켄치 발생 시 RF 반사파"
  [TXT] 20250306_195900 CM1 SRF cavity quench
  [IMG] 20250221_210600 RF fault 보고서 이미지

"존재하지않는키워드zzz" → returned=0, suppressed=true
```

## 검증 (2026-11-20)

- `PRAGMA integrity_check=ok`, `foreign_key_check` 0건
- `event_features` 311 / `waveform_vectors` 311 유지
- `report_documents` 91 / `report_document_events` 96 / `report_chunks` 157
- `report_images` 407 / `report_chunk_vectors` 157 / `report_image_vectors` 407
- caption 407 / OCR 404, FTS `report_fts` 157 / `report_image_fts` 407
- 벡터 차원 384, L2 norm 1.0, NaN 0
- MCP stdio smoke: 14개 도구,
  `reports_image: returned=4 images_indexed=407 kinds=['image','chunk',...]`

## 알려진 한계

- VLM 캡션은 표현 오류/추측이 있을 수 있다. OCR과 원본 이미지를 우선한다.
- HWP 도형 내부 텍스트·각주 등은 일부 누락될 수 있다.
- 스캔 PDF는 텍스트 레이어가 없어 `partial`이며, 캡션/OCR에 의존한다.
- 문서 91개/청크 157개/이미지 407개 규모라 전수 cosine으로 충분하다.
- 모델 교체 시 `model_key`를 바꿔 전체 재임베딩해야 한다.
- 실행 중인 MCP(`srf-mcp` 8765, `srf-mcp-json` 8766)는 2026-11-24 재시작 완료
  — 16개 도구가 라이브에 노출되어 있다.
