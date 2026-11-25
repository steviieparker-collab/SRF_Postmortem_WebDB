# SRF Postmortem 접근/보안 설계 (2026-11-23)

## 목표 (PLSArchiver와 동일한 모델)
- **사내망** `141.223.105.0/24` → guest (로그인 없이 읽기)
- **외부망** → `@postech.ac.kr` 이메일 로그인
- 쓰기(관리)는 별도 관리자 비밀번호/세션 유지

## 현재 (Cloudflare Tunnel)
- `postmortem.pls-srf.org` → cloudflared → `127.0.0.1:8050`
- Cloudflare Zero Trust Access 정책:
  1. (선택) Block: 알려진 공격 IP (예: 141.223.181.53, .57)
  2. Bypass(또는 IP 기반 Allow): `141.223.105.0/24`
  3. Allow: `@postech.ac.kr` (One-time PIN/Google)
- 주의: Bypass 정책은 플랜에 따라 다를 수 있음. 안 되면 하이브리드(사내 LAN 리스너 + 앱 guest) 사용.
- 사내 PC가 프록시를 거치면 Cloudflare가 프록시 IP를 보므로 IP 정책이 안 맞을 수 있음 → 실제 .105 PC에서 검증.

## 향후 (POSTECH 도메인/리버스 프록시로 이전)
- 앱은 Cloudflare에 종속되지 않는다. 접근 정책은 **프록시 계층**에서 동일하게 구현한다.
  - 사내 IP → guest, 외부 → POSTECH SSO/이메일.
- `TRUSTED_PROXY_CIDRS` (환경변수, 기본 `127.0.0.1/32,::1/128`):
  - 리버스 프록시 IP/CIDR을 추가해야 `CF-Connecting-IP`/`X-Forwarded-For`/`X-Real-IP`를 신뢰한다.
  - 신뢰 프록시가 아니면 peer IP만 사용하므로, 직접 접속자가 헤더를 위조해 rate limit을 우회할 수 없다.
- `web.host`:
  - 현재 `127.0.0.1` (터널 전용).
  - POSTECH 프록시가 **다른 호스트**면 `141.223.105.232` 등으로 바인딩하고, nftables에서 **프록시 IP만** 8050 허용.
  - 프록시가 같은 호스트면 `127.0.0.1` 유지.
- 전환 체크리스트:
  1. 새 도메인 DNS/프록시 설정, `config.yaml`의 `web.url_base` 변경
  2. 프록시에서 사내/외부 정책 구현 (IP guest + POSTECH 인증)
  3. `TRUSTED_PROXY_CIDRS`에 프록시 IP 추가 후 `srf-web` 재시작
  4. 프록시 IP만 8050 허용하도록 nftables 조정
  5. Cloudflare Tunnel/`cloudflared` 제거 (선택), Access 정책 해제
  6. 외부/내부 각각에서 접근 검증

## 방화벽 (nftables)
- inbound default-drop. loopback/established/ICMP 허용.
- `tailscale0`: 관리용 전체 허용 (SSH 등)
- `eno1`:
  - `141.223.105.230` → SSH(22), 8050, 8765, 8766 (관리 PC)
  - `141.223.105.0/24` → 8765, 8766 (**MCP 사내망 전용**)
- 8050/8090은 loopback 바인딩 (터널/SSH 터널 전용)
- SSH: key-only, root 차단, `AllowUsers shpark`, `MaxAuthTries 3`

## MCP (사내망 전용)
- Cloudflare Tunnel에 올리지 않는다. 외부에서 사용하지 않는다.
- `.env`:
  - `MCP_ALLOWED_CIDRS=141.223.105.0/24,127.0.0.1/32,::1/128`
  - `MCP_PASSWORDLESS_CIDRS=141.223.105.0/24` (사내 신뢰, 편의 우선)
- 방화벽에서 `.105.0/24` → 8765/8766 허용 (`~/apply_mcp_lan_nft.sh`).
- 사내 사용 URL: `http://141.223.105.232:8765/mcp` (SSE) / `:8766/mcp` (JSON)
- 내부망 전체가 passwordless이므로, MCP 도구가 DB를 조회할 수 있음을 전제로 신뢰 대역을 운영한다.
  - 더 엄격하게 하려면 `MCP_PASSWORDLESS_CIDRS`를 `.230/32`로 좁히고 Bearer/비밀번호를 요구.

## 앱 보안
- `/api/config`, 쓰기 API: 관리자 비밀번호/세션 필요
- `/docs`, `/redoc`, `/openapi.json`: 비활성화
- 비밀번호는 헤더(`X-Postmortem-Password`) + HttpOnly 세션; URL 노출 금지
- IP당 5회/60초 실패 → 15분 차단
- access log의 `password=` 마스킹
- 보안 헤더: CSP, X-Frame-Options DENY, HSTS, Referrer-Policy, nosniff

## 백업
- 1U 전면 분리 HDD를 이용한 오프라인 로테이션.
- restic/borg로 **암호화** 저장, 백업 시에만 마운트, 백업 후 분리.
- 패스프레이즈는 서버에 두지 않는다. 주기적 복원 테스트. 가능하면 오프사이트 1부(3-2-1).
- `.env`/`config.yaml`은 암호화 백업에만 포함.
