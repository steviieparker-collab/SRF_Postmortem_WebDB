-- notes 에 MPS 가 원인/분류로 기록된 Case 1: Beam_loss 이벤트를 수동 지정 Case 2: MPS 로 옮긴다.
-- 분류기 결과(fault_type, case_id)는 그대로 두고 user_fault_type 만 설정한다.
--
-- 2026-10-29 라이브 DB 실측: 대상 55건 (모두 notes 에 '분류: MPS')
--   - 원인 줄에 MPS 가 직접 적힌 건 39
--   - 원인 줄은 PS Off/Failed 등이고 '분류: MPS' 로 분류된 건 16
--
-- 적용 전 백업 후 실행. 재실행해도 이미 옮긴 건은 대상이 아니다.

BEGIN;

UPDATE events
   SET user_fault_type = 'Case 2: MPS'
 WHERE COALESCE(NULLIF(user_fault_type, ''), fault_type, '') = 'Case 1: Beam_loss'
   AND notes LIKE '%MPS%';

COMMIT;

SELECT 'case1 left with MPS notes' AS remaining, COUNT(*) FROM events
 WHERE COALESCE(NULLIF(user_fault_type, ''), fault_type, '') = 'Case 1: Beam_loss'
   AND notes LIKE '%MPS%';
SELECT 'case2 MPS total' AS remaining, COUNT(*) FROM events
 WHERE COALESCE(NULLIF(user_fault_type, ''), fault_type, '') = 'Case 2: MPS';
