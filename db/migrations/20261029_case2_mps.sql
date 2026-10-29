-- Case 2 라벨을 'Beam_loss' -> 'MPS' 로 변경 (자동 분류는 case 1 로 통합, MPS 는 수동 지정 전용)
--
-- 적용 전에 반드시 백업:  sqlite3 db/events.db ".backup db/events.db.bak.20261029"
-- 적용:                   sqlite3 db/events.db < db/migrations/20261029_case2_mps.sql
--
-- seed_fault_types() 는 INSERT OR IGNORE 라 기존 'Case 2: Beam_loss' 행을 고치지 못한다.
-- 그래서 운영 DB 에는 아래 UPDATE 가 필요하다. 실제 갱신 건수는 거의 0 일 것으로 예상.

BEGIN;

UPDATE fault_types
   SET name = 'Case 2: MPS',
       description = 'MPS trip (manually assigned).'
 WHERE name = 'Case 2: Beam_loss';

UPDATE events
   SET fault_type = 'Case 2: MPS'
 WHERE fault_type = 'Case 2: Beam_loss';

UPDATE events
   SET user_fault_type = 'Case 2: MPS'
 WHERE user_fault_type = 'Case 2: Beam_loss';

COMMIT;

-- 확인용 (UPDATE 결과는 sqlite3 CLI 에 출력되지 않으므로 아래로 검증)
SELECT name, description FROM fault_types WHERE name LIKE 'Case 2%';
SELECT COUNT(*) AS case2_events FROM events WHERE fault_type = 'Case 2: MPS';
