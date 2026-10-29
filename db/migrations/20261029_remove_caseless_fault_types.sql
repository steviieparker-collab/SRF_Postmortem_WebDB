-- case 번호가 없는 레거시 fault_types 행('Unknown'/'Utility'/'Others')을 정리한다.
-- 정식 항목은 'Case 0: Unknown', 'Case 14: Utility', 'Case 15: Others' 로 이미 존재한다.
--
-- 적용 전 백업:  python3 -c "import sqlite3;s=sqlite3.connect('db/events.db');d=sqlite3.connect('db/backups/events.db.bak.XX');s.backup(d)"
-- 적용:          python3 -c "import sqlite3;sqlite3.connect('db/events.db').executescript(open('db/migrations/20261029_remove_caseless_fault_types.sql',encoding='utf-8').read())"

BEGIN;

-- 이벤트가 레거시 이름을 참조하면 정식 case 이름으로 옮긴다 (2026-10-29 실측: 해당 2건, 모두 Case 0)
UPDATE events
   SET fault_type = CASE fault_type
                      WHEN 'Utility' THEN 'Case 14: Utility'
                      WHEN 'Others'  THEN 'Case 15: Others'
                      ELSE 'Case 0: Unknown'
                    END
 WHERE fault_type IN ('Unknown', 'Utility', 'Others');

UPDATE events
   SET user_fault_type = CASE user_fault_type
                           WHEN 'Utility' THEN 'Case 14: Utility'
                           WHEN 'Others'  THEN 'Case 15: Others'
                           ELSE 'Case 0: Unknown'
                         END
 WHERE user_fault_type IN ('Unknown', 'Utility', 'Others');

DELETE FROM fault_types
 WHERE name IN ('Unknown', 'Utility', 'Others');

COMMIT;

SELECT 'remaining caseless rows' AS remaining, name FROM fault_types
 WHERE name NOT LIKE 'Case %';
SELECT 'bare Unknown events' AS remaining, COUNT(*) FROM events WHERE fault_type = 'Unknown';
