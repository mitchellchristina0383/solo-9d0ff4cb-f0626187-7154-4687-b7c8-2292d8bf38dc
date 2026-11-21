"""内置样例：初始数据 + 交错脚本 + 核对查询。"""

PRESETS = [
    {
        "id": "shift-withdraw",
        "name": "两人同时撤班（写偏斜）",
        "description": "两人同时撤班，规则是“至少留一人在岗”。读已提交下两笔都成功 → 无人值守；"
                       "可串行化下后提交者被 40001 拒绝 → 仍有一人在岗。",
        "init_sql": """DROP TABLE IF EXISTS shift_duty;
CREATE TABLE shift_duty (
    name    text PRIMARY KEY,
    on_duty boolean NOT NULL
);
INSERT INTO shift_duty (name, on_duty) VALUES
    ('alice', true),
    ('bob', true);""",
        "script": """-- 两人同时撤班：每个人都先确认“在岗人数 >= 2”（即还有别人顶着），
-- 才把自己撤下。两个会话各撤一人，最后都提交。

@A complete
UPDATE shift_duty SET on_duty = false
WHERE name = 'alice'
  AND (SELECT count(*) FROM shift_duty WHERE on_duty) >= 2;

@B complete
UPDATE shift_duty SET on_duty = false
WHERE name = 'bob'
  AND (SELECT count(*) FROM shift_duty WHERE on_duty) >= 2;

@A complete
COMMIT;

@B complete
COMMIT;""",
        "check_sql": """-- 核对：还有几人在岗？谁在岗？0 即“无人值守”
SELECT count(*) FILTER (WHERE on_duty) AS on_duty_count,
       coalesce(string_agg(name, ', ') FILTER (WHERE on_duty), '（无）') AS still_on_duty
FROM shift_duty;""",
    },
    {
        "id": "lock-wait",
        "name": "行锁等待（blocked 模式）",
        "description": "两个会话更新同一行。B 用 blocked 模式等数据库确认被 A 阻塞即推进，"
                       "让持锁的 A 先提交。读已提交下 B 在 A 的结果上继续 → 700；"
                       "可串行化下 B 解除阻塞时收到 40001 → 900。",
        "init_sql": """DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
    id      int PRIMARY KEY,
    balance int NOT NULL
);
INSERT INTO accounts (id, balance) VALUES (1, 1000);""",
        "script": """-- A 先改第一行（持行锁），B 改同一行必然等锁。
-- B 用 blocked 模式：确认被 A 阻塞就推进，让 A 先 COMMIT，
-- B 的 UPDATE 解除阻塞后在 A 提交后的数据上继续。

@A complete
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

@B blocked
UPDATE accounts SET balance = balance - 200 WHERE id = 1;

@A complete
COMMIT;

@B complete
COMMIT;""",
        "check_sql": "SELECT id, balance FROM accounts ORDER BY id;",
    },
    {
        "id": "timeout-demo",
        "name": "超时回滚（演示）",
        "description": "B 用 complete 模式等 A 的行锁，但 A 的 COMMIT 排在 B 之后、永远轮不到 → "
                       "本轮超时，引擎取消卡住的语句并回滚两个事务，余额保持 1000。"
                       "重跑仍从初始数据开始。",
        "init_sql": """DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
    id      int PRIMARY KEY,
    balance int NOT NULL
);
INSERT INTO accounts (id, balance) VALUES (1, 1000);""",
        "script": """-- B 用 complete 模式：必须等语句完成才能推进，
-- 但它被 A 的行锁卡住，而 A 的 COMMIT 排在后面永远轮不到 → 超时。
-- 把 B 改成 blocked 模式（见“行锁等待”样例）即可正常跑完。

@A complete
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

@B complete
UPDATE accounts SET balance = balance - 200 WHERE id = 1;

@A complete
COMMIT;

@B complete
COMMIT;""",
        "check_sql": "SELECT id, balance FROM accounts ORDER BY id;",
    },
]
