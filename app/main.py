"""Flask 入口：页面 + 运行接口。

环境变量：PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD / PORT
"""
import os
import threading

from flask import Flask, jsonify, render_template, request

from engine import run_experiment
from presets import PRESETS
from script_parser import ScriptError, parse_script

app = Flask(__name__)

# 所有请求共用一个沙箱库，同一时刻只允许跑一轮实验，避免互相重置数据
RUN_LOCK = threading.Lock()

DEFAULT_TIMEOUT = 5.0
MAX_TIMEOUT = 60.0


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "sandbox"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/presets")
def get_presets():
    return jsonify(PRESETS)


@app.post("/api/run")
def run():
    body = request.get_json(force=True, silent=True) or {}
    init_sql = (body.get("init_sql") or "").strip()
    script = (body.get("script") or "").strip()
    check_sql = (body.get("check_sql") or "").strip()
    try:
        timeout_s = float(body.get("timeout_s") or DEFAULT_TIMEOUT)
    except (TypeError, ValueError):
        timeout_s = DEFAULT_TIMEOUT
    timeout_s = min(max(timeout_s, 1.0), MAX_TIMEOUT)

    if not init_sql:
        return jsonify({"error": "初始数据 SQL 不能为空"}), 400
    if not check_sql:
        return jsonify({"error": "核对查询不能为空"}), 400
    try:
        steps = parse_script(script)
    except ScriptError as exc:
        return jsonify({"error": f"脚本解析失败：{exc}"}), 400

    if not RUN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有实验正在运行，请稍候（所有会话共用一个沙箱库）"}), 409
    try:
        rounds = run_experiment(conninfo(), init_sql, steps, check_sql, timeout_s)
    finally:
        RUN_LOCK.release()
    return jsonify({"rounds": rounds, "timeout_s": timeout_s})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), threaded=True)
