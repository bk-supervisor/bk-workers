import os, time, zipfile
import requests

CC_BASE = os.environ.get("CC_BASE", "http://127.0.0.1:8030")
TIMEOUT = int(os.environ.get("WORKER_TIMEOUT_SEC", "3600"))

def cc_post(path: str, payload: dict):
    r = requests.post(f"{CC_BASE}{path}", json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def token_issue(job_id: int) -> str:
    j = requests.post(f"{CC_BASE}/api/jobs/{job_id}/token_issue", timeout=20).json()
    if not j.get("ok"):
        raise RuntimeError(f"token_issue failed: {j}")
    return j["token"]

def claim(job_id: int, token: str, actor: str):
    return cc_post(f"/api/jobs/{job_id}/claim", {"token": token, "actor": actor})

def log(job_id: int, token: str, level: str, msg: str):
    return cc_post(f"/api/jobs/{job_id}/log", {"token": token, "level": level, "message": msg[:2000]})

def complete(job_id: int, token: str, ok: bool, output_uri: str = None, error: str = None):
    payload = {"token": token, "ok": ok}
    if output_uri: payload["output_uri"] = output_uri
    if error: payload["error"] = error
    return cc_post(f"/api/jobs/{job_id}/complete", payload)

def do_demo_generate(job_id: int) -> str:
    out_dir = f"/tmp/bk_job_{job_id}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/output.zip"
    with zipfile.ZipFile(out_path, "w") as z:
        z.writestr("result.txt", f"job={job_id} generated_at={time.time()}\n")
    return "file://" + out_path

def run_one(job_id: int):
    actor = os.environ.get("WORKER_ACTOR", "runpod-worker")
    token = token_issue(job_id)
    claim(job_id, token, actor)
    log(job_id, token, "info", f"claimed job_id={job_id}")

    for i in range(3):
        log(job_id, token, "info", f"working step {i+1}/3")
        time.sleep(1)

    output_uri = do_demo_generate(job_id)
    complete(job_id, token, True, output_uri=output_uri)
    return {"ok": True, "job_id": job_id, "output_uri": output_uri}

if __name__ == "__main__":
    job_id = int(os.environ["JOB_ID"])
    print(run_one(job_id))
