import os
import runpod

# We import your existing worker logic
import worker as bk_worker

# RunPod calls this with event={"input": {...}}
def handler(event):
    payload = (event or {}).get("input", {}) or {}

    # Expect job_id to be provided by whoever calls the endpoint
    job_id = payload.get("job_id")
    if job_id is None:
        return {"ok": False, "error": "missing_job_id", "hint": "Send input: {job_id: <int>}"}

    # CC base can be set as env var in RunPod, but allow override in request
    cc_base = payload.get("cc_base") or os.environ.get("CC_BASE", "http://127.0.0.1:8030")
    os.environ["CC_BASE"] = cc_base

    # Worker expects JOB_ID env in your current implementation
    os.environ["JOB_ID"] = str(job_id)

    # Run the job (your worker prints + returns dict)
    result = bk_worker.run_one(job_id)
    return {"ok": True, "result": result}

runpod.serverless.start({"handler": handler})
