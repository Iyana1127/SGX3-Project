from hotqueue import HotQueue
from redis import Redis
import time

q = HotQueue("queue", host="localhost", port=6379, db=1)
job_store = Redis(host="localhost", port=6379, db=2)
redis_conn = Redis(host="localhost", port=6379)

@q.worker
def process_job(job_id):
    job_store.hset(job_id, "status", "in progress")
    try:
        # Simulated processing (you can call your real filters here)
        time.sleep(3)
        result = f"Processed job {job_id} (e.g. rush hour 2024 near UT)"
        redis_conn.set(f"result:{job_id}", result)
        job_store.hset(job_id, "status", "complete")
    except Exception as e:
        job_store.hset(job_id, "status", "failed")
        redis_conn.set(f"result:{job_id}", str(e))
