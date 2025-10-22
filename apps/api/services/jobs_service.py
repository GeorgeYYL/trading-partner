# 仅示意方法签名与依赖，不写实现
class JobsService:
    def __init__(self, repo, queue):
        self.repo = repo          # JobRepoAdapter
        self.queue = queue        # QueueAdapter

    def enqueue_daily(self, *, symbol: str, asof, requested_by: str, idempotency_key: str | None = None) -> dict:
        """编排：幂等保障→创建Job→构造消息→入队。返回 {job_id, status, created}"""
        ...

    def get_status(self, job_id: str):
        """读 Job 最新状态"""
        ...

    def readiness(self) -> dict:
        """聚合健康：{repo: bool, queue: bool, ok: bool}"""
        ...
