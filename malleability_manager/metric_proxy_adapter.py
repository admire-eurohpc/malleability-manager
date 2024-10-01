import requests


class MetricProxyAdapter:
    def __init__(self, url: str):
        self.url = url

    def get_pending_jobs(self) -> dict[str, dict]:
        jobs = requests.get(f'{self.url}/queue/list').json()
        jobs = {job_id: job for job_id, job in jobs['jobs'].items() if job['STATE'] == 'PENDING'}
        return jobs

    def get_running_jobs(self) -> dict[str, dict]:
        jobs = requests.get(f'{self.url}/job/list').json()
        jobs = {job['jobid']: job for job in jobs if job['size'] > 0}
        return jobs

    def get_profiles(self) -> list[dict]:
        return requests.get(f'{self.url}/profiles').json()

    def get_profile(self, job_id: str) -> dict:
        return requests.get(f'{self.url}/profiles/get?jobid={job_id}').json()

    def get_models(self, job_id: str) -> list[dict[str, str]] | None:
        result = requests.get(f'{self.url}/model/get?jobid={job_id}').json()
        if 'success' in result and result['success'] is False:
            return None
        return result

    def get_model_for(self, job_id: str, metric: str, start: float, end: float, step: float) -> list[list[str]] | None:
        metric = requests.utils.quote(metric)
        url = f'{self.url}/model/plot?jobid={job_id}&metric={metric}&start={start}&end={end}&step={step}'
        result = requests.get(url).json()
        if 'success' in result and result['success'] is False:
            return None
        return result

    def get_model_at(self, job_id: str, metric: str, size: float) -> float | None:
        result = self.get_model_for(job_id, metric, size, size + 1, 1)
        if result is None:
            return None
        return float(result[0][1])

    def get_metric(self, job_id: str) -> dict:
        return requests.get(f'{self.url}/trace/metrics/?job={job_id}').json()

    def get_trace(self, job_id: str) -> dict:
        return requests.get(f'{self.url}/trace/json?jobid={job_id}').json()

    def get_trace_metric(self, job_id: str, metric: str) -> dict:
        return requests.get(f'{self.url}/trace/plot/?jobid={job_id}&filter={requests.utils.quote(metric)}').json()
