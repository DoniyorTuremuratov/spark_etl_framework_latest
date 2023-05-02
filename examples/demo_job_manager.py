from etl_framework.job_manager import JobManager
from examples import jobs


class DemoJobManager(JobManager):
    extra_params = [
        'some_extra_parameter'
    ]

    def add_extra_params(self, global_params: dict):
        for param in self.extra_params:
            self.set_extra_param(param, global_params[param])

    def get_jobs_package(self):
        return jobs

    def run(self, global_params: dict):
        self.add_extra_params(global_params)
        super().run(global_params)
