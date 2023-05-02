from examples.demo_job_manager import DemoJobManager

if __name__ == '__main__':
    parameters = {
        'env': 'dev',
        'actual_date': '2022-04-30',
        'job_names': ['demo_job'],
        'some_extra_parameter': True
    }
    DemoJobManager().run(parameters)
