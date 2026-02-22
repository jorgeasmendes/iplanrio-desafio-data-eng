from prefect import flow, task
from prefect.logging import get_run_logger

@task
def task_test():
    logger=get_run_logger()
    logger.info("Task rodando")
    return

@flow
def flow_test():
    logger=get_run_logger()
    logger.info("Flow iniciando")
    task=task_test()
    logger.info("Flow terminando")