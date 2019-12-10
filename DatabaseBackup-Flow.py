import prefect
import time
from prefect import Flow, Task
from datetime import timedelta
from prefect.schedules import IntervalSchedule

class ValidateConnection(Task):
    def run(self):
        print('Validating connection...')
        time.sleep(5)
        print('Connection Validated')

class CreateDatabaseBackup(Task):
    def run(self):
        print('Creating database backup...')
        time.sleep(10)
        print('Database backup created.')

class MigrateDatabaseBackup(Task):
    def run(self):
        print('Migrating database backup...')
        time.sleep(10)
        print('Database migrated.')

schedule = IntervalSchedule(interval=timedelta(minutes=5))
with Flow("Create Database Backup", schedule=schedule) as DatabaseBackupFlow:
    print(f"Running on Prefect v{prefect.__version__}")
    validate = ValidateConnection()
    create = CreateDatabaseBackup()
    migrate = MigrateDatabaseBackup()

    validate()
    create(upstream_tasks=[validate])
    migrate(upstream_tasks=[create])

DatabaseBackupFlow.deploy(
    "DevOps",
    base_image="python:3.7",
    python_dependencies=[],
    registry_url="znicholasbrown",
    image_name="prefect_flow",
    image_tag="demo-database-backup-flow",
)