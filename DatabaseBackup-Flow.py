import prefect
import time
from prefect import Flow, Task
from datetime import timedelta, timezone, datetime
from prefect.environments.storage import Docker
from prefect.schedules import IntervalSchedule


class ValidateConnection(Task):
    def run(self):
        print("Validating connection...")
        time.sleep(5)
        print("Connection Validated")
        return True


class CreateDatabaseBackup(Task):
    def run(self):
        print("Creating database backup...")
        time.sleep(10)
        print("Database backup created.")
        return True


class MigrateDatabaseBackup(Task):
    def run(self):
        print("Migrating database backup...")
        time.sleep(10)
        print("Database migrated.")
        return True


class ValidateBackup(Task):
    def run(self):
        print("Validating database backup...")
        time.sleep(5)
        print("Backup validated.")
        return True


class NotifyOfBackup(Task):
    def run(self):
        print("Sending notification...")
        time.sleep(5)
        print("Notification sent.")
        return True


schedule = IntervalSchedule(interval=timedelta(minutes=1))
with Flow("Create Database Backup", schedule=schedule) as DatabaseBackupFlow:
    print(f"Running on Prefect v{prefect.__version__}")
    validate = ValidateConnection()
    create = CreateDatabaseBackup()(upstream_tasks=[validate])
    migrate = MigrateDatabaseBackup()(upstream_tasks=[create])
    validate_backup = ValidateBackup()(upstream_tasks=[migrate])
    notify = NotifyOfBackup()(upstream_tasks=[migrate, validate_backup])

# DatabaseBackupFlow.storage = Docker(
#     base_image="python:3.8",
#     python_dependencies=[],
#     registry_url="znicholasbrown",
#     image_name="prefect_flow",
#     image_tag="database-backup-flow",
# )

DatabaseBackupFlow.register(project_name="Flow Schematics")
