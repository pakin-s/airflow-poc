from airflow.decorators import dag
from datetime import datetime
from tasks.migrate_company_task import query_company_security, map_to_dataclass, filter_invalid_ids, print_data

@dag(
    dag_id="migrate_company_data",
    start_date=datetime(2024, 11, 21),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "Au Pakin"}
)
def migrate_company_data():

    query_company_security_task = query_company_security()
    mapped_data_task = map_to_dataclass(query_company_security_task)
    filtered_data_task = filter_invalid_ids(mapped_data_task)
    print_filtered_data_task = print_data(filtered_data_task)

    query_company_security_task >> mapped_data_task >> filtered_data_task >> print_filtered_data_task

migrate_company_data()
