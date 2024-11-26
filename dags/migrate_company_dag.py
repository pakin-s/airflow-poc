from airflow.decorators import dag
from datetime import datetime
from tasks.migrate_company_task import (
    query_all_company_securities,
    map_to_company_securities_list,
    filter_invalid_companies,
    map_to_company_list,
    map_to_company_information_list,
    print_data,
)

@dag(
    dag_id="migrate_company_data",
    start_date=datetime(2024, 11, 21),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "Au Pakin"}
)
def migrate_company_data():

    query_company_securities_task = query_all_company_securities()

    mapped_data_task = map_to_company_securities_list(query_company_securities_task)

    filtered_data_task = filter_invalid_companies(mapped_data_task)

    mapped_company_task = map_to_company_list(filtered_data_task)
    mapped_company_info_task = map_to_company_information_list(filtered_data_task)

    print_companies_task = print_data(mapped_company_task)
    print_company_info_task = print_data(mapped_company_info_task)

    query_company_securities_task >> mapped_data_task >> filtered_data_task
    filtered_data_task >> [mapped_company_task, mapped_company_info_task]
    mapped_company_task >> print_companies_task
    mapped_company_info_task >> print_company_info_task

migrate_company_data()
