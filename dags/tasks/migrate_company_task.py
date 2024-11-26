import os
import json
from typing import List
from models.migrate_company_model import CompanySecurities, Company, CompanyInformation
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from dataclasses import asdict

@task
def query_all_company_securities():
    hook = MySqlHook(mysql_conn_id="my_sql_exchange_conn")

    query_path = os.path.join(os.path.dirname(__file__), '../queries', 'get_company_security_by_id.sql')
    with open(query_path, 'r') as file:
        sql = file.read()

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()

    return result

@task
def map_to_company_securities_list(result) -> List[CompanySecurities]:
    mapped_data = [
        CompanySecurities(
            id=row[0],
            security_id=row[1],
            name_th=row[2],
            name_en=row[3],
            business_type=row[4],
            product_description=row[5],
            juristic_id=row[6],
            phone_number=row[7],
            website_url=row[8],
            address_number=row[9],
            address_road=row[10],
            address_province=row[11],
            address_district=row[12],
            address_subdistrict=row[13],
            address_zipcode=row[14],
            revenue_amount=row[15],
            revenue_year=row[16]
        ) for row in result
    ]

    serialized_data = [asdict(record) for record in mapped_data]

    return serialized_data

@task
def filter_invalid_companies(mapped_data: List[CompanySecurities]) -> List[CompanySecurities]:
    filtered_data = []

    for record in mapped_data:
        if isinstance(record['id'], (int, float)) and record['security_id'] is not None:
            filtered_data.append(record)

    return filtered_data

@task
def map_to_company_list(data: List[CompanySecurities]) -> List[Company]:
    mapped_data = [
        Company(
            name_th=record['name_th'],
            name_en=record['name_en'],
            business_type=record['business_type'],
            business_characteristics=record['product_description'],
            past_income=record['revenue_amount'],
        )
        for record in data
    ]

    serialized_data = [asdict(record) for record in mapped_data]

    return serialized_data

@task
def map_to_company_information_list(data: List[CompanySecurities]) -> List[CompanyInformation]:
    mapped_data = [
        CompanyInformation(
            juristic_id=record['juristic_id'],
            phone_number=record['phone_number'],
            website_url=record['website_url'],
            address_number=record['address_number'],
            road=record['address_road'],
            province=record['address_province'],
            district=record['address_district'],
            sub_district=record['address_subdistrict'],
            postal_code=record['address_zipcode'],
        )
        for record in data
    ]

    serialized_data = [asdict(record) for record in mapped_data]

    return serialized_data

@task
def print_data(data):
    """This task prints any data in a formatted way"""
    print(f"Printing {len(data)} records:")
    for record in data:
        print(json.dumps(record, indent=4, ensure_ascii=False))
