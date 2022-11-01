"""
    author  : Azwar Rizaldy
    project : Core ETL S3 to PostgreSQL
"""


from application import (
    ReadData
)

if __name__ == '__main__':
    run_data = ReadData()
    run_data.insert()