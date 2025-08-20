from util import ETL;

def main():
    etl_process = ETL()
    new_df = etl_process.etlProcess()
    etl_process.business_metrics()
    


if __name__ == "__main__":
    main()