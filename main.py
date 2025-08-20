from util import ETL;

def main():
    etl_process = ETL()
    new_df = etl_process.etlProcess()
    print(new_df.head())
    print(new_df.info())

if __name__ == "__main__":
    main()