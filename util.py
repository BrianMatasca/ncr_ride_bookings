import pandas as pd

class ETL:


    def __init__(self, source, destination):
        self.source = source
        self.destination = destination

    def extract(self):
        self.dfUber = pd.read_csv("ncr_ride_bookings.csv")
        pass

    def transform(self):
        categorical_Columns=["booking_status","vehicle_type","reason_for_cancelling_by_customer","driver_cancellation_reason","payment_method"]
        self.dfUber.columns = [col.replace(' ', '_').lower() for col in self.dfUber.columns]
        for column in categorical_Columns:
        # Primero normalizar los valores a minúsculas y sin espacios
            self.dfUber[column] = (self.dfUber[column]
                        .astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(' ', '_')
                        .astype('category'))
        
        datetime = self.dfUber['date'].str.cat(self.dfUber['time'], sep='_')
        self.dfUber.insert(0, 'datetime', datetime)
        self.dfUber = self.dfUber.drop_duplicates(subset=['booking_id'])
        pass

    def load(self):
        # Logic to load the transformed data into the destination
        pass