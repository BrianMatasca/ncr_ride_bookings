import pandas as pd
from tkinter import filedialog
import os

class ETL:

    def etlProcess(self):
        self.extract()
        self.transform()
        # self.load()
        return self.dfUber

    def extract(self):
        # self.dfUber = pd.read_csv("C:/Users/jufep/OneDrive/Escritorio/13vo Semestre/Distribuidos/ncr_ride_bookings.csv")
        file_path = filedialog.askopenfilename(
            title="Seleccionar archivo CSV",
            filetypes=[("CSV files", "*.csv"), ("Todos los archivos", "*.*")]
        )
        
        if file_path:
            self.dfUber = pd.read_csv(file_path)
            print(f"Archivo cargado: {os.path.basename(file_path)}")
            return True
        else:
            print("No se seleccionó ningún archivo")
            return False

   

    def standaredize(self):
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

    def duplicates(self):
        datetime = self.dfUber['date'].str.cat(self.dfUber['time'], sep='_')
        self.dfUber.insert(0, 'datetime', datetime)
        self.dfUber = self.dfUber.drop_duplicates(subset=['booking_id'])

    def transform(self):
       self.standaredize()
       self.duplicates()
       pass

    def load(self):
        # Logic to load the transformed data into the destination
        pass