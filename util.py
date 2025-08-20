import pandas as pd
from tkinter import filedialog
import os

class ETL:

    def etlProcess(self):
        self.extract()
        self.transform()
        self.load()
        return self.dfUber

    def extract(self):
        file_path = filedialog.askopenfilename(
            title="Seleccionar archivo CSV",
            filetypes=[("CSV files", "*.csv"), ("Todos los archivos", "*.*")]
        )
        
        if file_path:
            self.dfUber = pd.read_csv(file_path)
            print(f"Archivo cargado: {os.path.basename(file_path)}")
            print("Perfilado de datos:")
            print("Muestra un resumen del DataFrame: cantidad de filas, columnas, nombres de columnas, tipo de datos y número de valores no nulos")
            print(self.dfUber.info())
            print("Genera estadísticas descriptivas de todas las columnas, tanto numéricas como categóricas (conteo, media, min, max, valores únicos, moda, etc.)")
            print(self.dfUber.describe(include="all"))
            print("Valores nulos por columna:")
            print(self.dfUber.isnull().sum())
            return True
        else:
            print("No se seleccionó ningún archivo")
            return False

    def standardize(self):
        self.dfUber.columns = [col.strip().lower().replace(" ", "_") for col in self.dfUber.columns]
        
        categorical_cols = self.dfUber.select_dtypes(include=["object"]).columns

        categorical_cols = [c for c in categorical_cols if c not in ["date", "time"]]

        numeric_cols = self.dfUber.select_dtypes(include=["int64", "float64"]).columns

        for col in categorical_cols:
            self.dfUber[col] = (
                self.dfUber[col]
                .astype(str)
                .str.lower()
                .str.strip()
                .str.replace(" ", "_")
                .astype("category")
            )
        
        for col in numeric_cols:
            self.dfUber[col] = pd.to_numeric(self.dfUber[col], errors="coerce")

            
    def clean_dates(self):
        self.dfUber['datetime'] = pd.to_datetime(
            self.dfUber['date'] + " " + self.dfUber['time'],
            errors='coerce'
        )

        self.dfUber = self.dfUber.drop(columns=['date', 'time'], errors='coerce')

        
    def duplicates(self):
        self.dfUber = self.dfUber.drop_duplicates(subset=['booking_id'])

    def remove_outliers(self):
        numeric_cols = self.dfUber.select_dtypes(include=["int64", "float64"]).columns

        for col in numeric_cols:
            Q1 = self.dfUber[col].quantile(0.25)
            Q3 = self.dfUber[col].quantile(0.75)
            IQR = Q3 - Q1
            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR

            mean_val = self.dfUber[col].mean()

            # detectar outliers
            outlier_mask = (self.dfUber[col] < lower) | (self.dfUber[col] > upper)
            outliers = outlier_mask.sum()
            print(f"Outliers detectados en {col}: {outliers}")

            # reemplazar outliers por la media
            self.dfUber.loc[outlier_mask, col] = mean_val


    def transform(self):
        self.standardize()
        self.clean_dates()
        self.duplicates()
        self.remove_outliers()
        pass

    def load(self):
        self.dfUber.to_csv('clean_dataset.csv', index=False)

    def business_metrics(self):
        total_income = self.dfUber["booking_value"].sum()
        avg_distance = self.dfUber["ride_distance"].mean()

        cancellation_rate = (
            self.dfUber["booking_status"]
            .astype(str)      
            .str.lower()
            .str.contains("cancel", na=False) 
            .mean()
        )

        print("Métricas del negocio:")
        print(f" - Total de ingresos: {total_income:,.2f}")
        print(f" - Distancia promedio: {avg_distance:.2f}")
        print(f" - Tasa de cancelación: {cancellation_rate:.2%}")
