# IMPORTING NECESSARY LIBRARIES
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import re
from typing import List, Dict, Tuple

class DataPipeline:
    def __init__(self, config: Dict):
        self.config = config
        
        os.makedirs(os.path.dirname(config.get('log_path')), exist_ok=True) # Ensures log directory exists
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename = config.get('log_path'), level=logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
        
        self.orders_rules = config.get('orders_validation_rules', {})
        self.products_rules = config.get('products_validation_rules', {})
        self.order_path = config.get('orders_path')
        self.product_path = config.get('products_path')
        if not os.path.exists(self.order_path):
            self.logger.error(f"Order file not found at {self.order_path}")
            raise FileNotFoundError(f"Order file not found at {self.order_path}")
        if not os.path.exists(self.product_path):
            self.logger.error(f"Product file not found at {self.product_path}")
            raise FileNotFoundError(f"Product file not found at {self.product_path}")
        
    
    def read_raw_data(self) -> Dict[str, pd.DataFrame]:
        # Read raw data from CSV files and perform initial data profiling
        self.logger.info("Reading raw data from CSV files")
        try:
            orders_df = pd.read_csv(self.order_path)
            products_df = pd.read_csv(self.product_path)
            self.logger.info("Raw data read successfully")
            
            # Initial data profiling of Orders DataFrame
            if orders_df.empty:
                self.logger.warning("Orders DataFrame is empty")
            else:
                self.logger.info(f"Orders DataFrame Columns: {orders_df.columns.tolist()}")
                self.logger.info(f"Orders DataFrame Column DataTypes: {orders_df.dtypes.to_dict()}")
                self.logger.info(f"Orders DataFrame Missing Values: {orders_df.isnull().sum().to_dict()}")
                self.logger.info(f"Orders DataFrame Duplicates: {orders_df.duplicated().sum()}")
                self.logger.info(f"Orders DataFrame Unique Values:\n{orders_df.nunique().to_dict()}")
                self.logger.info(f"Orders DataFrame Head:\n{orders_df.head()}")
                self.logger.info(f"Orders DataFrame Shape: {orders_df.shape}")
                
                # Basic statistics for numerical columns
                self.logger.info(f"Orders DataFrame Basic Statistics:\n{orders_df.describe().transpose()}")
            
            # Initial data profiling of Products DataFrame
            if products_df.empty:
                self.logger.warning("Products DataFrame is empty")
            else:
                self.logger.info(f"Products DataFrame Columns: {products_df.columns.tolist()}")
                self.logger.info(f"Products DataFrame Column DataTypes: {products_df.dtypes.to_dict()}")
                self.logger.info(f"Products DataFrame Missing Values: {products_df.isnull().sum().to_dict()}")
                self.logger.info(f"Products DataFrame Duplicates: {products_df.duplicated().sum()}")
                self.logger.info(f"Products DataFrame Unique Values:\n{products_df.nunique().to_dict()}")
                self.logger.info(f"Products DataFrame Head:\n{products_df.head()}")
                self.logger.info(f"Products DataFrame Shape: {products_df.shape}")

                # Basic statistics for numerical columns
                self.logger.info(f"Products DataFrame Basic Statistics:\n{products_df.describe().transpose()}")
            
            return {'orders': orders_df, 'products': products_df}
        except Exception as e:
            self.logger.error(f"Error reading raw data: {e}")
            return {"orders": pd.DataFrame(), "products": pd.DataFrame()}


    def validate_data_quality(self, df: pd.DataFrame, rules: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        
        def validate_date_format(date_str: str) -> bool:
            self.logger.info("Validating data quality")
            try:
                pd.to_datetime(date_str, format="%Y-%m-%d", errors='raise')
                return True
            except (ValueError, TypeError):
                return False
        
        
        valid_records = df.copy()
        error_records = pd.DataFrame(columns=df.columns)
        for column, rule in rules.items():
            if column not in valid_records.columns:
                self.logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            if rule == "not_null":
                null_mask = valid_records[column].isnull()
                if null_mask.any():
                    error_df = valid_records[null_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Value is Null"
                    error_records = pd.concat([error_records, error_df])
                    valid_records = valid_records[~null_mask]
                    self.logger.warning(f"Null values found in column {column}")

            # Primary key validation should be unique and not null.
            elif rule == "primary_key":
                duplicates_mask = valid_records.duplicated(subset=[column], keep = 'first') | valid_records[column].isnull()
                if duplicates_mask.any():
                    error_df = valid_records[duplicates_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Duplicates of Primary key is not allowed"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove duplicates from valid records
                    valid_records = valid_records[~duplicates_mask]
                    self.logger.warning(f"Duplicate values found in primary key column {column}")
            
            # Handling NaN values in non-negative columns as well.
            elif rule == "positive":
                # valid_records[column] = valid_records[column].abs() --> Included this just in case we want to keep change the negative values to positive and keep them in valid records
                negative_mask = (valid_records[column] <= 0) | (valid_records[column].isna())
                if negative_mask.any():
                    error_df = valid_records[negative_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Value is not positive or is NaN"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove negative or zero values from valid records
                    valid_records = valid_records[~negative_mask]
                    self.logger.warning(f"Negative or zero values found in column {column}")
            
            # Handling NaN values in non-negative columns as well.
            elif rule == "non_negative":
                # valid_records[column] = valid_records[column].abs() --> Included this just in case we want to keep change the negative values to positive and keep them in valid records
                negative_mask = (valid_records[column] < 0) | (valid_records[column].isna())
                if negative_mask.any():
                    error_df = valid_records[negative_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Value is negative or is NaN"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove negative values from valid records
                    valid_records = valid_records[~negative_mask]
                    self.logger.warning(f"Negative values found in column {column}")
            
            elif rule == "check_date_format":
                invalid_date_mask = ~valid_records[column].apply(validate_date_format)
                if invalid_date_mask.any():
                    error_df = valid_records[invalid_date_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Invalid Date Format"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove invalid date format records from valid records
                    valid_records = valid_records[~invalid_date_mask]
                    self.logger.warning(f"Invalid date format found in column {column}")
            # This rule should also eliminate those order records in which price, quantity or amount are null. 
            elif rule == "multiple_of_quantity_unit_price": 
                if "quantity" in valid_records.columns and "unit_price" in valid_records.columns:
                    expected_amount = valid_records["quantity"] * valid_records["unit_price"]
                    invalid_mask = ~np.isclose(valid_records[column], expected_amount, rtol=0.02)
                    
                    if invalid_mask.any():
                        error_df = valid_records[invalid_mask].copy()
                        error_df["error_column"] = column
                        error_df["error_type"] = "Value is not a multiple of quantity and unit price"
                        error_records = pd.concat([error_records, error_df])
                        
                        # Remove invalid total_amount records from valid records
                        valid_records = valid_records[~invalid_mask]
                        self.logger.warning(f"Invalid total amount found in column {column}")

            elif isinstance(rule, list):
                invalid_mask = ~valid_records[column].isin(rule)
                if invalid_mask.any():
                    error_df = valid_records[invalid_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Invalid Status value"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove invalid records from valid records
                    valid_records = valid_records[~invalid_mask]
                    self.logger.warning(f"Invalid values found in column {column}: {valid_records[column][invalid_mask].unique()}")
            
            elif rule == "exists_in_products":
                validation_products_df = pd.read_csv(self.product_path)
                invalid_mask = ~valid_records[column].isin(validation_products_df['product_id'])
                if invalid_mask.any():
                    error_df = valid_records[invalid_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Product ID does not exist in Products table"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove invalid records from valid records
                    valid_records = valid_records[~invalid_mask]
                    self.logger.warning(f"Product IDs not found in products DataFrame: {valid_records[column][invalid_mask].unique()}")
        
        error_records = error_records.drop_duplicates(subset=error_records.columns.tolist())
        return valid_records, error_records
         

    def cleanse_data(self, df: pd.DataFrame) -> pd.DataFrame:

        # Trimming whitespace from string fields
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()
        
        # Rounding decimal places to 2 for not just prices but also for any float columns
        if df.select_dtypes(include='float').empty:
            self.logger.warning("No float columns found to round")
        else:
            df[df.select_dtypes(include='float').columns] = df.select_dtypes(include='float').round(2)

        for col in df.columns:
            # Standardizing text case (Title Case for names)
            if col == "product_name":
                df["product_name"] = df["product_name"].str.title()

            # Standardizing text case (Upper Case for status, order_id, product_id, customer_id, supplier_id)
            elif col == "order_status" or col == "product_id" or col == "order_id" or col == "customer_id" or col == "supplier_id":
                df[col] = df[col].str.upper()
        
            # Fixing common date format issues – Date Format should be DD-MM-YYYY
            elif col == "order_date":
                # Coercing should handle any invalid date formats (NaT format)
                df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce').dt.strftime('%d-%m-%Y')
        
        return df


    
    def curate_data(self, orders_cleansed: pd.DataFrame, products_cleansed: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        aggregated_orders = orders_cleansed.groupby('product_id').agg(
            total_orders=('order_id', 'count'),
            total_quantity_sold=('quantity', 'sum'),
            avg_order_quantity=('quantity', 'mean'),
            total_revenue=('total_amount', 'sum'),
            first_order_date=('order_date', 'min'),
            last_order_date=('order_date', 'max')
        ).reset_index()

    def log_errors(self, Table_name: str, error_records: pd.DataFrame):
        if error_records.empty:
            self.logger.info("No errors found in the data")
            return
        error_records.to_csv(f"logs/{Table_name}_error_records.csv", index=False)
        self.logger.warning(f"Logged {len(error_records)} erroneous records to logs/{Table_name}_error_records.csv")

    def run_pipeline(self):
        self.logger.info("Starting data pipeline execution")
        
        # Step 1: Reading raw data files
        raw_data = self.read_raw_data()
        if not raw_data['orders'].empty and not raw_data['products'].empty:
            self.logger.info("Raw data loaded successfully")
        else:
            self.logger.error("Failed to load raw data")
            return
        orders_df = raw_data['orders']
        products_df = raw_data['products']

        # Step 2: Data Quality Validation
        self.logger.info("Starting data quality validation")
        self.logger.info("# Cleaning products data first because products are referenced in orders (Product ID in orders must exist in products)")
        # Cleaning products data first because products are referenced in orders (Product ID in orders must exist in products)
        valid_products_df, product_errors_df = self.validate_data_quality(products_df, self.products_rules) 
        valid_orders_df, order_errors_df = self.validate_data_quality(orders_df, self.orders_rules)
        

        self.log_errors("order_errors", order_errors_df)
        self.log_errors("products_errors", product_errors_df)
        if not valid_orders_df.empty and not valid_products_df.empty:
            self.logger.info("Data validation completed successfully")
        else:
            self.logger.error("Data validation failed")
            return
        
        # Step 3: Cleansing Data
        self.logger.info("Starting data cleansing")
        clean_orders = self.cleanse_data(valid_orders_df)
        clean_products = self.cleanse_data(valid_products_df)
        clean_orders.to_csv("cleansed/clean_orders.csv", index=False)
        clean_products.to_csv("cleansed/clean_products.csv", index=False)
        



config = {
    "orders_path": "raw/order.csv",
    "products_path": "raw/products.csv",
    'log_path': "logs/data_pipeline.log",
    "orders_validation_rules": {
        "order_id": "primary_key", # Assumings order_id is the primary key. Hence, it cannot be null and must be unique
        "order_date": "check_date_format", # Checking if order_date is in YYYY-MM-DD format
        "customer_id": "not_null",
        "product_id": "not_null", # Assuming product_id also cannot be null - (An order must have a product)
        "product_id": "exists_in_products",
        "quantity": "positive", # Assuming quantity cannot be zero
        "unit_price": "positive", # Assuming price cannot be zero
        "total_amount": "multiple_of_quantity_unit_price",
        "order_status": ["Pending", "Confirmed", "Cancelled", "Shipped", "Delivered","pending", "confirmed", "cancelled", "shipped", "delivered"] # Assuming these are the only valid statuses
    },
    "products_validation_rules": {
        "product_id": "primary_key", #product_id is the primary key. Hence, it cannot be null and must be unique
        "product_name": "not_null",
        "category": "not_null",
        "price": "positive",
        "stock_quantity": "non_negative" # Assuming stock quantity cannot be negative, can be zero
    }
}

pipeline = DataPipeline(config)
pipeline.run_pipeline()