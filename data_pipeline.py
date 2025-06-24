# IMPORTING NECESSARY LIBRARIES
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import re
from typing import List, Dict, Tuple
import json

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
            
            # Replacing null values with default values.
            if rule == "not_null":
                null_mask = valid_records[column].isnull()
                if column == "customer_id":
                    valid_records.loc[null_mask, column] = "CUSTXXX"  # Assigning a default value for customer_id if null
                elif column == "supplier_id":
                    valid_records.loc[null_mask, column] = "SUPXXX"
                elif column == "product_name":
                    valid_records.loc[null_mask, column] = "Unknown Product"
                elif column == "category":
                    valid_records.loc[null_mask, column] = "Uncategorized"
                if null_mask.any():
                    error_df = valid_records[null_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Value is null. Default value added temporarily"
                    error_records = pd.concat([error_records, error_df])
                    
                    self.logger.warning(f"Null values found in column {column}")


            # Primary key validation should be unique and not NaN.
            elif rule == "primary_key":
                duplicates_mask = valid_records.duplicated(subset=[column], keep = 'first') | valid_records[column].isnull()
                if duplicates_mask.any():
                    error_df = valid_records[duplicates_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Duplicates of Primary key or NaN is not allowed"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove duplicates from valid records
                    valid_records = valid_records[~duplicates_mask]
                    self.logger.warning(f"Rows with NaN values in primary key column - {column} are removed")
                    self.logger.warning(f"Duplicate and NaN values found in primary key column {column}")
            
            # Handling NaN values in non-negative columns as well.
            elif rule == "positive":
                # valid_records[column] = valid_records[column].abs() --> Included this just in case we want to keep change the negative values to positive and keep them in valid records
                negative_mask = (valid_records[column] <= 0) | (valid_records[column].isna())
                if negative_mask.any():
                    error_df = valid_records[negative_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Value is not positive or is NaN. Default value added temporarily"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Using the absolute value to keep the positive values in valid records
                    
                    valid_records[column] = valid_records[column].abs()
                    # NaN values are retained as is, but if we want to replace them with 0, we can uncomment the next line
                    # df.loc[valid_records[column].isnull(), column] = 0
                    self.logger.warning(f"Negative or zero values found in column {column}")
            
            # Handling NaN values in non-negative columns as well.
            elif rule == "non_negative":
                # valid_records[column] = valid_records[column].abs() --> Included this just in case we want to keep change the negative values to positive and keep them in valid records
                negative_mask = (valid_records[column] < 0) | (valid_records[column].isna())
                if negative_mask.any():
                    error_df = valid_records[negative_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Value is negative or is NaN. Default value added temporarily"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Using the absolute value to keep the positive values in valid records
                    # Replacing NaN with 0 (Making assumption that 0 is a valid value for these columns)
                    valid_records[column] = valid_records[column].abs()
                    valid_records.loc[valid_records[column].isnull(), column] = 0
                    self.logger.warning(f"Negative values found in column {column}")
            
            elif rule == "check_date_format":
                valid_records[column] = valid_records[column].abs()
                invalid_date_mask = ~valid_records[column].apply(validate_date_format)
                if invalid_date_mask.any():
                    error_df = valid_records[invalid_date_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Invalid Date Format, defaulting to today's date temporarily"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Replacing invalid dates with today's date
                    # Assuming today's date is the desired replacement for invalid dates
                    today_str = pd.to_datetime("today").strftime("%Y-%m-%d")  # or your desired format
                    valid_records.loc[invalid_date_mask, column] = today_str

                    self.logger.warning(f"Invalid date format found in column {column}; replaced with today's date")

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
                title_cased_values = valid_records[column].astype(str).str.title()
                invalid_mask = ~title_cased_values.isin(rule)
                if invalid_mask.any():
                    error_df = valid_records[invalid_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Invalid Status value"
                    error_records = pd.concat([error_records, error_df])
                    
                    # Remove invalid records from valid records
                    valid_records = valid_records[~invalid_mask]
                    self.logger.warning(f"Invalid values found in column {column}: {valid_records[column][invalid_mask].unique()}")
            
            # This rule should also eliminate those order records in which product_id is null.
            elif rule == "exists_in_products":
                validation_products_df = pd.read_csv(self.product_path)
                invalid_mask = ~valid_records[column].isin(validation_products_df['product_id'])
                if invalid_mask.any():
                    error_df = valid_records[invalid_mask].copy()
                    error_df["error_column"] = column
                    error_df["error_type"] = "Product ID does not exist in Products table"
                    error_records = pd.concat([error_records, error_df])
                    self.logger.warning(f"Product IDs not found in products DataFrame: {valid_records[column][invalid_mask].unique()}")

                    # Remove invalid records from valid records
                    valid_records = valid_records[~invalid_mask]
                    
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
                df["order_date"] = pd.to_datetime(df["order_date"], format = "%d-%m-%Y", errors = 'coerce', dayfirst = True)

        return df


    
    def curate_data(self, orders_cleansed: pd.DataFrame, products_cleansed: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        self.logger
        aggregated_orders = orders_cleansed.groupby('product_id').agg(
            total_orders = ('order_id', 'count'),
            total_quantity_sold = ('quantity', 'sum'),
            avg_order_quantity = ('quantity', 'mean'),
            total_revenue = ('total_amount', 'sum'),
            first_order_date = ('order_date', 'min'),
            last_order_date = ('order_date', 'max')
        ).reset_index()

        self.logger.info("Aggregated orders data successfully")
        # Performing the COALESCE operation to fill NaN values with 0 for total_orders, total_quantity_sold, avg_order_quantity, and total_revenue
        product_performance_raw = pd.merge(products_cleansed, aggregated_orders, on = 'product_id', how = 'left')
        product_performance_raw['total_orders'] = product_performance_raw['total_orders'].fillna(0).astype(int)
        product_performance_raw['total_quantity_sold'] = product_performance_raw['total_quantity_sold'].fillna(0).astype(int)
        product_performance_raw['avg_order_quantity'] = product_performance_raw['avg_order_quantity'].fillna(0).round(2)
        product_performance_raw['total_revenue'] = product_performance_raw['total_revenue'].fillna(0).round(2)

        product_performance_raw['last_order_date'] = pd.to_datetime(product_performance_raw['last_order_date'], errors='coerce')
        product_performance_raw['days_since_last_order'] = (pd.Timestamp.now().normalize() - product_performance_raw['last_order_date']).dt.days

        self.logger.info("Merged products and aggregated orders data successfully")
        self.logger.info("Orders Curated DataFrame Successfully Created")
        
        self.logger.info("Calculating performance metrics for products")
        self.logger.info("Calculating performance rank based on total revenue")
        product_performance = product_performance_raw.copy()
        # When using rank method min is used because SQL Rank function uses min method to assign the same rank to same values 
        product_performance['performance_rank'] = product_performance['total_revenue'].rank(ascending=False, method='min').astype(int)
        self.logger.info("Performance rank calculated successfully")
        return {"curated_data": product_performance, "orders_curated": product_performance_raw}

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
        self.logger.info("# Validating products data first because products are referenced in orders (Product ID in orders must exist in products)")
        # Cleaning products data first because products are referenced in orders (Product ID in orders must exist in products)
        valid_products_df, product_errors_df = self.validate_data_quality(products_df, self.products_rules) 
        self.logger.info("Starting data quality validation of Orders data")
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

        # Step 4: Curating Data
        self.logger.info("Starting data curation")
        curated_data = self.curate_data(clean_orders, clean_products)
        curated_data['curated_data'].to_csv("curated/product_performance_curated.csv", index=False)
        curated_data['orders_curated'].to_csv("curated/orders_curated.csv", index=False)
        self.logger.info("Data curation completed successfully")

        # Step 5: Final Logging
        self.logger.info("Data pipeline execution completed successfully")
        

# ---------------------------------------------------------------------------------------------------------
# Assumptions for the Configuration of the Data Pipeline
# Assumings order_id is the primary key. Hence, it cannot be null and must be unique
# Checking if order_date is in YYYY-MM-DD format
# Assuming product_id also cannot be null - (An order must have a product)
# Assuming quantity cannot be zero
# Assuming price cannot be zero
# product_id is the primary key. Hence, it cannot be null and must be unique
# Assuming stock quantity cannot be negative, can be zero
# ---------------------------------------------------------------------------------------------------------

def main():
    try:
        with open("config.json", "r") as f:
            # Loading configuration from a JSON file
            config = json.load(f)
            
    except FileNotFoundError:
        print("Config file not found: config.json Kindly ensure it exists in the working directory and re-run the script.")
        config = {}
        return
    except json.JSONDecodeError as e:
        print(f"Config file is not valid JSON: {e}. Kindly ensure it exists in the working directory and re-run the script.")
        config = {}
        return
    except Exception as e:
        print(f"Unexpected error while loading config: {e}. Kindly ensure it exists in the working directory and re-run the script.")
        config = {}
        return
    
    try:
        pipeline = DataPipeline(config)
        pipeline.run_pipeline()
    except Exception as e:
        logging.error(f"Error during pipeline execution: {e}", exc_info=True)
        print("An error occurred during pipeline execution. Please check the log file for details.")
    

if __name__ == "__main__":
    main()