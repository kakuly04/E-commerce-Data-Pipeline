# E-commerce-Data-Pipeline
Building a complete data engineering pipeline that processes raw e-commerce data through multiple quality layers using Python and pandas. 

## 📌 Project Overview

A modular data pipeline that performs:

* Initial data ingestion and profiling
* Rule-based data validation
* Data cleansing and transformation
* Business-focused data curation
* Error logging and reporting

Designed for E-commerce datasets involving `orders` and `products`.

---

## 💪 Features

* Reads raw CSV data using paths defined in a `config.json` file
* Validates data using customizable rules (`not_null`, `primary_key`, `positive`, etc.)
* Cleanses and standardizes text, dates, and floats
* Merges and aggregates data to build product performance views
* Ranks products by total revenue
* Logs errors and pipeline progress using Python's `logging` module
* Saves cleansed and curated datasets to disk
* Automatically creates required folders if not present

---

## 📁 Project Structure

```
E-commerce-Data-Pipeline/
├── config.json
├── data_pipeline.py
├── requirements.txt
├── README.md
├── raw/
│   ├── orders.csv
│   └── products.csv
├── cleansed/
│   ├── clean_orders.csv
│   └── clean_products.csv
├── curated/
│   ├── product_performance_curated.csv
│   └── orders_curated.csv
├── logs/
│   ├── data_pipeline.log
│   ├── order_errors_error_records.csv
│   └── products_errors_error_records.csv
```

---

## ⚙️ How to Run

```bash
python data_pipeline.py
```

Ensure you're inside the project’s virtual environment, and that `config.json` exists.

---

## 📂 Sample `config.json`

```json
{
    "orders_path": "raw/order.csv",
    "products_path": "raw/products.csv",
    "log_path": "logs/data_pipeline.log",
    "orders_validation_rules": {
        "order_id": "primary_key",
        "order_date": "check_date_format", 
        "customer_id": "not_null",
        "product_id": "exists_in_products",
        "quantity": "positive",
        "unit_price": "positive", 
        "total_amount": "multiple_of_quantity_unit_price",
        "order_status": ["Pending", "Confirmed", "Cancelled", "Shipped", "Delivered"]
    },
    "products_validation_rules": {
        "product_id": "primary_key", 
        "product_name": "not_null",
        "category": "not_null",
        "price": "positive",
        "stock_quantity": "non_negative"
    }
}
```

---

## ✅ Validation Rules Supported

| Rule                              | Description                                                                |
| --------------------------------- | -------------------------------------------------------------------------- |
| `not_null`                        | Value must not be missing - NaN replaces with defaults                     |
| `primary_key`                     | Must be unique and not null                                                |
| `positive`                        | Must be > 0 - invalid values replaced with absolute value                  |
| `non_negative`                    | Must be ≥ 0 - invalid values replaced with absolute value                  |
| `check_date_format`               | Validates date format `%Y-%m-%d` - invalid date replaced with today's date |
| `multiple_of_quantity_unit_price` | Checks if `total_amount ≈ quantity × unit_price` (±2%)                     |
| `exists_in_products`              | Ensures `product_id` exists in products table                              |
| `[list of values]`                | Column must match one of the listed values (e.g., `order_status`)          |

---

## 📊 Output

* **Cleansed data**: `cleansed/`
* **Curated performance report**: `curated/`
* **Validation errors**: `logs/*.csv`
* **Pipeline execution logs**: `logs/data_pipeline.log`

---

## 🔍 Assumptions

* `order_id` and `product_id` are primary keys in their respective datasets
* `order_date` should be in `%Y-%m-%d` format - then converted to `%d-%m-%Y`
* `quantity`, `price`, and `unit_price` should be positive
* `stock_quantity` can be zero but not negative
* `total_amount` should be within 2% of `quantity × unit_price`
* Check Comments in the Code for other Assumptions
---

## 🔧 Requirements

* Python 3.7+
* pandas, numpy

To install dependencies:

```bash
pip install -r requirements.txt
```

---

## 🧠 Next Steps / Ideas

* Integrate with a database or cloud storage
