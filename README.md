# Synthetic Data Pipeline

This project is a Python-based synthetic data generator designed to create realistic business datasets for use in data engineering pipelines, testing, or demonstrations. It generates various dimension and fact tables and outputs them as CSV files.

## Project Structure
- `config/generators.py`: The main script that uses the `Faker` library to generate synthetic data for various entities.
- `config/utils.py`: Contains enumerations, choices, and configuration datasets (like product registries, location mappings, and pricing rules) used by the data generator.
- `data/raw/`: The destination directory where the generated CSV files are saved.
- `requirements.txt`: Python dependencies required to run the project.

## Generated Data
When the pipeline runs, it generates the following files in the `data/raw/` directory:
- `dim_product.csv`: Dimension table containing product details (SKUs, categories, brands).
- `dim_customer.csv`: Dimension table containing customer information, geographical data, and channels.
- `dim_gross_price.csv`: Fact/Dimension table tracking historical product pricing across multiple fiscal years.
- `fact_orders.csv`: Fact table recording individual sales transactions, referencing products and customers.

## Getting Started

### Prerequisites
Make sure you have Python 3 installed on your system.

### Setup
1. **Clone the repository or navigate to the project directory:**
   ```bash
   cd data_pipeline
   ```

2. **Set up a virtual environment (recommended):**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
   
   ```

3. **Install the required dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage
To generate the raw synthetic datasets, run the generator script from within the `config/` directory:

```bash
cd config
python generators.py
```

The script will output the progress to the console and create the `.csv` files inside the `../data/raw` folder.
