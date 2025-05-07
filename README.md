# 🔍 FSSP Debt Checker - Automated Debt Verification System

[![Python 3.8+](https://img.shields.io/badge/Python-3.8+-blue?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Style: Ruff](https://img.shields.io/badge/Code%20Style-Ruff-red.svg)](https://github.com/astral-sh/ruff)

## 🚀 Key Features & Technical Highlights

✅ **High-Performance API Client**  
- Concurrent processing with `ThreadPoolExecutor` (Up to 20 simultaneous requests)  
- Comprehensive error handling (network, API limits, data validation)  

✅ **Professional-Grade Engineering**  
- Modern Python packaging (`pyproject.toml`)  
- Type hints throughout codebase  
- CI/CD-ready structure (tests, linting, formatting)  

✅ **Enterprise-Ready Outputs**  
- Excel report generation with `pandas`/`openpyxl`  
- Progress tracking with `tqdm` progress bar  
- Detailed logging with rotation and levels  

✅ **Resilience Features**  
- Graceful SIGINT handling for mid-process termination  
- Periodic auto-save to temporary files  
- Configurable timeouts and delays  

## 🛠️ Tech Stack Deep Dive

| Component          | Technology Used              | Why It Matters               |
|--------------------|------------------------------|------------------------------|
| **Core Logic**     | Python 3.8+                  | Maintainable, typed code     |
| **Concurrency**    | concurrent.futures           | 10-20x faster than sequential|
| **API Client**     | requests                     | Resilient network operations |
| **Data Processing**| pandas                       | Professional Excel output    |
| **Logging**        | logging + python-json-logger | Production-ready monitoring  |


## 📦 Installation & Setup
```bash
# Clone and setup
git clone https://github.com/svplaksin/fssp_api
cd fssp-api
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.\.venv\Scripts\activate  # Windows

pip install -e ".[dev]"  # Install with development tools

Configure your .env file:
API_TOKEN="your_fssp_token"


## 🚀 Quick Start

1. **Place your input file**:
   ```bash
   # Copy your Excel file to project root
   cp ~/Downloads/debtors.xlsx numbers.xlsx
2. **Run the check:
   ```bash
    debt-checker  # Processes numbers.xlsx automatically
3. **Get results:
Output saved to: numbers_with_debt.xlsx


## 💼 Professional Features
# Input File Requirements
- Must be named numbers.xlsx in project root
- Requires column: number (enforcement procedure numbers)
- Optional column: Debt Amount (for updating existing data)

## 📊 Sample Output
number	            Debt Amount
1234/56/7890-ИП	    15,200.50
9876/54/3210-ИП	    0.00


Output Includes
✅ Debt amounts

🏗️ Project Structure
fssp-api/
├── numbers.xlsx            # Input file (required in root)
├── numbers_with_debt.xlsx # Generated output file
├── debt_checker/          # Core package
│   ├── api_client.py      # API communication
│   ├── logging_config.py  # Custom logging setup
│   ├── main.py            # CLI interface
│   └── utils.py           # Concurrent processing
├── tests/                 # Unit tests
└── pyproject.toml         # Modern dependency config

## 🔬 Development Practices
# Run tests with coverage
pytest --cov --cov-report=html

# Format and lint code
ruff format . && ruff check .

## 📈 Performance Example
Processed 1,500 records in 5.5 minutes (20 threads)
Average request time: 320ms

📄 License
MIT © [svplaksin]:
FSSP Debt Checker | Python API Client | [https://github.com/svplaksin/fssp_api]
