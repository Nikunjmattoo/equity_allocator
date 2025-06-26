# Equity Allocator

A financial data processing application for equity allocation analysis using NSE data.

## 🚀 Quick Setup

### Prerequisites
- Python 3.8+
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Nikunjmattoo/equity_allocator.git
   cd equity_allocator
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Configuration**
   
   Copy `.env` file as-is - it's already configured to use our shared Neon cloud database:
   ```
   DATABASE_URL=postgresql://neondb_owner:npg_xg...@ep-twilight-firefly...neon.tech/neondb
   ```
   
   ✅ **No additional setup needed** - the database is ready to use!

4. **Test Connection**
   ```bash
   python test_neon_connection.py
   ```

## 📊 Database

### Cloud Database (Neon)
- **Provider**: Neon (Serverless PostgreSQL)
- **Size**: 600MB+ with 10 tables
- **Tables**: balance_sheet, cash_flow, earnings, financials, fundamentals, price_history, recommendations, sustainability, tickers, data_contracts
- **Shared Access**: All team members use the same database

### Local Development
If you need local development database:
1. Uncomment the local DATABASE_URL in `.env`
2. Set up PostgreSQL locally
3. Import data using `database.bak`

## 🔧 Usage

### Load Stock Data
```bash
python yfinance_data_downloader.py
```

### Test Database Connection
```bash
python test_neon_connection.py
```

## 📁 Project Structure

```
equity_allocator/
├── computers/          # Data computation modules
├── data/              # Data files (15K+ files)
├── downloaders/       # Data download utilities
├── extractors/        # Data extraction tools
├── mapping_files/     # Symbol mappings
├── reports/           # Generated reports
├── db.py             # Database connection manager
├── yfinance_data_downloader.py  # Main data loader
└── test_neon_connection.py      # Database test utility
```

## 🤝 Team Collaboration

1. **Database**: Shared Neon cloud database - no local setup needed
2. **Git Workflow**: Standard git pull/push workflow
3. **Environment**: Each member uses the same `.env` configuration
4. **Data**: All data is synchronized via the shared database

## 🛠️ Development

- **Framework**: SQLAlchemy ORM
- **Database**: PostgreSQL (Neon Cloud)
- **Data Source**: Yahoo Finance API
- **Python**: 3.8+

## 📈 Features

- Direct database loading (bypasses CSV)
- NSE stock data processing
- Financial statements analysis
- Price history tracking
- Data completeness monitoring
- Rate-limited API calls