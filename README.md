# AtliQ Hospitality — Enterprise Analytics Platform

AI-powered analytics platform for AtliQ Hotels with:
- 📊 Executive Dashboard with real-time KPIs
- 💬 Chat with Your Data (natural language queries)
- 🔍 KPI Monitoring & Anomaly Detection

## Architecture

- **Frontend:** Streamlit
- **Database:** Supabase (PostgreSQL)
- **AI Agent:** LiteLLM + Native Function Calling
- **Metrics Engine:** Deterministic SQL Builder (24 KPIs)

## Setup

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set environment variables (see below)
4. Run: `streamlit run frontend/dashboard.py`

## Environment Variables

| Variable | Description |
|---|---|
| `CLEAN_SUPABASE_DB_URI` | PostgreSQL connection string for clean database |
| `OPENROUTER_API_KEY` | OpenRouter API key for LLM access |

## Pages

| Page | Description |
|---|---|
| Dashboard | Executive KPI overview with filters |
| Chat with Data | Natural language data queries |
| KPI Monitoring | Automated anomaly detection & alerts |