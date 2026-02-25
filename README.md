# Voyage Financials (VFM) Analytics Dashboard 

A robust, enterprise-grade Streamlit application designed for deep-dive financial and operational analysis of cruise line voyages. Powered by **Snowflake (Snowpark)** and **Plotly**, this dashboard allows users to transform natural language prompts into actionable financial insights, track KPIs, detect anomalies, and visualize global fleet deployment.

## ✨ Key Features

* **🧠 NLP-Powered Querying:** Users can type natural language questions (e.g., *"Show product ranking by PCD and GSS asc"*). The app features custom spell-correction (Jaro-Winkler), synonym mapping, and entity extraction to automatically route users to the correct dashboard flow.
* **🔄 Multi-Dimensional Analysis Flows:**
* **General Overview:** High-level KPIs (Load Factor, Margin, PCD, PPD) and macro rankings.
* **Product-Centric:** Drill down from overall product performance to specific ship classes, ships, and individual financial components (Revenue/Cost).
* **Ship-Centric:** Analyze specific vessels, the products they carry, and their historical monthly trends.
* **Voyage-Centric & Itinerary Analysis:** Inspect individual voyage profitability and utilize clustering algorithms to find "Like-for-Like" voyages based on KPIs (GSS, LF, Seasonality).


* **🚨 Advanced Outlier Detection:** Uses Statistical models (Z-score and IQR) to flag anomalous financial components and account-level deviations across years and months.
* **🗺️ Geospatial & Heatmap Visualizations:** Interactive global deployment maps using `pydeck` and detailed multi-axis heatmaps to analyze seasonality and product activity.
* **📄 Automated HTML Reporting:** Automatically compiles dynamically generated insights and Plotly charts into a downloadable HTML report.
* **🔐 Secure Authentication:** Built-in user registration and login system with `bcrypt` password hashing, storing credentials securely in Snowflake.

## 🛠️ Tech Stack

* **Frontend / Framework:** [Streamlit](https://streamlit.io/)
* **Data Warehouse & Compute:** [Snowflake Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/index)
* **Visualizations:** [Plotly](https://plotly.com/python/) (Express, Graph Objects, Subplots), [Pydeck](https://deckgl.readthedocs.io/en/latest/)
* **Data Processing:** Pandas, NumPy, SciPy (Z-score)
* **NLP / Text Processing:** `textdistance`, `re`, `difflib`
* **Security:** `bcrypt`

## ⚙️ Prerequisites

Before running the application, ensure you have the following:

1. **Python 3.8+** installed.
2. A **Snowflake Account** with Snowpark enabled.
3. Appropriate Snowflake Role (`SYSADMINDEV` or `CR_VOSOA_APP_VIEWER` as defined in the code).
4. The specific database schema deployed in your Snowflake environment (`VESSOPS_D.L00_STG`).

## 🚀 Installation & Setup

1. **Clone the repository:**
```bash
git clone https://github.com/yourusername/voyage-financials-dashboard.git
cd voyage-financials-dashboard

```


2. **Install dependencies:**
Create a `requirements.txt` file with the necessary libraries (Streamlit, pandas, numpy, plotly, snowflake-snowpark-python, scipy, bcrypt, pydeck, pyyaml, textdistance) and run:
```bash
pip install -r requirements.txt

```


3. **Configure Snowflake Connection:**
Streamlit requires a `secrets.toml` file to connect to Snowflake. Create a folder named `.streamlit` in the root directory and add a `secrets.toml` file:
```toml
# .streamlit/secrets.toml
[connections.snowflake]
account = "your_account_identifier"
user = "your_username"
password = "your_password"
role = "your_role"
warehouse = "your_warehouse"
database = "VESSOPS_D"
schema = "L00_STG"

```


4. **Run the Application:**
```bash
streamlit run app.py

```



## 🏗️ Architecture & Data Sources

The application relies on several staging tables within the Snowflake `VESSOPS_D.L00_STG` schema:

* `VFM_STREAMLIT_DATA_BACKUP`: Core voyage-level financial data.
* `VFM_SNOW_M0_M1_BACKUP`: Granular account-level data.
* `VFM_RULES`: YAML configuration containing domain terms, metric mappings, and revenue/cost component definitions.
* `STREAMLIT_USER_TABLE`: Stores hashed user credentials.
* `STREAMLIT_QUERY_LOGS`: Tracks user queries for continuous NLP improvement.
* `VFM_IMAGE`: Stores base64 encoded images/logos used in the UI.

## 🧠 NLP Engine Configuration

The app reads a configuration YAML file directly from Snowflake upon startup. This file dictates how the app interprets user input. To modify keywords, synonyms, or focus areas (e.g., adding a new ship code), update the `YAML_CONTENT` in the `VFM_RULES` Snowflake table.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

[MIT License](https://www.google.com/search?q=LICENSE) (or whichever license is appropriate for your organization).
