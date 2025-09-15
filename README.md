# EcoTrend - Data Pipeline Project to Demonstrate US Economy Trend for the Past Five Years
## Summary
EcoTrend is a cloud data engineering project using big data btach pipeline to track key US economy indicators in the past five years. The pipeline automation implemented data ingestion, transfermation, and storage of US economy data from FRED API to optimized structured table used to build interactive dashboard, facilitaing the decision making on finance, business planning and policies.
## Challenge and Approach
Monitoring key economic indicators—such as stock market trends, inflation, and housing prices—is essential for informed decision-making in finance, policy, and business. However, manually collecting this data from multiple sources can be time-consuming and inefficient. 

To resolve this issue, an automatic workflow was build to
1. Extract economic data from the Federal Reserve Economic Data (FRED) Python API.

2. Store raw data in GCS as a data lake.

3. Process data with Apache Spark on GCP Dataproc.

4. Store structured, partitioned, and clustered data in BigQuery as data warehouse for analytics.

5. Visualize data through an interactive dashboard in Looker Studio.

The key economic indicators included:

- Financial Markets: S&P 500 Index, 10-Year Treasury Yield, VIX (Volatility Index) also known as fear index
- Interest Rates: Effective Federal Funds Rate
- Inflation & Price Levels: Consumer Price Index (CPI-U)
- Labor Market: Labor Force Participation Rate
- Economic Activity: Industrial Production Index
- Housing Market: House Price Index (Case-Shiller National Home Price Index)

<img width="723" height="274" alt="EcoTrend" src="https://github.com/user-attachments/assets/d71506a7-fb19-4a03-ac86-3dbc3316b1f0" />

## Resources:
- FRED API for the source of economic data
- Google Cloud Platform (GCS, BigQuery, Dataproc)
- Kestra for workflow orchestration
- Apache Spark for scalable data transformations
- Looker Studio for visualization

