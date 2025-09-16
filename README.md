# EcoTrend - Data Pipeline Project to Reveal US Economy Trend for the Past Five Years
## Summary
__EcoTrend__ is a cloud-based data engineering project leveraging a big data batch pipeline to track key U.S. economic indicators over the past five years. The automated pipeline handles data ingestion, transformation, and storage of U.S. economic data from the FRED API into optimized, structured tables. These tables power interactive dashboards that facilitate data-driven decision-making in finance, business planning, and policy development.
## Challenge and Approach
Monitoring key economic indicators—such as stock market trends, inflation, and housing prices—is essential for informed decision-making in finance, policy, and business. However, manually collecting this data from multiple sources can be time-consuming and inefficient. 

To resolve this issue, an automatic workflow was built to
1. Extract economic data from the __Federal Reserve Economic Data (FRED)__ Python API.
   
2. Ingesting API data with workflow orchestration tool __Kestra__. 

3. Store raw data in __GCS__ as a data lake.

4. Process data with __Apache Spark__ on __GCP Dataproc__.

5. Store structured, partitioned, and clustered data in __BigQuery__ as __data warehouse__ for analytics.

6. Visualize data through an interactive dashboard in __Looker Studio__.

The key economic indicators included:

- __Financial Markets:__ S&P 500 Index, 10-Year Treasury Yield, VIX (Volatility Index) also known as fear index
- __Interest Rates:__ Effective Federal Funds Rate
- __Inflation & Price Levels:__ Consumer Price Index (CPI-U)
- __Labor Market:__ Labor Force Participation Rate
- __Economic Activity:__ Industrial Production Index
- __Housing Market:__ House Price Index (Case-Shiller National Home Price Index)

<img width="723" height="274" alt="EcoTrend" src="https://github.com/user-attachments/assets/d71506a7-fb19-4a03-ac86-3dbc3316b1f0" />

## Visualization

- Date Range Controls
- Monthly Trending of Financial Market Signals
- Trending of Inflation and Housing Signals
- Pattern of Labor Market and Economic Activity Signals

<img width="723" height="374" alt="Dashboard" src="https://github.com/user-attachments/assets/c403ca46-5d12-4960-bb75-909e4cbdcda0" />

## Future Direction for Improvement

- Automate Spark job execution via Kestra
- Implement scheduled workflows for daily updates
- Expand dashboard visualizations with other metrics
  
## Resources
- FRED API for the source of economic data
- Google Cloud Platform (GCS, BigQuery, Dataproc)
- Kestra for workflow orchestration
- Apache Spark for scalable data transformations
- Looker Studio for visualization

