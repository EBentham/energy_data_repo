ELT pipleine for UK & European Power Systems Data. Data is saved into "silver" Parquet files for use in ML models (GAM for energy demand forecasting).

For ENTSOE:
- day_ahead_prices
- generation_per_type
- total_load

For Elexon:
- Actual Aggregated Generation Per Type (AGPT / B1620) [https://data.elexon.co.uk/bmrs/api/v1/datasets/AGPT?publishDateTimeFrom=2023-07-18%2018%3A00&publishDateTimeTo=2023-07-21%2009%3A00&format=json
- Actual Total Load (B1610) [https://data.elexon.co.uk/bmrs/api/v1/demand/actual/total?from=2023-07-18&to=2023-07-21&settlementPeriodFrom=36&settlementPeriodTo=12&format=json

<img width="2738" height="1253" alt="Blank diagram" src="https://github.com/user-attachments/assets/de1eb228-fd28-4cbc-9eec-fbd6a971725a" />

