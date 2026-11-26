# Phase 1: Sales & Demand Intelligence
## Analytical Query Layer Documentation

### 1. Hospital-wise Monthly Demand Analysis
**Purpose:** Compare forecast medicine demand with actual hospital consumption and identify unfulfilled demand for each hospital by month.

**Key outputs:**
- Forecast demand packs
- Actual consumption packs
- Unfulfilled demand packs
- Demand variance
- Consumption versus forecast percentage

### 2. Forecast Accuracy and Request Fulfilment
**Purpose:** Evaluate forecast accuracy and hospital request fulfilment by year, hospital type and disease category.

**Key outputs:**
- Total forecast packs
- Actual consumption packs
- Forecast accuracy percentage
- Total requested packs
- Total received packs
- Request fulfilment rate percentage

### 3. Sales Channel Profitability
**Purpose:** Compare Physical Pharmacy and Ecommerce sales channels based on sales volume and profitability.

**Key outputs:**
- Total transactions
- Total packs sold
- Net sales amount (USD)
- Gross margin (USD)
- Gross margin percentage

### 4. Query Performance Results

Performance measured using PostgreSQL EXPLAIN ANALYZE.

| Query | Planning Time (ms) | Execution Time (ms) | Rows Returned |
|---|---:|---:|---:|
| Q1 - Hospital Monthly Demand | 0.788 | 26.861 | 456 |
| Q2 - Forecast Accuracy and Fulfilment | 1.207 | 39.655 | 112 |
| Q3 - Sales Channel Profitability | 0.806 | 236.696 | 2 |

### 5. Query Optimization Assessment

- PostgreSQL execution plans were inspected using EXPLAIN ANALYZE and BUFFERS.
- Existing indexes on the sales fact table were inspected.
- Query 3 uses a parallel sequential scan to aggregate the sales records.
- No additional index was created because the query aggregates the full sales table and does not filter by sales channel.
- The measured execution times are recorded as baseline performance results.

### 6. Supporting Files

- queries/query_01_hospital_monthly_demand.sql
- queries/query_02_forecast_accuracy_fulfilment.sql
- queries/query_03_sales_channel_profitability.sql
- reports/query_performance_baseline.csv
