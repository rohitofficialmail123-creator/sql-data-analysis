# SQL Data Analysis Portfolio

**5 SQL queries** written for operations leadership to support metric tracking, reimbursement auditing, defect analysis, and regional queue monitoring.

These queries were built against a data warehouse (Redshift-compatible) to answer real business questions from operations managers and leadership — replacing manual data pulls and enabling data-driven decision making.

---

## Queries Overview

| # | Query | Purpose | Key SQL Techniques |
|---|-------|---------|-------------------|
| 1 | [Shipment Reimbursement Analysis](./shipment_reimbursement_analysis.sql) | Audit flagged shipments for missing units and reimbursement amounts | LEFT JOIN with aggregated subquery, CASE inside MAX(), COALESCE |
| 2 | [Frontline Resolve Metrics](./frontline_resolve_metrics.sql) | Monthly resolution data across 4 regional/stage segments (NA frontline, NA final review, EU frontline, EU final review) | INNER JOIN, date range filtering, queue-based regional segmentation |
| 3 | [Queue Transfer Tracking](./queue_transfer_tracking.sql) | Monitor incoming volume and transfer timing to newly established regional queues | convert_timezone() for cross-timezone reporting, event log JOIN |
| 4 | [Root Cause Defect Analysis](./root_cause_defect_analysis.sql) | Analyze defect patterns and reopen rates by root cause category | CTEs, multi-criteria filtering, ORDER BY with NULLS LAST |
| 5 | [Shipment Reimbursement Summary](./shipment_reimbursement_summary.sql) | Roll up case-level data to shipment level with aggregated metrics | 4-CTE pipeline, LISTAGG, COUNT(DISTINCT), SUM, multiple LEFT JOINs |

---

## SQL Techniques Demonstrated

### Joins & Subqueries
- INNER JOIN for strict matching (case ↔ decision data)
- LEFT JOIN for preserving all records with optional enrichment
- LEFT JOIN with pre-aggregated subquery to avoid duplicates
- Multi-table joins (up to 5 tables in a single query)

### Aggregation & Grouping
- `COUNT(DISTINCT)` for accurate counting across duplicates
- `SUM`, `MIN`, `MAX` for metric computation
- `LISTAGG ... WITHIN GROUP` for string aggregation (Redshift/Oracle)
- `GROUP BY` with computed and joined columns

### CTEs (Common Table Expressions)
- Single CTE for filtering logic separation
- Multi-CTE pipelines (4 CTEs → final assembly) for modular, readable queries

### Data Quality & Null Handling
- `COALESCE` for fallback values across joined tables
- `CASE` expressions for conditional aggregation
- `NULLS LAST` for clean sorting with nullable columns
- Exclusion filters for incomplete records

### Date & Timezone
- `BETWEEN` for date range filtering
- `convert_timezone()` for cross-timezone reporting (UTC → IST, PST → IST)

### Regional Segmentation
- Queue-based filtering for multi-marketplace analysis (US, CA, UK, DE, FR, IT, ES, MX, BR, AU, SG)
- Parameterized date ranges for monthly reporting cadence

---

## Database Environment

- **Engine:** Redshift (PostgreSQL-compatible)
- **Query patterns:** Analytical / reporting (read-only)
- **Data sources:** Case management system, decision/reimbursement tables, event logs

---

## Project Structure

```
sql_data_analysis/
├── README.md
├── shipment_reimbursement_analysis.sql    — Batch shipment audit
├── frontline_resolve_metrics.sql          — 4 regional resolution variants
├── queue_transfer_tracking.sql            — New queue volume monitoring
├── root_cause_defect_analysis.sql         — Defect pattern analysis
└── shipment_reimbursement_summary.sql     — Multi-CTE shipment rollup
```

---

## Author

Data Analysis & SQL | Redshift, Python, ETL Automation

---

## License

MIT
