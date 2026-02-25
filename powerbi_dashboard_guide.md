# Power BI Dashboard Guide — Annie's Magic Numbers

> **Source tables:** All `gold.*` tables from the Databricks Medallion pipeline  
> **Connection:** Databricks Partner Connect → Power BI → Select `gold.*` tables  
> **Report:** 2 pages — Executive Profitability Summary + Operational Deep-Dive

---

## How to Connect Power BI to Databricks

1. In Databricks: **Partner Connect → Power BI → Download .pbids file**
2. Open the `.pbids` file in Power BI Desktop — it pre-configures the connector
3. Authenticate with your Databricks PAT token or Azure AD credentials
4. In **Navigator**, select these Gold tables:
   - `gold.sales_enriched`
   - `gold.product_profitability`
   - `gold.brand_profitability`
   - `gold.loss_makers`
   - `gold.sales_by_store`
   - `gold.sales_time_series`
   - `gold.inventory_delta`
   - `gold.vendor_performance`
   - `gold.size_analysis`
   - `gold.classification_performance`
5. Click **Load** (use **DirectQuery** mode for live data; **Import** for best performance with static data)

---

## DAX Measures to Create

Before building visuals, create these measures in the **gold_product_profitability** or **gold_sales_enriched** table:

```dax
-- Total Revenue
Total Revenue = SUM(gold_sales_enriched[sales_dollars])

-- Total Profit
Total Profit = SUM(gold_sales_enriched[profit_dollars])

-- Overall Margin %
Overall Margin % = 
    DIVIDE(
        SUM(gold_sales_enriched[profit_dollars]),
        SUM(gold_sales_enriched[sales_dollars])
    ) * 100

-- Loss-Making SKU Count
Loss-Making SKUs = 
    COUNTROWS(
        FILTER(gold_product_profitability, gold_product_profitability[total_profit_dollars] < 0)
    )

-- Loss-Making Brands Count
Loss-Making Brands = 
    COUNTROWS(
        FILTER(gold_brand_profitability, gold_brand_profitability[total_profit_dollars] < 0)
    )

-- Total Units Sold
Total Units Sold = SUM(gold_sales_enriched[sales_quantity])

-- Total Volume (Liters)
Total Volume (L) = SUM(gold_sales_enriched[volume])

-- Month-over-Month Profit Change %
MoM Profit % = 
    VAR CurrentMonthProfit = SUM(gold_sales_time_series[monthly_profit])
    VAR PrevMonthProfit = CALCULATE(
        SUM(gold_sales_time_series[monthly_profit]),
        DATEADD(gold_sales_time_series[sale_month], -1, MONTH)
    )
    RETURN DIVIDE(CurrentMonthProfit - PrevMonthProfit, ABS(PrevMonthProfit)) * 100

-- Profit per Liter
Profit per Liter = DIVIDE([Total Profit], [Total Volume (L)])

-- Top-N Product Label (for dynamic Top N)
Top Product Rank = MIN(gold_product_profitability[rank_by_profit])
```

---

## Page 1 — Executive Profitability Summary

> **Goal:** Give Annie a one-glance answer to "What makes us money and what doesn't?"

### Layout

```
┌────────────────────────────────────────────────────────────────────────┐
│  💰 $X.XM Revenue   📈 $X.XM Profit   % X.X% Margin   ⚠ XX Loss SKUs  │  ← KPI Row
├─────────────────────────────┬──────────────────────────────────────────┤
│  Top 10 Products by Profit  │  Top 10 Products by Margin %            │
│  (Horizontal Bar Chart)     │  (Horizontal Bar Chart)                  │
├─────────────────────────────┼──────────────────────────────────────────┤
│  Top 10 Brands by Profit $  │  Top 10 Brands by Margin %              │
│  (Horizontal Bar Chart)     │  (Horizontal Bar Chart)                  │
├─────────────────────────────┴──────────────────────────────────────────┤
│  🚨 Loss-Making Items Table                                             │
│  Brand | Product | Size | Revenue | Loss $ | Margin % | Recommendation │
├─────────────────────────────────────────────────────────────────────────┤
│  SLICERS: [Brand ▼] [Classification ▼] [Size ▼]                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Visual Specifications

#### 🔢 KPI Cards (Top Row — 4 cards)

| Card | Measure | Format | Color |
|---|---|---|---|
| Total Revenue | `[Total Revenue]` | `$#,##0.0,,M` | Blue |
| Total Profit | `[Total Profit]` | `$#,##0.0,,M` | Green |
| Overall Margin % | `[Overall Margin %]` | `#0.0%` | Teal |
| Loss-Making SKUs | `[Loss-Making SKUs]` | `#,##0` | Red |

#### 📊 Top 10 Products by Profit $ (Horizontal Bar Chart)

- **Table:** `gold_product_profitability`
- **Y-axis:** `description` (product name)
- **X-axis:** `total_profit_dollars`
- **Filter:** `rank_by_profit <= 10`
- **Sort:** Descending by `total_profit_dollars`
- **Tooltip:** `avg_margin_pct`, `total_units_sold`, `total_revenue`
- **Color:** Gradient from light to dark green (most profitable = darkest)
- **Data labels:** Show `$` value at bar end

#### 📊 Top 10 Products by Margin % (Horizontal Bar Chart)

- **Table:** `gold_product_profitability`
- **Y-axis:** `description`
- **X-axis:** `avg_margin_pct`
- **Filter:** `rank_by_margin <= 10` AND `total_units_sold > 100` (exclude low-volume flukes)
- **Sort:** Descending
- **Tooltip:** `total_profit_dollars`, `total_units_sold`
- **Color:** Blue gradient

#### 📊 Top 10 Brands by Profit $ (Horizontal Bar Chart)

- **Table:** `gold_brand_profitability`
- **Y-axis:** `brand` (or use lookup for brand name if available)
- **X-axis:** `total_profit_dollars`
- **Filter:** `rank_by_profit <= 10`
- **Tooltip:** `avg_margin_pct`, `sku_count`, `total_units_sold`

#### 📊 Top 10 Brands by Margin % (Horizontal Bar Chart)

- **Table:** `gold_brand_profitability`
- **Y-axis:** `brand`
- **X-axis:** `avg_margin_pct`
- **Filter:** `rank_by_margin <= 10` AND `total_units_sold > 500`

#### 🚨 Loss-Making Items Table

- **Table:** `gold_loss_makers`
- **Columns:** `level`, `brand_id`, `description`, `size`, `total_revenue`, `total_profit_dollars`, `avg_margin_pct`, `total_units_sold`, `stores_stocking`, `recommendation`
- **Conditional formatting:**
  - `total_profit_dollars` → Red background scale (worst loss = darkest red)
  - `avg_margin_pct` → Red font if < 0
- **Sort:** `total_profit_dollars` ascending (worst first)
- **Filter:** Cross-filters with Slicers

#### 🎛 Slicers

| Slicer | Field | Type |
|---|---|---|
| Brand | `gold_product_profitability[brand]` | Dropdown |
| Classification | `gold_classification_performance[classification]` | Dropdown |
| Size | `gold_size_analysis[size]` | Dropdown |

---

## Page 2 — Operational Deep-Dive

> **Goal:** Show Annie the operational patterns — trends, geography, vendors, inventory health.

### Layout

```
┌────────────────────────────────────────────────────────────────────────┐
│  Monthly Profit Trend 2016 (Line + Bar Combo)                          │  ← Full width
├────────────────────┬───────────────────────┬───────────────────────────┤
│  Revenue by Spirit │   Map: Profit by City  │  Profit Margin Scatter   │
│  Classification    │                        │  (Margin % vs Profit $)  │
│  (Treemap)         │                        │                          │
├────────────────────┴───────────────────────┴───────────────────────────┤
│  Profit by Bottle Size     │  Top 10 Vendors by Spend                  │
│  (Column Chart)            │  (Horizontal Bar)                         │
├────────────────────────────┴───────────────────────────────────────────┤
│  Revenue vs Profit by Top 15 Brands (Clustered Bar)                    │
├─────────────────────────────────────────────────────────────────────────┤
│  Inventory Health Table                                                  │
│  Product | Brand | Beg Qty | End Qty | Δ Change | Status               │
├─────────────────────────────────────────────────────────────────────────┤
│  SLICERS: [Month ▼] [City / Store ▼] [Vendor ▼]                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Visual Specifications

#### 📈 Monthly Profit Trend (Combo Chart — Line + Clustered Column)

- **Table:** `gold_sales_time_series`
- **X-axis:** `sale_month_name` (sort by `sale_month` number)
- **Column (bars):** `monthly_revenue` (light blue)
- **Line:** `monthly_profit` (green, secondary Y-axis)
- **Secondary line:** `avg_margin_pct` (orange dashed, secondary Y-axis)
- **Tooltip:** `total_units_sold`, `active_brands`, `transaction_count`
- **Title:** "Monthly Revenue & Profit Trend — Full Year 2016"

#### 🌳 Revenue by Spirit Classification (Treemap)

- **Table:** `gold_classification_performance`
- **Group:** `classification`
- **Size:** `total_revenue`
- **Color:** `overall_margin_pct` (gradient: low margin = red, high = green)
- **Tooltip:** `total_profit_dollars`, `unique_skus`, `total_units_sold`

#### 🗺 Map: Profit by City/Store

- **Table:** `gold_sales_by_store` (join with `gold_beg_inventory` for city names)
- **Location:** `store` (configure Geocoding or add city column)
- **Bubble size:** `total_revenue`
- **Bubble color:** `avg_margin_pct`
- **Tooltip:** `total_profit_dollars`, `unique_brands`, `transaction_count`

> **Note:** If store numbers don't map to cities automatically, create a manual lookup table in Power BI with city names.

#### 💠 Scatter Plot: Margin % vs Profit $ (Product-Level)

- **Table:** `gold_product_profitability`
- **X-axis:** `avg_margin_pct`
- **Y-axis:** `total_profit_dollars`
- **Bubble size:** `total_units_sold`
- **Color:** `is_loss_maker` (True = Red, False = Blue)
- **Tooltip:** `description`, `brand`, `size`, `total_revenue`
- **Reference lines:**
  - Vertical: `avg_margin_pct = 0` (zero margin line)
  - Horizontal: `total_profit_dollars = 0` (zero profit line)
- **Quadrant interpretation:**
  - Top-right: Stars (high margin + high profit)
  - Bottom-left: Dogs (low margin + low profit — loss-maker candidates)

#### 📦 Profit by Bottle Size (Column Chart)

- **Table:** `gold_size_analysis`
- **X-axis:** `size`
- **Y-axis:** `total_profit_dollars`
- **Sort:** Descending by profit
- **Color:** `avg_margin_pct` gradient
- **Tooltip:** `total_units_sold`, `unique_skus`, `avg_selling_price`

#### 🏭 Top 10 Vendors by Purchase Spend (Horizontal Bar)

- **Table:** `gold_vendor_performance`
- **Y-axis:** `vendor_name`
- **X-axis:** `total_purchase_spend`
- **Filter:** Top 10 by `total_purchase_spend`
- **Tooltip:** `brands_supplied`, `avg_cost_per_unit`, `avg_lead_time_days`, `total_po_count`
- **Secondary bar:** `avg_lead_time_days` (helps identify slow suppliers)

#### 📊 Revenue vs Profit: Top 15 Brands (Clustered Bar)

- **Table:** `gold_brand_profitability`
- **Y-axis:** `brand`
- **Filter:** Top 15 by `total_revenue`
- **Bar 1 (blue):** `total_revenue`
- **Bar 2 (green/red):** `total_profit_dollars`
- **Conditional color:** Profit bar red if < 0
- **Sort:** `total_revenue` descending

#### 🗃 Inventory Health Table

- **Table:** `gold_inventory_delta`
- **Columns:** `brand`, `description`, `size`, `vendor_name`, `beg_on_hand`, `end_on_hand`, `inventory_change`, `inventory_value_change`, `stock_status`
- **Conditional formatting:**
  - `stock_status = OVERSTOCKED` → Yellow row
  - `stock_status = DEPLETED` → Orange row
- **Filter:** Show only `OVERSTOCKED` or `DEPLETED` items (hide STABLE)
- **Sort:** `inventory_change` descending (most overstocked items first)

#### 🎛 Slicers (Page 2)

| Slicer | Field | Type |
|---|---|---|
| Month | `gold_sales_time_series[sale_month_name]` | List (checkboxes) |
| Store | `gold_sales_by_store[store]` | Dropdown |
| Vendor | `gold_vendor_performance[vendor_name]` | Search dropdown |

---

## Report Design Tips

### Consistent Color Palette

| Meaning | Color |
|---|---|
| Revenue / Positive | `#1F77B4` (Steel Blue) |
| Profit / Good | `#2CA02C` (Forest Green) |
| Loss / Warning | `#D62728` (Alert Red) |
| Neutral / Secondary | `#7F7F7F` (Gray) |
| Margin % | `#FF7F0E` (Orange) |

### Report-Level Filters

Apply these as **Page Filters** on Page 1:
```
gold_product_profitability[total_units_sold] > 0   (exclude phantom inventory)
gold_sales_enriched[sale_year] = 2016              (scope to available data)
```

### Cross-Page Drillthrough

Configure **Drillthrough** from Page 1 → Page 2:
- Right-click on a Brand in the Top 10 Brands chart
- "Drillthrough → Operational Deep-Dive"
- Page 2 auto-filters to that brand's data

### Performance Mode

Use **Import mode** instead of DirectQuery for dashboards shared with many users — schedule a daily refresh at 6:00 AM after the Databricks pipeline completes.

---

## Sharing the Report

1. Publish from Power BI Desktop → Power BI Service
2. Create a **Workspace**: "Annie's Dashboard"
3. Share with Annie's email or embed in Teams
4. Schedule **automatic data refresh** (Settings → Scheduled refresh → Daily 7:00 AM)
5. Configure **Row-Level Security** if store managers should only see their stores
