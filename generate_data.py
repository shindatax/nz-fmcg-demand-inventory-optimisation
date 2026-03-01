"""
=============================================================================
NZ FMCG Demand Planning & Inventory Optimisation
Step 2: Synthetic Data Generation
=============================================================================
Author   : Shinyeong Kim
GitHub   : https://github.com/shindatax
LinkedIn : https://www.linkedin.com/in/shinyeong-kim-49b16b361
Context  : Generates realistic master + transactional data for a
           Store–DC–Supplier 3-tier NZ FMCG supply chain.
           Output CSVs are loaded into MySQL in Step 3.

Realistic patterns included:
  - NZ seasonality (Summer/Autumn/Winter/Spring)
  - Day-of-week effect (weekend uplift)
  - Promotion events (random + seasonal)
  - Store size variance (large metro vs small regional)
  - SKU-level demand volatility
  - Supplier lead time variability
  - Stockout events

Outputs (data/):
  dim_supplier.csv      dim_product.csv       dim_dc.csv
  dim_store.csv         dim_calendar.csv
  fact_sales_daily.csv  fact_inventory_store.csv
  fact_inventory_dc.csv fact_purchase_orders.csv
  fact_shipments.csv
=============================================================================
"""

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

np.random.seed(42)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

START_DATE = pd.Timestamp("2022-01-01")
END_DATE   = pd.Timestamp("2024-12-31")
DATES      = pd.date_range(START_DATE, END_DATE, freq="D")

print("=" * 60)
print("  NZ FMCG — Synthetic Data Generation")
print("=" * 60)


# =============================================================================
# 1. dim_supplier
# =============================================================================
print("\n[1/10] Generating dim_supplier ...")

suppliers = pd.DataFrame([
    {"supplier_id": "SUP001", "supplier_name": "Anchor Foods NZ",      "country": "New Zealand", "lead_time_days": 3,  "lead_time_stddev_days": 0.5, "reliability_score": 0.97, "min_order_qty": 50,  "is_active": 1},
    {"supplier_id": "SUP002", "supplier_name": "Goodman Fielder ANZ",  "country": "Australia",   "lead_time_days": 7,  "lead_time_stddev_days": 1.5, "reliability_score": 0.92, "min_order_qty": 100, "is_active": 1},
    {"supplier_id": "SUP003", "supplier_name": "McCain Foods NZ",      "country": "New Zealand", "lead_time_days": 4,  "lead_time_stddev_days": 0.8, "reliability_score": 0.95, "min_order_qty": 60,  "is_active": 1},
    {"supplier_id": "SUP004", "supplier_name": "Sanitarium Health NZ", "country": "New Zealand", "lead_time_days": 5,  "lead_time_stddev_days": 1.0, "reliability_score": 0.93, "min_order_qty": 80,  "is_active": 1},
    {"supplier_id": "SUP005", "supplier_name": "Simplot Australia",    "country": "Australia",   "lead_time_days": 10, "lead_time_stddev_days": 2.0, "reliability_score": 0.88, "min_order_qty": 120, "is_active": 1},
    {"supplier_id": "SUP006", "supplier_name": "Cerebos Greggs NZ",    "country": "New Zealand", "lead_time_days": 4,  "lead_time_stddev_days": 0.7, "reliability_score": 0.96, "min_order_qty": 50,  "is_active": 1},
])
suppliers.to_csv(DATA_DIR / "dim_supplier.csv", index=False)
print(f"    {len(suppliers)} suppliers saved.")


# =============================================================================
# 2. dim_product
# =============================================================================
print("\n[2/10] Generating dim_product ...")

products = pd.DataFrame([
    # Dairy
    {"product_id": "P001", "product_name": "Full Cream Milk 2L",       "category": "Dairy",   "subcategory": "Milk",    "brand": "Anchor",      "unit_of_measure": "EA", "unit_cost_nzd": 2.10, "unit_price_nzd": 3.49, "shelf_life_days": 14, "is_cold_chain": 1, "supplier_id": "SUP001", "base_demand_daily": 120, "demand_stddev": 25, "is_active": 1},
    {"product_id": "P002", "product_name": "Greek Yoghurt 500g",        "category": "Dairy",   "subcategory": "Yoghurt", "brand": "Anchor",      "unit_of_measure": "EA", "unit_cost_nzd": 2.80, "unit_price_nzd": 4.99, "shelf_life_days": 21, "is_cold_chain": 1, "supplier_id": "SUP001", "base_demand_daily": 60,  "demand_stddev": 18, "is_active": 1},
    {"product_id": "P003", "product_name": "Cheddar Cheese 500g",       "category": "Dairy",   "subcategory": "Cheese",  "brand": "Anchor",      "unit_of_measure": "EA", "unit_cost_nzd": 4.50, "unit_price_nzd": 7.99, "shelf_life_days": 60, "is_cold_chain": 1, "supplier_id": "SUP001", "base_demand_daily": 45,  "demand_stddev": 12, "is_active": 1},
    # Bakery
    {"product_id": "P004", "product_name": "White Bread 700g",          "category": "Bakery",  "subcategory": "Bread",   "brand": "Tip Top",     "unit_of_measure": "EA", "unit_cost_nzd": 1.40, "unit_price_nzd": 2.99, "shelf_life_days": 7,  "is_cold_chain": 0, "supplier_id": "SUP002", "base_demand_daily": 150, "demand_stddev": 30, "is_active": 1},
    {"product_id": "P005", "product_name": "Multigrain Bread 700g",     "category": "Bakery",  "subcategory": "Bread",   "brand": "Tip Top",     "unit_of_measure": "EA", "unit_cost_nzd": 1.80, "unit_price_nzd": 3.49, "shelf_life_days": 7,  "is_cold_chain": 0, "supplier_id": "SUP002", "base_demand_daily": 90,  "demand_stddev": 22, "is_active": 1},
    # Frozen
    {"product_id": "P006", "product_name": "Frozen Chips 1kg",          "category": "Frozen",  "subcategory": "Potato",  "brand": "McCain",      "unit_of_measure": "EA", "unit_cost_nzd": 2.60, "unit_price_nzd": 4.49, "shelf_life_days": 365,"is_cold_chain": 1, "supplier_id": "SUP003", "base_demand_daily": 80,  "demand_stddev": 20, "is_active": 1},
    {"product_id": "P007", "product_name": "Frozen Peas 750g",          "category": "Frozen",  "subcategory": "Veg",     "brand": "Wattie's",    "unit_of_measure": "EA", "unit_cost_nzd": 2.20, "unit_price_nzd": 3.79, "shelf_life_days": 365,"is_cold_chain": 1, "supplier_id": "SUP005", "base_demand_daily": 55,  "demand_stddev": 15, "is_active": 1},
    # Breakfast
    {"product_id": "P008", "product_name": "Weet-Bix 750g",             "category": "Breakfast","subcategory": "Cereal",  "brand": "Sanitarium",  "unit_of_measure": "EA", "unit_cost_nzd": 2.90, "unit_price_nzd": 4.99, "shelf_life_days": 180,"is_cold_chain": 0, "supplier_id": "SUP004", "base_demand_daily": 70,  "demand_stddev": 18, "is_active": 1},
    {"product_id": "P009", "product_name": "Up&Go Choc 250ml 3pk",      "category": "Breakfast","subcategory": "Drinks",  "brand": "Sanitarium",  "unit_of_measure": "EA", "unit_cost_nzd": 3.10, "unit_price_nzd": 5.49, "shelf_life_days": 180,"is_cold_chain": 0, "supplier_id": "SUP004", "base_demand_daily": 40,  "demand_stddev": 12, "is_active": 1},
    # Condiments
    {"product_id": "P010", "product_name": "Tomato Sauce 575g",         "category": "Condiments","subcategory": "Sauce",  "brand": "Wattie's",    "unit_of_measure": "EA", "unit_cost_nzd": 1.90, "unit_price_nzd": 3.49, "shelf_life_days": 730,"is_cold_chain": 0, "supplier_id": "SUP005", "base_demand_daily": 50,  "demand_stddev": 14, "is_active": 1},
    {"product_id": "P011", "product_name": "Instant Coffee 100g",       "category": "Beverages","subcategory": "Coffee",  "brand": "Greggs",      "unit_of_measure": "EA", "unit_cost_nzd": 4.20, "unit_price_nzd": 7.49, "shelf_life_days": 730,"is_cold_chain": 0, "supplier_id": "SUP006", "base_demand_daily": 35,  "demand_stddev": 10, "is_active": 1},
    {"product_id": "P012", "product_name": "Milo 400g",                 "category": "Beverages","subcategory": "Powder",  "brand": "Nestle",      "unit_of_measure": "EA", "unit_cost_nzd": 3.80, "unit_price_nzd": 6.99, "shelf_life_days": 730,"is_cold_chain": 0, "supplier_id": "SUP002", "base_demand_daily": 30,  "demand_stddev": 9,  "is_active": 1},
])
products.to_csv(DATA_DIR / "dim_product.csv", index=False)
print(f"    {len(products)} products saved.")


# =============================================================================
# 3. dim_dc
# =============================================================================
print("\n[3/10] Generating dim_dc ...")

dcs = pd.DataFrame([
    {"dc_id": "DC001", "dc_name": "Auckland DC",      "location": "Wiri, Auckland",     "region": "Auckland",    "capacity_pallets": 5000, "has_cold_storage": 1, "supplier_id": "SUP001", "is_active": 1},
    {"dc_id": "DC002", "dc_name": "Wellington DC",    "location": "Petone, Wellington", "region": "Wellington",  "capacity_pallets": 2500, "has_cold_storage": 1, "supplier_id": "SUP002", "is_active": 1},
    {"dc_id": "DC003", "dc_name": "Christchurch DC",  "location": "Rolleston, Canterbury","region": "Canterbury","capacity_pallets": 3000, "has_cold_storage": 1, "supplier_id": "SUP003", "is_active": 1},
])
dcs.to_csv(DATA_DIR / "dim_dc.csv", index=False)
print(f"    {len(dcs)} DCs saved.")


# =============================================================================
# 4. dim_store
# =============================================================================
print("\n[4/10] Generating dim_store ...")

stores = pd.DataFrame([
    # Auckland stores (served by DC001)
    {"store_id": "S001", "store_name": "Auckland CBD Super",    "store_format": "Supermarket",  "region": "Auckland",      "city": "Auckland",     "dc_id": "DC001", "avg_weekly_footfall": 12000, "size_multiplier": 1.5, "is_active": 1},
    {"store_id": "S002", "store_name": "Newmarket Metro",       "store_format": "Metro",        "region": "Auckland",      "city": "Auckland",     "dc_id": "DC001", "avg_weekly_footfall": 6000,  "size_multiplier": 0.8, "is_active": 1},
    {"store_id": "S003", "store_name": "Manukau Superstore",    "store_format": "Supermarket",  "region": "Auckland",      "city": "Manukau",      "dc_id": "DC001", "avg_weekly_footfall": 15000, "size_multiplier": 1.8, "is_active": 1},
    {"store_id": "S004", "store_name": "Henderson Supermarket", "store_format": "Supermarket",  "region": "Auckland",      "city": "Henderson",    "dc_id": "DC001", "avg_weekly_footfall": 9000,  "size_multiplier": 1.1, "is_active": 1},
    {"store_id": "S005", "store_name": "Pukekohe Express",      "store_format": "Convenience",  "region": "Auckland",      "city": "Pukekohe",     "dc_id": "DC001", "avg_weekly_footfall": 3000,  "size_multiplier": 0.5, "is_active": 1},
    # Wellington stores (served by DC002)
    {"store_id": "S006", "store_name": "Wellington CBD Super",  "store_format": "Supermarket",  "region": "Wellington",    "city": "Wellington",   "dc_id": "DC002", "avg_weekly_footfall": 10000, "size_multiplier": 1.3, "is_active": 1},
    {"store_id": "S007", "store_name": "Porirua Supermarket",   "store_format": "Supermarket",  "region": "Wellington",    "city": "Porirua",      "dc_id": "DC002", "avg_weekly_footfall": 8000,  "size_multiplier": 1.0, "is_active": 1},
    {"store_id": "S008", "store_name": "Lower Hutt Metro",      "store_format": "Metro",        "region": "Wellington",    "city": "Lower Hutt",   "dc_id": "DC002", "avg_weekly_footfall": 5000,  "size_multiplier": 0.7, "is_active": 1},
    # Christchurch stores (served by DC003)
    {"store_id": "S009", "store_name": "Christchurch CBD Super","store_format": "Supermarket",  "region": "Canterbury",    "city": "Christchurch", "dc_id": "DC003", "avg_weekly_footfall": 11000, "size_multiplier": 1.4, "is_active": 1},
    {"store_id": "S010", "store_name": "Riccarton Superstore",  "store_format": "Supermarket",  "region": "Canterbury",    "city": "Christchurch", "dc_id": "DC003", "avg_weekly_footfall": 13000, "size_multiplier": 1.6, "is_active": 1},
    {"store_id": "S011", "store_name": "Rangiora Express",      "store_format": "Convenience",  "region": "Canterbury",    "city": "Rangiora",     "dc_id": "DC003", "avg_weekly_footfall": 2500,  "size_multiplier": 0.4, "is_active": 1},
    {"store_id": "S012", "store_name": "Hamilton Supermarket",  "store_format": "Supermarket",  "region": "Waikato",       "city": "Hamilton",     "dc_id": "DC001", "avg_weekly_footfall": 9500,  "size_multiplier": 1.2, "is_active": 1},
])
stores.to_csv(DATA_DIR / "dim_store.csv", index=False)
print(f"    {len(stores)} stores saved.")


# =============================================================================
# 5. dim_calendar  (NZ public holidays + seasons)
# =============================================================================
print("\n[5/10] Generating dim_calendar ...")

def get_nz_season(month):
    if month in [12, 1, 2]:  return "Summer"
    elif month in [3, 4, 5]: return "Autumn"
    elif month in [6, 7, 8]: return "Winter"
    else:                     return "Spring"

# NZ public holidays (fixed dates only — Waitangi, ANZAC, Christmas, etc.)
nz_holidays = {
    # 2022
    "2022-01-03": "New Year (observed)", "2022-01-04": "Day after New Year",
    "2022-02-07": "Waitangi Day (observed)", "2022-04-15": "Good Friday",
    "2022-04-18": "Easter Monday", "2022-04-25": "ANZAC Day",
    "2022-06-06": "Queen's Birthday", "2022-10-24": "Labour Day",
    "2022-12-26": "Christmas Day", "2022-12-27": "Boxing Day",
    # 2023
    "2023-01-02": "New Year", "2023-01-03": "Day after New Year",
    "2023-02-06": "Waitangi Day", "2023-04-07": "Good Friday",
    "2023-04-10": "Easter Monday", "2023-04-25": "ANZAC Day",
    "2023-06-05": "King's Birthday", "2023-10-23": "Labour Day",
    "2023-12-25": "Christmas Day", "2023-12-26": "Boxing Day",
    # 2024
    "2024-01-01": "New Year", "2024-01-02": "Day after New Year",
    "2024-02-06": "Waitangi Day", "2024-03-29": "Good Friday",
    "2024-04-01": "Easter Monday", "2024-04-25": "ANZAC Day",
    "2024-06-03": "King's Birthday", "2024-10-28": "Labour Day",
    "2024-12-25": "Christmas Day", "2024-12-26": "Boxing Day",
}

calendar_rows = []
for d in DATES:
    ds = str(d.date())
    is_holiday = 1 if ds in nz_holidays else 0
    calendar_rows.append({
        "date_id":           d.date(),
        "year":              d.year,
        "quarter":           d.quarter,
        "month":             d.month,
        "month_name":        d.strftime("%B"),
        "week_of_year":      int(d.strftime("%W")),
        "day_of_week":       d.dayofweek + 1,   # 1=Mon … 7=Sun
        "day_name":          d.strftime("%A"),
        "is_weekend":        1 if d.dayofweek >= 5 else 0,
        "is_public_holiday": is_holiday,
        "holiday_name":      nz_holidays.get(ds, None),
        "nz_season":         get_nz_season(d.month),
    })

calendar = pd.DataFrame(calendar_rows)
calendar.to_csv(DATA_DIR / "dim_calendar.csv", index=False)
print(f"    {len(calendar)} days saved.")


# =============================================================================
# 6. fact_sales_daily
# =============================================================================
print("\n[6/10] Generating fact_sales_daily ...")

# Season multipliers (NZ context: dairy/frozen popular in winter)
SEASON_MULT = {
    "Dairy":     {"Summer": 0.90, "Autumn": 1.00, "Winter": 1.15, "Spring": 1.00},
    "Bakery":    {"Summer": 0.95, "Autumn": 1.00, "Winter": 1.10, "Spring": 1.00},
    "Frozen":    {"Summer": 0.85, "Autumn": 1.00, "Winter": 1.20, "Spring": 1.05},
    "Breakfast": {"Summer": 1.05, "Autumn": 1.00, "Winter": 0.95, "Spring": 1.00},
    "Beverages": {"Summer": 1.15, "Autumn": 1.00, "Winter": 0.85, "Spring": 1.00},
    "Condiments":{"Summer": 1.10, "Autumn": 1.00, "Winter": 0.95, "Spring": 1.00},
}

# Day-of-week multipliers (weekend uplift)
DOW_MULT = {1: 0.90, 2: 0.90, 3: 0.95, 4: 1.00, 5: 1.10, 6: 1.30, 7: 1.25}

# Promotion calendar (roughly 8 events/year)
promo_dates = set()
for yr in [2022, 2023, 2024]:
    # Summer sale, Easter, EOFY, Christmas run-up, etc.
    for md in ["01-15", "04-10", "06-25", "08-20", "10-05", "11-25", "12-10", "12-20"]:
        base = pd.Timestamp(f"{yr}-{md}")
        for offset in range(7):  # 1-week promo window
            promo_dates.add(str((base + pd.Timedelta(days=offset)).date()))

cal_lookup = calendar.set_index("date_id")

sales_rows = []
sale_id = 1

for _, store in stores.iterrows():
    for _, product in products.iterrows():
        base   = product["base_demand_daily"] * store["size_multiplier"]
        stddev = product["demand_stddev"]      * store["size_multiplier"]

        for d in DATES:
            ds      = str(d.date())
            season  = cal_lookup.loc[d.date(), "nz_season"]
            is_hol  = cal_lookup.loc[d.date(), "is_public_holiday"]
            dow     = d.dayofweek + 1

            # Seasonal + day-of-week effect
            s_mult  = SEASON_MULT.get(product["category"], {}).get(season, 1.0)
            d_mult  = DOW_MULT[dow]
            hol_mult= 1.20 if is_hol else 1.0

            # Promotion
            is_promo      = 1 if ds in promo_dates else 0
            promo_discount= round(np.random.uniform(0.10, 0.25), 2) if is_promo else None
            promo_mult    = 1.35 if is_promo else 1.0

            # Final demand (with noise)
            demand = max(0, int(np.random.normal(
                base * s_mult * d_mult * hol_mult * promo_mult,
                stddev * 0.5
            )))

            unit_price = product["unit_price_nzd"]
            if is_promo:
                unit_price = round(unit_price * (1 - promo_discount), 2)

            returns = np.random.poisson(0.02 * demand) if demand > 0 else 0

            sales_rows.append({
                "sale_id":          sale_id,
                "date_id":          d.date(),
                "store_id":         store["store_id"],
                "product_id":       product["product_id"],
                "units_sold":       demand,
                "revenue_nzd":      round(demand * unit_price, 2),
                "units_returned":   returns,
                "is_promotion":     is_promo,
                "promo_discount_pct": promo_discount,
            })
            sale_id += 1

sales = pd.DataFrame(sales_rows)
sales.to_csv(DATA_DIR / "fact_sales_daily.csv", index=False)
print(f"    {len(sales):,} daily sales records saved.")


# =============================================================================
# 7. fact_inventory_store  (rolling simulation)
# =============================================================================
print("\n[7/10] Generating fact_inventory_store ...")

REPLENISHMENT_DAYS = 3  # Store reorder cycle
inv_store_rows = []
inv_id = 1

# Sales lookup for quick access
sales_lookup = sales.groupby(["date_id", "store_id", "product_id"])["units_sold"].sum()

for _, store in stores.iterrows():
    for _, product in products.iterrows():
        base_demand = product["base_demand_daily"] * store["size_multiplier"]

        # Initial stock: 7 days of supply
        qty = int(base_demand * 7)
        rop = int(base_demand * (REPLENISHMENT_DAYS + 1))
        ss  = int(base_demand * 1.5)

        for d in DATES:
            key = (d.date(), store["store_id"], product["product_id"])
            sold = int(sales_lookup.get(key, 0))

            qty -= sold
            qty  = max(0, qty)

            # Simple replenishment trigger
            if qty <= rop:
                qty += int(base_demand * 7)

            is_stockout = 1 if qty == 0 else 0
            dos = round(qty / max(base_demand, 1), 2)

            inv_store_rows.append({
                "inventory_id":  inv_id,
                "date_id":       d.date(),
                "store_id":      store["store_id"],
                "product_id":    product["product_id"],
                "qty_on_hand":   qty,
                "qty_on_order":  int(base_demand * 3) if qty <= rop else 0,
                "reorder_point": rop,
                "safety_stock":  ss,
                "is_stockout":   is_stockout,
                "days_of_supply":dos,
            })
            inv_id += 1

inv_store = pd.DataFrame(inv_store_rows)
inv_store.to_csv(DATA_DIR / "fact_inventory_store.csv", index=False)
print(f"    {len(inv_store):,} store inventory records saved.")


# =============================================================================
# 8. fact_inventory_dc
# =============================================================================
print("\n[8/10] Generating fact_inventory_dc ...")

inv_dc_rows = []
inv_dc_id = 1

store_by_dc = stores.groupby("dc_id")["store_id"].apply(list).to_dict()
dc_sales = sales.merge(stores[["store_id", "dc_id"]], on="store_id")
dc_sales_lookup = dc_sales.groupby(["date_id", "dc_id", "product_id"])["units_sold"].sum()

for _, dc in dcs.iterrows():
    for _, product in products.iterrows():
        n_stores   = len(store_by_dc.get(dc["dc_id"], []))
        base_demand = product["base_demand_daily"] * n_stores * 1.0

        qty       = int(base_demand * 14)
        rop       = int(base_demand * 7)
        ss        = int(base_demand * 3)

        for d in DATES:
            key  = (d.date(), dc["dc_id"], product["product_id"])
            sold = int(dc_sales_lookup.get(key, 0))

            qty -= sold
            qty  = max(0, qty)

            if qty <= rop:
                qty += int(base_demand * 14)

            reserved  = int(qty * 0.3)
            available = max(0, qty - reserved)
            is_stockout = 1 if qty == 0 else 0

            inv_dc_rows.append({
                "inventory_id":  inv_dc_id,
                "date_id":       d.date(),
                "dc_id":         dc["dc_id"],
                "product_id":    product["product_id"],
                "qty_on_hand":   qty,
                "qty_on_order":  int(base_demand * 7) if qty <= rop else 0,
                "qty_reserved":  reserved,
                "qty_available": available,
                "reorder_point": rop,
                "safety_stock":  ss,
                "is_stockout":   is_stockout,
            })
            inv_dc_id += 1

inv_dc = pd.DataFrame(inv_dc_rows)
inv_dc.to_csv(DATA_DIR / "fact_inventory_dc.csv", index=False)
print(f"    {len(inv_dc):,} DC inventory records saved.")


# =============================================================================
# 9. fact_purchase_orders  (DC → Supplier)
# =============================================================================
print("\n[9/10] Generating fact_purchase_orders ...")

po_rows = []
po_counter = 1

sup_lookup = suppliers.set_index("supplier_id")
prod_sup   = products.set_index("product_id")["supplier_id"].to_dict()

for _, dc in dcs.iterrows():
    for _, product in products.iterrows():
        sup_id  = prod_sup[product["product_id"]]
        sup     = sup_lookup.loc[sup_id]
        lt_mean = sup["lead_time_days"]
        lt_std  = sup["lead_time_stddev_days"]

        # One PO roughly every 2 weeks
        order_dates = pd.date_range(START_DATE, END_DATE, freq="14D")

        for od in order_dates:
            actual_lt  = max(1, int(np.random.normal(lt_mean, lt_std)))
            exp_date   = od + pd.Timedelta(days=lt_mean)
            actual_date= od + pd.Timedelta(days=actual_lt)
            is_late    = 1 if actual_lt > lt_mean else 0
            delay_days = max(0, actual_lt - lt_mean)

            n_stores   = len(store_by_dc.get(dc["dc_id"], []))
            qty        = int(product["base_demand_daily"] * n_stores * 14
                            * np.random.uniform(0.9, 1.1))

            # Occasionally cancel a PO
            status = "CANCELLED" if np.random.rand() < 0.03 else "RECEIVED"
            qty_recv = qty if status == "RECEIVED" else 0

            po_rows.append({
                "po_id":          f"PO{po_counter:06d}",
                "dc_id":          dc["dc_id"],
                "supplier_id":    sup_id,
                "product_id":     product["product_id"],
                "order_date":     od.date(),
                "expected_date":  exp_date.date(),
                "actual_date":    actual_date.date() if status == "RECEIVED" else None,
                "qty_ordered":    qty,
                "qty_received":   qty_recv if status == "RECEIVED" else None,
                "unit_cost_nzd":  product["unit_cost_nzd"],
                "total_cost_nzd": round(qty * product["unit_cost_nzd"], 2),
                "po_status":      status,
                "is_late":        is_late,
                "delay_days":     delay_days,
            })
            po_counter += 1

pos = pd.DataFrame(po_rows)
pos.to_csv(DATA_DIR / "fact_purchase_orders.csv", index=False)
print(f"    {len(pos):,} purchase orders saved.")


# =============================================================================
# 10. fact_shipments  (DC → Store)
# =============================================================================
print("\n[10/10] Generating fact_shipments ...")

ship_rows = []
ship_counter = 1

for _, store in stores.iterrows():
    dc_id = store["dc_id"]
    for _, product in products.iterrows():
        base_demand = product["base_demand_daily"] * store["size_multiplier"]

        # Replenishment shipment every 3 days
        ship_dates = pd.date_range(START_DATE, END_DATE, freq="3D")

        for sd in ship_dates:
            transit_mean = 1 if store["region"] in ["Auckland", "Wellington", "Canterbury"] else 2
            actual_transit = max(1, int(np.random.normal(transit_mean, 0.5)))
            exp_date   = sd + pd.Timedelta(days=transit_mean)
            actual_date= sd + pd.Timedelta(days=actual_transit)
            is_late    = 1 if actual_transit > transit_mean else 0
            delay_days = max(0, actual_transit - transit_mean)

            qty = int(base_demand * 3 * np.random.uniform(0.85, 1.15))
            status = "DELIVERED" if actual_date <= END_DATE else "IN_TRANSIT"
            qty_recv = qty if status == "DELIVERED" else None

            ship_rows.append({
                "shipment_id":    f"SHP{ship_counter:07d}",
                "dc_id":          dc_id,
                "store_id":       store["store_id"],
                "product_id":     product["product_id"],
                "dispatch_date":  sd.date(),
                "expected_date":  exp_date.date(),
                "actual_date":    actual_date.date() if status == "DELIVERED" else None,
                "qty_dispatched": qty,
                "qty_received":   qty_recv,
                "shipment_status":status,
                "is_late":        is_late,
                "delay_days":     delay_days,
                "transit_days":   actual_transit if status == "DELIVERED" else None,
            })
            ship_counter += 1

shipments = pd.DataFrame(ship_rows)
shipments.to_csv(DATA_DIR / "fact_shipments.csv", index=False)
print(f"    {len(shipments):,} shipments saved.")


# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 60)
print("  DATA GENERATION COMPLETE")
print("=" * 60)
print(f"\n  Suppliers  : {len(suppliers)}")
print(f"  Products   : {len(products)}")
print(f"  DCs        : {len(dcs)}")
print(f"  Stores     : {len(stores)}")
print(f"  Calendar   : {len(calendar):,} days ({START_DATE.date()} → {END_DATE.date()})")
print(f"\n  Sales      : {len(sales):,} records")
print(f"  Inv Store  : {len(inv_store):,} records")
print(f"  Inv DC     : {len(inv_dc):,} records")
print(f"  POs        : {len(pos):,} records")
print(f"  Shipments  : {len(shipments):,} records")
print(f"\n  All CSVs saved → data/")
print(f"  Stockout rate (store): {inv_store['is_stockout'].mean()*100:.2f}%")
print(f"  PO late rate         : {pos['is_late'].mean()*100:.1f}%")
print(f"  Shipment late rate   : {shipments['is_late'].mean()*100:.1f}%")
