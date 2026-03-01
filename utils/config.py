# utils/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# ════════════════════════════════════════════════
# CONNECTION & LLM CONFIG
# ════════════════════════════════════════════════
CLEAN_DB_URI = os.getenv("CLEAN_SUPABASE_DB_URI")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
LLM_MODEL = "openrouter/x-ai/grok-4.1-fast"
LLM_BASE_URL = "https://openrouter.ai/api/v1"

# ════════════════════════════════════════════════
# SCHEMA MAP
# ════════════════════════════════════════════════
SCHEMA_MAP = {
    "tables": {
        "fact_bookings": {
            "alias": "fb",
            "description": "Individual booking records. Each row = one booking.",
            "grain": "One row per booking_id",
        },
        "fact_aggregated_bookings": {
            "alias": "fa",
            "description": "Daily room capacity and successful bookings per property per room type.",
            "grain": "One row per (property_id, check_in_date, room_category)",
        },
        "dim_hotels": {
            "alias": "dh",
            "description": "Hotel/property master data.",
            "grain": "One row per property_id",
        },
        "dim_date": {
            "alias": "dd",
            "description": "Date dimension covering May-July 2022.",
            "grain": "One row per date",
        },
        "dim_rooms": {
            "alias": "dr",
            "description": "Room type master data.",
            "grain": "One row per room_id",
        }
    },
    "join_paths": {
        "fb_to_dh": "fact_bookings.property_id = dim_hotels.property_id",
        "fb_to_dd": "fact_bookings.check_in_date = dim_date.date",
        "fb_to_dr": "fact_bookings.room_category = dim_rooms.room_id",
        "fa_to_dh": "fact_aggregated_bookings.property_id = dim_hotels.property_id",
        "fa_to_dd": "fact_aggregated_bookings.check_in_date = dim_date.date",
        "fa_to_dr": "fact_aggregated_bookings.room_category = dim_rooms.room_id"
    },
    "critical_rules": [
        "NEVER direct-join fact_bookings with fact_aggregated_bookings — different granularity. Use CTEs.",
        "week_no is TEXT type. Always compare as string: week_no = '31' NOT = 31",
        "day_type values are 'Weekend' (Friday & Saturday) and 'Weekday' (Sunday through Thursday).",
        "revenue_realized is THE revenue column. revenue_generated is gross before adjustments.",
        "ratings_given = 0 means 'not rated'. Filter WHERE ratings_given > 0 for average rating.",
    ]
}

# ════════════════════════════════════════════════
# METRIC LIBRARY
# ════════════════════════════════════════════════
METRIC_LIBRARY = {
    "revenue": {
        "sql": "SUM(fb.revenue_realized)",
        "tables": ["fact_bookings"],
        "aliases": ["revenue", "total revenue", "earnings", "income", "sales"],
        "description": "Total realized revenue after cancellation adjustments"
    },
    "total_bookings": {
        "sql": "COUNT(fb.booking_id)",
        "tables": ["fact_bookings"],
        "aliases": ["bookings", "total bookings", "booking count", "number of bookings"],
        "description": "Total number of bookings made"
    },
    "average_rating": {
        "sql": "ROUND(AVG(fb.ratings_given) FILTER(WHERE fb.ratings_given > 0)::numeric, 2)",
        "tables": ["fact_bookings"],
        "aliases": ["rating", "average rating", "avg rating", "customer rating", "guest rating"],
        "description": "Average guest rating (excludes unrated bookings where rating=0)"
    },
    "total_cancelled_bookings": {
        "sql": "COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Cancelled')",
        "tables": ["fact_bookings"],
        "aliases": ["cancellations", "cancelled bookings", "cancelled"],
        "description": "Total bookings with status Cancelled"
    },
    "total_checked_out": {
        "sql": "COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Checked Out')",
        "tables": ["fact_bookings"],
        "aliases": ["checked out", "completed stays", "successful stays"],
        "description": "Total bookings where guest actually stayed"
    },
    "total_no_show": {
        "sql": "COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'No Show')",
        "tables": ["fact_bookings"],
        "aliases": ["no shows", "no show bookings", "did not show up"],
        "description": "Total bookings where guest booked but never arrived"
    },
    "no_of_days": {
        "sql": "COUNT(DISTINCT dd.date)",
        "tables": ["dim_date"],
        "aliases": ["number of days", "day count", "period length"],
        "description": "Count of distinct dates in the selected period"
    },
    "total_capacity": {
        "sql": "SUM(fa.capacity)",
        "tables": ["fact_aggregated_bookings"],
        "aliases": ["capacity", "total capacity", "room capacity", "available rooms"],
        "description": "Total room-nights available across all properties"
    },
    "total_successful_bookings": {
        "sql": "SUM(fa.successful_bookings)",
        "tables": ["fact_aggregated_bookings"],
        "aliases": ["successful bookings", "confirmed bookings", "rooms sold"],
        "description": "Total rooms successfully booked (from aggregated data)"
    },
    "occupancy_pct": {
        "sql": "ROUND(SUM(fa.successful_bookings)::numeric / NULLIF(SUM(fa.capacity), 0) * 100, 2)",
        "tables": ["fact_aggregated_bookings"],
        "aliases": ["occupancy", "occupancy rate", "occupancy %", "how full", "room utilization"],
        "description": "Percentage of available rooms that were successfully booked"
    },
    "adr": {
        "sql": "ROUND(SUM(fb.revenue_realized)::numeric / NULLIF(COUNT(fb.booking_id), 0), 2)",
        "tables": ["fact_bookings"],
        "aliases": ["ADR", "average daily rate", "average rate", "rate per booking", "average room rate"],
        "description": "Average revenue per booking"
    },
    "revpar": {
        "sql": "ROUND(total_revenue::numeric / NULLIF(total_capacity, 0), 2)",
        "tables": ["fact_bookings", "fact_aggregated_bookings"],
        "aliases": ["RevPAR", "revenue per available room", "revenue yield per room", "yield per room", "revenue yield"],
        "description": "Revenue per available room-night. Needs BOTH fact tables via CTEs.",
        "needs_cte": True
    },
    "realisation_pct": {
        "sql": "ROUND((1.0 - (COUNT(fb.booking_id) FILTER(WHERE fb.booking_status IN ('Cancelled','No Show')))::numeric / NULLIF(COUNT(fb.booking_id), 0)) * 100, 2)",
        "tables": ["fact_bookings"],
        "aliases": ["realisation", "realization", "realisation %", "conversion", "conversion rate",
                     "conversion of bookings into guests", "booking to guest conversion", "guest conversion"],
        "description": "Percentage of bookings that resulted in actual stays"
    },
    "cancellation_pct": {
        "sql": "ROUND(COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Cancelled')::numeric / NULLIF(COUNT(fb.booking_id), 0) * 100, 2)",
        "tables": ["fact_bookings"],
        "aliases": ["cancellation rate", "cancellation %", "cancel rate"],
        "description": "Percentage of bookings that were cancelled"
    },
    "no_show_rate_pct": {
        "sql": "ROUND(COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'No Show')::numeric / NULLIF(COUNT(fb.booking_id), 0) * 100, 2)",
        "tables": ["fact_bookings"],
        "aliases": ["no show rate", "no show %", "no show rate %"],
        "description": "Percentage of bookings that were no-shows"
    },
    "booking_pct_by_platform": {
        "sql": "ROUND(COUNT(fb.booking_id)::numeric / NULLIF(SUM(COUNT(fb.booking_id)) OVER(), 0) * 100, 2)",
        "tables": ["fact_bookings"],
        "aliases": ["booking % by platform", "platform share", "booking share", "platform breakdown",
                     "which platform", "booking percentage by platform"],
        "description": "Each platform's share of total bookings. Must GROUP BY booking_platform."
    },
    "booking_pct_by_room_class": {
        "sql": "ROUND(COUNT(fb.booking_id)::numeric / NULLIF(SUM(COUNT(fb.booking_id)) OVER(), 0) * 100, 2)",
        "tables": ["fact_bookings", "dim_rooms"],
        "aliases": ["booking % by room", "room share", "room class breakdown"],
        "description": "Each room class's share of total bookings. Must GROUP BY room_class."
    },
    "dbrn": {
        "sql": "ROUND(COUNT(fb.booking_id)::numeric / NULLIF(COUNT(DISTINCT dd.date), 0), 2)",
        "tables": ["fact_bookings", "dim_date"],
        "aliases": ["DBRN", "daily booked room nights", "bookings per day"],
        "description": "Average bookings per day in the period"
    },
    "dsrn": {
        "sql": "ROUND(SUM(fa.capacity)::numeric / NULLIF(COUNT(DISTINCT dd.date), 0), 2)",
        "tables": ["fact_aggregated_bookings", "dim_date"],
        "aliases": ["DSRN", "daily sellable room nights", "capacity per day"],
        "description": "Average available room-nights per day"
    },
    "durn": {
        "sql": "ROUND(COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Checked Out')::numeric / NULLIF(COUNT(DISTINCT dd.date), 0), 2)",
        "tables": ["fact_bookings", "dim_date"],
        "aliases": ["DURN", "daily utilized room nights", "stays per day"],
        "description": "Average checked-out bookings per day"
    },
    "wow_change": {
        "sql": "ROUND(((current_week_value::numeric / NULLIF(previous_week_value, 0)) - 1) * 100, 2)",
        "tables": ["depends_on_base_metric"],
        "aliases": ["WoW", "week over week", "weekly change", "compared to last week",
                     "compared to the one before", "weekly trend", "week on week"],
        "description": "Percentage change between two consecutive weeks.",
        "needs_cte": True
    }
}

# ════════════════════════════════════════════════
# BUSINESS RULES
# ════════════════════════════════════════════════
BUSINESS_RULES = {
    "time_intelligence": {
        "week_numbering": "week_no is TEXT ('19' to '32'). Covers May-July 2022.",
        "day_type_logic": "Friday & Saturday = 'Weekend', Sunday through Thursday = 'Weekday'.",
        "latest_week_warning": "Week '32' has only 1 day. Use latest FULL week for comparisons.",
        "wow_calculation": "WoW % = ((Current Week Value / Previous Week Value) - 1) * 100"
    },
    "revenue_logic": {
        "primary_column": "revenue_realized — NET revenue after cancellation adjustments.",
        "cancellation_rule": "Cancelled = hotel keeps 40% of revenue_generated.",
        "no_show_rule": "No Show = full revenue_generated goes to hotel.",
        "checked_out_rule": "Checked Out = full revenue_generated goes to hotel."
    },
    "granularity_rules": {
        "fact_bookings": "One row per booking. Use for revenue, ADR, status, platform, ratings.",
        "fact_aggregated_bookings": "One row per (property, date, room). Use for capacity, occupancy.",
        "why_no_direct_join": "Different granularity. Direct join causes row multiplication."
    }
}