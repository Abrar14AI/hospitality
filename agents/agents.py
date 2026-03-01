# agents/agents.py
"""
Enterprise Chat-with-Data Pipeline for AtliQ Hospitality.

Architecture: LLM + Native Function Calling

  ┌──────────────────────────────────────────────────────┐
  │  LLM receives question + database context            │
  │       ↓                                              │
  │  LLM natively calls: calculate_metrics({             │
  │    metrics: ["occupancy_pct", "revpar"],             │
  │    filters: {},                                      │
  │    group_by: "category"                              │
  │  })                                                  │
  │       ↓                                              │
  │  Python: Deterministic SQL builder → DB → Data       │
  │       ↓                                              │
  │  Data sent back to LLM                               │
  │       ↓                                              │
  │  LLM presents professional business answer           │
  └──────────────────────────────────────────────────────┘

Why this works:
  - Native function calling (structured JSON, not text parsing)
  - LLM CAN make multiple tool calls in one response
  - Deterministic SQL builder = zero syntax errors
  - Works with ANY model: Grok, Claude, GPT, Gemini
  - No CrewAI, no ReAct loop, no text-based "Action/Action Input"
"""

import json
import traceback
import litellm

from tools.tools import (
    execute_multiple_metrics, execute_custom_sql,
    get_database_context, resolve_metric_name
)
from utils.config import (
    LLM_MODEL, LLM_BASE_URL, OPENROUTER_API_KEY, METRIC_LIBRARY
)

# Suppress litellm debug noise
litellm.suppress_debug_info = True

VERBOSE = True


def _log(msg):
    if VERBOSE:
        print(msg)


# ════════════════════════════════════════════════
# SYSTEM PROMPT (built with live DB context)
# ════════════════════════════════════════════════

def _build_system_prompt(db_context):
    """Build system prompt with live database context and metric catalog."""

    # Build concise metric reference
    metric_lines = []
    for key, info in METRIC_LIBRARY.items():
        aliases = info.get('aliases', [])[:3]
        alias_str = f" (also: {', '.join(aliases)})" if aliases else ""
        metric_lines.append(f"  {key}: {info['description']}{alias_str}")

    metric_catalog = "\n".join(metric_lines)

    return f"""You are a data analyst for AtliQ Hospitality, a hotel chain operating in India.

Your job: Answer business questions using the tools provided. Call tools to get data, then present a professional answer.

DATABASE CONTEXT:
- Date range: {db_context['date_range']}
- Cities: {', '.join(db_context['cities'])}
- Weeks: {db_context['weeks']} (week_no is TEXT — always quote as '31' not 31)
- Latest full week: '{db_context['latest_full_week']}'
- Platforms: {', '.join(db_context['platforms'])}
- Categories: Luxury, Business
- Room classes: Standard, Elite, Premium, Presidential
- day_type: 'Weekend' (Friday & Saturday), 'Weekday' (Sunday to Thursday)

AVAILABLE METRICS (use these exact names in calculate_metrics):
{metric_catalog}

WoW METRICS (pass current_week in filters instead of week_no):
  wow_revenue, wow_occupancy, wow_adr, wow_revpar, wow_realisation, wow_dsrn

HOW TO INTERPRET QUESTIONS:
- "performance" / "snapshot" / "overview" → metrics: [revenue, occupancy_pct, adr, revpar, realisation_pct]
- "filling rooms" / "how full" / "utilization" → metrics: [occupancy_pct]
- "revenue per room" / "yield per room" → metrics: [revpar]
- "conversion" / "bookings to stays" / "checked in" → metrics: [realisation_pct]
- "rate" / "average rate" → metrics: [adr]
- "luxury vs business" / "by category" → group_by: "category"
- "by city" / "across cities" / "per city" → group_by: "city"
- "trend" / "weekly" / "over time" → group_by: "week_no"
- "weekend vs weekday" → group_by: "day_type"
- "by platform" → group_by: "booking_platform"
- "by hotel" / "by property" → group_by: "property_name"
- "compared to last week" / "week over week" → use wow_ metrics with current_week filter
- "latest week" → filter week_no: '{db_context['latest_full_week']}'
- "top N" / "ranking" / "which hotel has highest/lowest" → use run_custom_sql

SCHEMA (for run_custom_sql only):
- fact_bookings (fb): booking_id, property_id, check_in_date, checkout_date, revenue_realized, booking_status ('Checked Out'/'Cancelled'/'No Show'), booking_platform, room_category (RT1-RT4), ratings_given (0=not rated), no_guests
- fact_aggregated_bookings (fa): property_id, check_in_date, room_category, successful_bookings, capacity
- dim_hotels (dh): property_id, property_name, category, city
- dim_date (dd): date, mmm_yy, week_no (TEXT!), day_type
- dim_rooms (dr): room_id, room_class
- Joins: fb/fa.property_id = dh.property_id | fb/fa.check_in_date = dd.date | fb/fa.room_category = dr.room_id
- NEVER direct-join fact_bookings with fact_aggregated_bookings
- revenue_realized = THE revenue column (not revenue_generated)

ANSWER RULES:
- Revenue → ₹X.XXM or ₹X.XXB
- Percentages → XX.X%
- Rates/RevPAR/ADR → ₹X,XXX
- Keep under 200 words
- End with one actionable business insight
- ONLY use numbers from tool results — NEVER invent numbers
"""


# ════════════════════════════════════════════════
# TOOL SCHEMAS (OpenAI Function Calling Format)
# ════════════════════════════════════════════════

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "calculate_metrics",
            "description": (
                "Calculate one or more hotel KPIs with optional filters and grouping. "
                "Handles ALL metrics including cross-table ones like RevPAR (uses CTEs automatically). "
                "This is the PRIMARY tool — use it for all standard KPI questions. "
                "You CAN request multiple metrics in one call."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "List of metric names to calculate. Available: "
                            "revenue, total_bookings, adr, average_rating, "
                            "realisation_pct, cancellation_pct, no_show_rate_pct, "
                            "occupancy_pct, total_capacity, total_successful_bookings, "
                            "revpar, dbrn, dsrn, durn, "
                            "booking_pct_by_platform, booking_pct_by_room_class, "
                            "wow_revenue, wow_occupancy, wow_adr, wow_revpar, wow_realisation, wow_dsrn"
                        )
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional filters to narrow results.",
                        "properties": {
                            "city": {
                                "type": "string",
                                "description": "City name: Delhi, Mumbai, Hyderabad, or Bangalore"
                            },
                            "week_no": {
                                "type": "string",
                                "description": "Week number as TEXT: '19' to '32'"
                            },
                            "category": {
                                "type": "string",
                                "description": "Hotel category: Luxury or Business"
                            },
                            "property_name": {
                                "type": "string",
                                "description": "Exact hotel name, e.g., 'Atliq Grands'"
                            },
                            "day_type": {
                                "type": "string",
                                "description": "'Weekend' (Fri+Sat) or 'Weekday' (Sun-Thu)"
                            },
                            "mmm_yy": {
                                "type": "string",
                                "description": "Month: 'May 22', 'Jun 22', or 'Jul 22'"
                            },
                            "room_class": {
                                "type": "string",
                                "description": "Standard, Elite, Premium, or Presidential"
                            },
                            "booking_platform": {
                                "type": "string",
                                "description": "Booking channel name"
                            },
                            "current_week": {
                                "type": "string",
                                "description": "Current week for WoW metrics (use INSTEAD of week_no for wow_ metrics)"
                            }
                        }
                    },
                    "group_by": {
                        "type": "string",
                        "description": (
                            "Optional: group results by a dimension. "
                            "Options: city, property_name, category, week_no, "
                            "mmm_yy, day_type, room_class, booking_platform"
                        )
                    }
                },
                "required": ["metrics"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "run_custom_sql",
            "description": (
                "Execute a custom PostgreSQL query for questions that don't map to standard metrics. "
                "Use for: top N rankings, specific record lookups, custom conditions, correlations. "
                "NEVER direct-join fact_bookings with fact_aggregated_bookings."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": (
                            "A PostgreSQL SELECT or WITH (CTE) query. "
                            "Tables: fact_bookings fb, fact_aggregated_bookings fa, "
                            "dim_hotels dh, dim_date dd, dim_rooms dr. "
                            "Remember: week_no is TEXT — use '31' not 31."
                        )
                    }
                },
                "required": ["sql_query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_metric",
            "description": (
                "Find the correct metric name for a business concept. "
                "Use when you're unsure which metric name to pass to calculate_metrics."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "search_term": {
                        "type": "string",
                        "description": "Business concept to search, e.g., 'revenue yield', 'conversion rate', 'room utilization'"
                    }
                },
                "required": ["search_term"]
            }
        }
    }
]


# ════════════════════════════════════════════════
# TOOL EXECUTOR
# ════════════════════════════════════════════════

def _execute_tool_call(tool_name, arguments):
    """Execute a tool call and return the result as a string."""

    _log(f"  🔧 Tool: {tool_name}")
    _log(f"     Args: {json.dumps(arguments, indent=2)}")

    try:
        if tool_name == "calculate_metrics":
            metrics = arguments.get("metrics", [])
            filters = arguments.get("filters") or {}
            group_by = arguments.get("group_by")

            # Clean filters: remove None/empty values, stringify all
            filters = {
                k: str(v) for k, v in filters.items()
                if v is not None and str(v).strip()
            }

            _log(f"     📊 Metrics: {metrics}")
            _log(f"     🔍 Filters: {filters}")
            _log(f"     📂 Group by: {group_by}")

            results = execute_multiple_metrics(metrics, filters, group_by)

            output_parts = []
            for metric_name, result in results.items():
                if result['error']:
                    output_parts.append(f"❌ {metric_name}: {result['error']}")
                    _log(f"     ❌ {metric_name}: {result['error']}")
                else:
                    row_count = len(result['df']) if result['df'] is not None else 0
                    output_parts.append(f"✅ {metric_name}:\n{result['markdown']}")
                    _log(f"     ✅ {metric_name}: {row_count} rows")

            return "\n\n".join(output_parts) if output_parts else "No results returned."

        elif tool_name == "run_custom_sql":
            sql = arguments.get("sql_query", "")
            _log(f"     SQL: {sql[:120]}...")

            df, err = execute_custom_sql(sql)
            if err:
                _log(f"     ❌ {err}")
                return f"SQL Error: {err}"
            if df is None or df.empty:
                return "Query returned 0 rows. Check your filter values."

            _log(f"     ✅ {len(df)} rows returned")
            return df.to_markdown(index=False)

        elif tool_name == "search_metric":
            term = arguments.get("search_term", "")
            matches = resolve_metric_name(term)
            if not matches:
                all_metrics = list(METRIC_LIBRARY.keys())
                return f"No metric found for '{term}'. Available metrics: {all_metrics}"

            lines = []
            for key in matches:
                info = METRIC_LIBRARY[key]
                lines.append(f"  {key}: {info['description']}")
            return "Matching metrics:\n" + "\n".join(lines)

        else:
            return f"Unknown tool: {tool_name}"

    except Exception as e:
        error_msg = f"Tool execution error: {str(e)}"
        _log(f"     ❌ {error_msg}")
        return error_msg


# ════════════════════════════════════════════════
# MAIN PIPELINE
# ════════════════════════════════════════════════

def query_data_agent(question: str) -> str:
    """
    Enterprise Chat-with-Data entry point.

    Uses native function calling:
      1. LLM understands question
      2. LLM calls tools via structured JSON (not text parsing)
      3. Python executes tools deterministically
      4. LLM presents business answer

    Works reliably with ANY model that supports function calling.
    Same interface as before — frontend doesn't change.
    """
    _log(f"\n{'='*60}")
    _log(f"❓ QUESTION: {question}")
    _log(f"🔧 Model: {LLM_MODEL}")
    _log(f"{'='*60}")

    try:
        # ── Step 1: Load DB context ──
        _log(f"\n📊 Loading database context...")
        db_context = get_database_context()
        if 'error' in db_context:
            return f"Database connection error: {db_context['error']}"
        _log(f"   ✅ {len(db_context['weeks'])} weeks, {len(db_context['cities'])} cities loaded")

        # ── Step 2: Build conversation ──
        system_prompt = _build_system_prompt(db_context)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question}
        ]

        # ── Step 3: Conversation loop (LLM ↔ Tools) ──
        max_iterations = 5

        for iteration in range(max_iterations):
            _log(f"\n🤖 LLM Call #{iteration + 1}...")

            response = litellm.completion(
                model=LLM_MODEL,
                api_key=OPENROUTER_API_KEY,
                messages=messages,
                tools=TOOL_SCHEMAS,
                tool_choice="auto",
                temperature=0.1,
            )

            message = response.choices[0].message

            # ── LLM wants to call tools ──
            if hasattr(message, 'tool_calls') and message.tool_calls:
                _log(f"   📞 {len(message.tool_calls)} tool call(s)")

                # Add assistant message (with tool_calls) to conversation
                assistant_msg = {
                    "role": "assistant",
                    "content": message.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments
                            }
                        }
                        for tc in message.tool_calls
                    ]
                }
                messages.append(assistant_msg)

                # Execute each tool call and add results
                for tc in message.tool_calls:
                    fn_name = tc.function.name

                    # Parse arguments
                    try:
                        fn_args = json.loads(tc.function.arguments)
                    except json.JSONDecodeError:
                        _log(f"   ⚠️ Bad JSON args: {tc.function.arguments[:100]}")
                        fn_args = {}

                    # Execute
                    result = _execute_tool_call(fn_name, fn_args)

                    # Add tool result to conversation
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": str(result)
                    })

                # Continue loop — LLM will see results and respond
                continue

            # ── LLM gave final answer (no tool calls) ──
            else:
                final_answer = message.content or "No response generated."
                _log(f"\n✅ Answer received ({len(final_answer)} chars)")
                return final_answer

        # If we exit the loop without a final answer
        return "Analysis could not be completed. Please try rephrasing your question."

    except Exception as e:
        _log(f"\n❌ Pipeline error: {str(e)}")
        _log(traceback.format_exc())
        return f"I encountered an error while processing your question: {str(e)}"


# ════════════════════════════════════════════════
# TEST SUITE
# ════════════════════════════════════════════════
if __name__ == "__main__":
    test_questions = [
        # Test 1: Cross-table metric (RevPAR) — hardest failure before
        #"What is the RevPAR for week 31?",

        # Test 2: Single-table with city + week filter
        #"What is the occupancy rate in Delhi in week 30?",

        # Test 3: Multi-metric + comparison (THE test that always failed)
        "For each city, identify the 'Luxury' category hotel that achieved the highest RevPAR during the last full week of data. Show the hotel name, its RevPAR, and how much higher its RevPAR was compared to the city average for luxury hotels that same week",
    ]

    for q in test_questions:
        print(f"\n{'='*60}")
        print(f"QUESTION: {q}")
        print(f"{'='*60}")
        try:
            response = query_data_agent(q)
            print(f"\nFINAL ANSWER:\n{response}")
        except Exception as e:
            print(f"ERROR: {str(e)}")
            traceback.print_exc()
        print(f"\n{'─'*60}\n")