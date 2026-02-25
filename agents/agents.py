# agents/agents.py
from crewai import Agent, Task, Crew, Process, LLM
from tools.tools import (
    get_db_context, explore_schema, find_exact_values, 
    execute_sql, lookup_metric
)
from utils.config import (
    SCHEMA_MAP, METRIC_LIBRARY, BUSINESS_RULES,
    LLM_MODEL, LLM_BASE_URL, OPENROUTER_API_KEY
)
from prompts.cot_prompts import (
    REASONING_ENGINE_PROMPT, DISCOVERY_PROMPT, 
    SEMANTIC_PROMPT, SQL_ARCHITECT_PROMPT, ANALYST_PROMPT
)

# ════════════════════════════════════════════════
# LLM CONFIGURATION
# ════════════════════════════════════════════════
llm = LLM(
    model=LLM_MODEL,
    base_url=LLM_BASE_URL,
    api_key=OPENROUTER_API_KEY,
    temperature=0.1
)

# ════════════════════════════════════════════════
# KNOWLEDGE INJECTION
# Format schema + rules as a compact reference string
# that gets injected into agent backstories
# ════════════════════════════════════════════════
SCHEMA_REFERENCE = f"""
TABLES & RELATIONSHIPS:
{chr(10).join(f"- {name} ({info['alias']}): {info['description']} | Grain: {info['grain']}" for name, info in SCHEMA_MAP['tables'].items())}

JOIN PATHS:
{chr(10).join(f"- {path}" for path in SCHEMA_MAP['join_paths'].values())}

CRITICAL RULES:
{chr(10).join(f"- {rule}" for rule in SCHEMA_MAP['critical_rules'])}
"""

BUSINESS_CONTEXT = f"""
TIME RULES: {BUSINESS_RULES['time_intelligence']}
REVENUE RULES: {BUSINESS_RULES['revenue_logic']}  
GRANULARITY: {BUSINESS_RULES['granularity_rules']}
"""

# ════════════════════════════════════════════════
# AGENT DEFINITIONS
# ════════════════════════════════════════════════

data_scout = Agent(
    role="Data Discovery Scout",
    goal=(
        "Verify ALL entities and time boundaries from the database. "
        "Output ONLY a clean bulleted list of verified facts."
    ),
    backstory=(
        "You are the investigative foundation of the analytics team. "
        "Before any analysis can happen, YOU verify the facts. "
        "You never assume — you query. You never guess — you look up. "
        "Your outputs are short, factual, and bullet-pointed.\n\n"
        f"SCHEMA AWARENESS:\n{SCHEMA_REFERENCE}"
    ),
    tools=[get_db_context, find_exact_values, execute_sql, explore_schema],
    llm=llm,
    verbose=True,
    allow_delegation=False,
    max_iter=6
)

semantic_guardian = Agent(
    role="Metric & Logic Guardian",
    goal=(
        "Map every user intent to exact Metric Library formulas. "
        "Produce a structured build plan — never raw SQL."
    ),
    backstory=(
        "You are the guardian of AtliQ's business logic. "
        "You know every KPI, its formula, and which tables power it. "
        "You receive verified facts from the Scout and produce a precise build plan "
        "that tells the Architect exactly what to build.\n\n"
        "You use the `lookup_metric` tool to find exact SQL formulas. "
        "You NEVER invent formulas. If it's not in the library, you say so.\n\n"
        f"BUSINESS RULES:\n{BUSINESS_CONTEXT}"
    ),
    tools=[lookup_metric],
    llm=llm,
    verbose=True,
    allow_delegation=False,
    max_iter=4
)

sql_architect = Agent(
    role="Senior PostgreSQL Architect",
    goal=(
        "Build ONE clean, tested SQL query from the build plan. "
        "Return both the query AND its results."
    ),
    backstory=(
        "You are the SQL engine of the team. You receive a precise build plan "
        "and translate it into working PostgreSQL. You ALWAYS test your query "
        "before declaring it done. If it fails, you read the error, fix it, "
        "and retry — up to 3 times.\n\n"
        "You can use `explore_schema` to verify column names if uncertain.\n\n"
        f"SCHEMA REFERENCE:\n{SCHEMA_REFERENCE}"
    ),
    tools=[execute_sql, explore_schema],
    llm=llm,
    verbose=True,
    allow_delegation=False,
    max_iter=8
)

analyst_agent = Agent(
    role="Executive Business Analyst",
    goal=(
        "Present SQL results as a clear, professional business answer. "
        "Never re-query. Only interpret."
    ),
    backstory=(
        "You are the voice of the analytics team. You receive verified numbers "
        "and translate them into business language that any stakeholder can understand. "
        "You highlight wins, flag concerns, and provide brief actionable insights.\n\n"
        "You have NO tools. You use ONLY the data handed to you by the Architect."
    ),
    tools=[],
    llm=llm,
    verbose=True,
    allow_delegation=False,
    max_iter=2
)

# ════════════════════════════════════════════════
# CREW ORCHESTRATION
# ════════════════════════════════════════════════

def query_data_agent(question: str):
    """
    Enterprise Chain-of-Thought pipeline.
    Handles any natural language question about the hospitality dataset.
    """
    
    # Phase 1: Discovery
    t1 = Task(
        description=(
            f"{REASONING_ENGINE_PROMPT}\n\n"
            f"{DISCOVERY_PROMPT.format(question=question)}"
        ),
        expected_output=(
            "A bullet list of verified facts:\n"
            "- Time boundaries (latest week, comparison weeks if needed)\n"
            "- Verified entity values (exact DB strings)\n"
            "- Any data quality warnings"
        ),
        agent=data_scout
    )

    # Phase 2: Semantic Mapping
    t2 = Task(
        description=(
            f"{REASONING_ENGINE_PROMPT}\n\n"
            f"{SEMANTIC_PROMPT.format(question=question)}"
        ),
        expected_output=(
            "A structured build plan:\n"
            "- Metrics with exact SQL formulas (from lookup_metric)\n"
            "- Required tables and joins\n"
            "- WHERE filters with verified values\n"
            "- GROUP BY columns if needed\n"
            "- Query pattern (simple/grouped/comparison/complex)"
        ),
        agent=semantic_guardian,
        context=[t1]
    )

    # Phase 3: SQL Construction & Testing
    t3 = Task(
        description=(
            f"{REASONING_ENGINE_PROMPT}\n\n"
            f"{SQL_ARCHITECT_PROMPT}"
        ),
        expected_output=(
            "Two deliverables:\n"
            "1. The final tested SQL query\n"
            "2. The result table showing actual data"
        ),
        agent=sql_architect,
        context=[t2]
    )

    # Phase 4: Business Interpretation
    t4 = Task(
        description=(
            f"{REASONING_ENGINE_PROMPT}\n\n"
            f"{ANALYST_PROMPT.format(question=question)}"
        ),
        expected_output=(
            "A professional business summary with exact numbers, "
            "formatted for a stakeholder audience."
        ),
        agent=analyst_agent,
        context=[t3]
    )

    crew = Crew(
        agents=[data_scout, semantic_guardian, sql_architect, analyst_agent],
        tasks=[t1, t2, t3, t4],
        process=Process.sequential,
        verbose=True,
        memory=True,
        max_rpm=10
    )
    
    result = crew.kickoff()
    
    # Return just the final output string
    return result.raw if hasattr(result, 'raw') else str(result)


# ════════════════════════════════════════════════
# TEST SUITE
# ════════════════════════════════════════════════
if __name__ == "__main__":
    test_questions = [
        # Simple metric
        #"What is the RevPAR for week 31?",
        # Filtered
        "What is the occupancy rate in Delhi in week 30?",
        # Breakdown
        # "What is the booking % share of each booking platform?",
        # WoW comparison
        # "Compare revenue for week 30 vs week 31",
        # Complex
        # "How did our properties in Delhi perform in the latest available week compared to the one before, specifically regarding the conversion of bookings into guests and our revenue yield per room?",
    ]
    
    for q in test_questions:
        print(f"\n{'='*60}")
        print(f"QUESTION: {q}")
        print(f"{'='*60}")
        try:
            response = query_data_agent(q)
            print(f"\n{'─'*60}")
            print(f"ANSWER:\n{response}")
            print(f"{'─'*60}")
        except Exception as e:
            print(f"ERROR: {str(e)}")