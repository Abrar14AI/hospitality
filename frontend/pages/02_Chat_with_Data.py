# frontend/pages/02_Chat_with_Data.py
"""
Chat with Your Data — AtliQ Hospitality

Natural language interface to query hotel booking data.
Powered by the same metrics engine as the dashboard.

Features:
  - Full conversation UI with chat history
  - Suggested starter questions
  - Clear chat functionality
  - Thinking indicator
  - Error recovery
"""

import streamlit as st
from agents.agents import query_data_agent


# ════════════════════════════════════════════════
# PAGE CONFIG
# ════════════════════════════════════════════════

st.set_page_config(
    page_title="Chat with Data | AtliQ",
    page_icon="💬",
    layout="wide",
)

st.markdown("""
<style>
    .stChatMessage { max-width: 900px; }
    .suggested-q {
        background-color: #F0F2F6;
        border: 1px solid #E0E0E0;
        border-radius: 8px;
        padding: 8px 12px;
        margin: 4px;
        cursor: pointer;
        font-size: 0.85rem;
    }
    .suggested-q:hover {
        background-color: #E3E8EF;
        border-color: #5D5FEF;
    }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════

st.title("💬 Chat with Your Data")
st.caption(
    "Ask natural language questions about AtliQ hotel booking data. "
    "The AI agent queries the database in real-time using the same metrics engine as the dashboard."
)


# ════════════════════════════════════════════════
# SIDEBAR: HELP & CONTROLS
# ════════════════════════════════════════════════

with st.sidebar:
    st.title("💡 Guide")

    st.markdown("### What you can ask:")
    st.markdown("""
    **Simple KPIs:**
    - "What is the total revenue?"
    - "Show me occupancy rate for Delhi"

    **Comparisons:**
    - "Compare luxury vs business hotels"
    - "Weekend vs weekday performance"

    **Trends:**
    - "Show revenue trend by week"
    - "How has ADR changed over time?"

    **Rankings:**
    - "Which hotel has the highest RevPAR?"
    - "Top 3 cities by occupancy"

    **Week-over-Week:**
    - "Revenue change compared to last week"
    - "WoW occupancy trend for Mumbai"

    **Complex Analysis:**
    - "Which city has best occupancy but worst ADR?"
    - "Revenue per available room by category and city"
    """)

    st.markdown("---")

    st.markdown("### Available Dimensions:")
    st.markdown("""
    - **Cities:** Delhi, Mumbai, Hyderabad, Bangalore
    - **Categories:** Luxury, Business
    - **Room Classes:** Standard, Elite, Premium, Presidential
    - **Day Types:** Weekend (Fri-Sat), Weekday (Sun-Thu)
    - **Months:** May 22, Jun 22, Jul 22
    - **Weeks:** 19 through 32
    """)

    st.markdown("---")

    # Clear chat button
    if st.button("🗑️ Clear Chat History", use_container_width=True):
        st.session_state.chat_messages = []
        st.rerun()

    st.caption("Powered by AtliQ Metrics Engine")


# ════════════════════════════════════════════════
# CHAT STATE
# ════════════════════════════════════════════════

if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []


# ════════════════════════════════════════════════
# SUGGESTED QUESTIONS (shown when chat is empty)
# ════════════════════════════════════════════════

if not st.session_state.chat_messages:
    st.markdown("### 🚀 Quick Start — Try a question:")

    suggestions = [
        "What is the overall revenue, occupancy, and ADR?",
        "Compare luxury vs business hotel performance",
        "Which city has the highest RevPAR?",
        "Show me the weekly revenue trend",
        "What is the cancellation rate by booking platform?",
        "Weekend vs weekday occupancy comparison",
        "Top 5 hotels by revenue in Mumbai",
        "How did RevPAR change week over week for week 31?",
    ]

    # Display in 2 columns
    q_col1, q_col2 = st.columns(2)
    for i, suggestion in enumerate(suggestions):
        col = q_col1 if i % 2 == 0 else q_col2
        with col:
            if st.button(suggestion, key=f"suggest_{i}", use_container_width=True):
                st.session_state.chat_messages.append(
                    {"role": "user", "content": suggestion}
                )
                st.rerun()

    st.markdown("---")


# ════════════════════════════════════════════════
# DISPLAY CHAT HISTORY
# ════════════════════════════════════════════════

# Check if last message needs a response
needs_response = False
if st.session_state.chat_messages:
    last_msg = st.session_state.chat_messages[-1]
    needs_response = last_msg["role"] == "user"

# Display all existing messages
for message in st.session_state.chat_messages:
    if message["role"] == "user":
        with st.chat_message("user"):
            st.markdown(message["content"])
    elif message["role"] == "assistant":
        with st.chat_message("assistant", avatar="🏨"):
            st.markdown(message["content"])

# If last user message has no response yet, generate one
if needs_response:
    user_question = st.session_state.chat_messages[-1]["content"]

    with st.chat_message("assistant", avatar="🏨"):
        with st.spinner("🔍 Analyzing your question..."):
            try:
                response = query_data_agent(user_question)

                if not response or response.strip() == "":
                    response = "I couldn't generate an answer. Please try rephrasing your question."

                st.markdown(response)
                st.session_state.chat_messages.append(
                    {"role": "assistant", "content": response}
                )
            except Exception as e:
                error_msg = (
                    f"⚠️ I encountered an error while processing your question:\n\n"
                    f"`{str(e)}`\n\n"
                    f"Please try rephrasing or ask a simpler question."
                )
                st.error(error_msg)
                st.session_state.chat_messages.append(
                    {"role": "assistant", "content": error_msg}
                )


# ════════════════════════════════════════════════
# CHAT INPUT
# ════════════════════════════════════════════════

if prompt := st.chat_input("Ask about hotel performance, KPIs, trends..."):
    # Add user message
    st.session_state.chat_messages.append({"role": "user", "content": prompt})
    st.rerun()