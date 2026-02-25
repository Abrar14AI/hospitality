# pages/02_Chat_with_Data.py
import streamlit as st
from agents.agents_system import query_data_agent

st.title("💬 Chat with Your Data")
st.markdown("Ask natural language questions about the hotel booking data (e.g., 'Which city has the highest occupancy rate?', 'Show me revenue trend for AtliQ Palace')")

# Initialize chat history
if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []

# Display chat history
for message in st.session_state.chat_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask your question..."):
    # Add user message to history
    st.session_state.chat_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get agent response
    with st.chat_message("assistant"):
        with st.spinner("Agent is analyzing..."):
            try:
                # Call your existing agent (no memory, single argument)
                response = query_data_agent(prompt)
                st.markdown(response)
                st.session_state.chat_messages.append({"role": "assistant", "content": response})
            except Exception as e:
                error_msg = f"Error during agent execution: {str(e)}"
                st.error(error_msg)
                st.session_state.chat_messages.append({"role": "assistant", "content": error_msg})