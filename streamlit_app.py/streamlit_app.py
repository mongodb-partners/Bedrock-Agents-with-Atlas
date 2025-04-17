import streamlit as st
import boto3

# Set page configuration
st.set_page_config(
    page_title="MongoDB Atlas Travel Assistant",
    page_icon="🌍",
    layout="wide"
)

# Initialize session state for chat history if it doesn't exist
if "messages" not in st.session_state:
    st.session_state.messages = []

def initialize_bedrock_agent_client():
    """Initialize the Bedrock agent runtime client."""
    return boto3.client(
        service_name="bedrock-agent-runtime",
        region_name="us-east-1"
    )

def get_agent_response(agent_client, agent_id, user_input):
    """Get response from the Bedrock agent."""
    try:
        response = agent_client.invoke_agent(
            agentId=agent_id,
            agentAliasId="<ALIASID>",  # You might need to adjust this
            sessionId="streamlit-session",
            inputText=user_input
        )
        
        # Extract the response from the agent
        response_stream = response.get('completion')
        if response_stream:
            result = b''
            for event in response_stream:
                print(f"got event: {str(event)}")
                chunk = event.get('chunk')
                if chunk:
                    result += chunk.get('bytes', b'')
            
            return result.decode('utf-8')
        return "No response from agent"
    
    except Exception as e:
        st.error(f"Error invoking Bedrock agent: {str(e)}")
        return f"Error: {str(e)}"

# Main app
def main():
    # App header
    st.title("🌍 MongoDB Atlas Travel Assistant")
    st.markdown("""
    Ask questions about travel destinations, best times to visit, and more!
    """)
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        agent_id = st.text_input("Bedrock Agent ID", value="<AGENTID>")
        
        # Add a clear chat button
        if st.button("Clear Chat"):
            st.session_state.messages = []
            st.rerun()
    
    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about travel destinations..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
        
        # Display assistant response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                # Initialize Bedrock agent client
                agent_client = initialize_bedrock_agent_client()
                
                # Get response from Bedrock agent
                response = get_agent_response(agent_client, agent_id, prompt)
                
                # Display the response
                st.write(response)
        
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": response})

if __name__ == "__main__":
    main()
