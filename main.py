import os
from datetime import datetime
from pathlib import Path

from agno.agent import Agent
from agno.models.google import Gemini
from agno.tools.sql import SQLTools
from tools.sequential_thinking_tool import SequentialThinkingTools
from utils.prompts import PROMPT_MAIN
from agno.os import AgentOS


def handle_greeting(user_input: str) -> str | None:
    """
    Handle simple greetings and return a friendly welcome message.
    
    Args:
        user_input: The user's input string
        
    Returns:
        A welcome message string if greeting detected, None otherwise
    """
    greetings = ["hi", "hello", "hey", "greetings", "good morning", "good afternoon", "good evening"]
    user_lower = user_input.lower().strip()
    
    # Check if input is exactly a greeting or starts with a greeting
    is_greeting = (
        user_lower in greetings or
        any(user_lower.startswith(greeting + " ") for greeting in greetings) or
        any(user_lower.startswith(greeting + ",") for greeting in greetings)
    )
    
    if is_greeting:
        return """# Welcome to the Matchmaking Agent!

I'm here to help you pair SEC Form D / Reg CF deals with Form ADV advisers using our comprehensive database.

## What I Can Do

- **Find advisers for a deal**: Provide a Form D accession number, and I'll find the top matching advisers
- **Find deals for an adviser**: Provide an adviser FilingID, and I'll show suitable deals
- **Analyze fit scores**: I'll explain why advisers match specific deals based on geography, capital, audience, and more
- **Query the database**: Ask about specific deals, advisers, or market trends

## Example Queries

- "Find the top 5 advisers for Form D accession 0000005108-25-000002"
- "Show deals that adviser FilingID 1620806 is a fit for"
- "List Reg CF offerings that allow retail investors"

How can I assist you today?"""
    return None


db_path = Path("data/staging.sqlite").resolve()
db_url = f"sqlite:///{db_path}"

agno_agent = Agent(
    name="Match Making Agent",
    model=Gemini(id="gemini-2.5-flash", api_key=""),
    tools=[SequentialThinkingTools(), SQLTools(db_url=db_url)],
    add_history_to_context=True,
    markdown=True,
    debug_mode=True,
    system_message=PROMPT_MAIN
)


if __name__ == "__main__":

    USER_INPUT = """
Find the top 5 advisers for Form D accession 0000005108-25-000002. 
Return advisers details. 
Explain briefly why each adviser is a good fit.
"""
    USER_INPUT_2 = """
Find the top 5 advisers for Form D accession 0000005108-25-000002. 
Return adviser_id, adviser_name, adviser_city, adviser_state, total_raum, composite_score,
plus the geography/capital/audience component scores. 
Explain briefly why each adviser is a good fit.
"""
    
    # Check if input is a simple greeting
    greeting_response = handle_greeting(USER_INPUT)
    
    if greeting_response:
        # Handle greeting without invoking the agent
        print(greeting_response)
        res_content = greeting_response
    else:
        # Run the agent for actual queries
        res = agno_agent.run(USER_INPUT)
        res_content = res.content

    markdown_dir = Path("markdown")
    os.makedirs(markdown_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = markdown_dir / f"output_{timestamp}.md"
    with open(output_path, "w", encoding="utf-8") as md_file:
        md_file.write(res_content)
  