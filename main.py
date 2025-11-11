import os
from datetime import datetime
from pathlib import Path

from agno.agent import Agent
from agno.models.google import Gemini
from agno.tools.sql import SQLTools
from tools.sequential_thinking_tool import SequentialThinkingTools
from utils.prompts import PROMPT_MAIN
from agno.os import AgentOS


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
    res = agno_agent.run(USER_INPUT)

    markdown_dir = Path("markdown")
    os.makedirs(markdown_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = markdown_dir / f"output_{timestamp}.md"
    with open(output_path, "w", encoding="utf-8") as md_file:
        md_file.write(res.content)
  