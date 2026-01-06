from google.adk.agents import Agent
from google.adk.tools import google_search  # Import the tool


instruction = """

"""


root_agent = Agent(
    name="google_search_agent",
    # model="gemini-live-2.5-flash-preview",
    model="gemini-2.5-flash-native-audio-preview-09-2025",
    # model="gemini-2.0-flash-live-001",
    description="Agent to answer questions using Google Search.",
    instruction=instruction,
    # tools=[google_search],
)
