import os
import json
import asyncio
import base64
from typing import AsyncGenerator
import uuid
import warnings

from pathlib import Path
from dotenv import load_dotenv

from fastapi.websockets import WebSocketState
import plivo  # type: ignore

from google.genai.types import (
    Part,
    Content,
    Blob,
)

from google.adk.runners import InMemoryRunner
from google.adk.agents import LiveRequestQueue
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.genai import types
from google.adk.events import Event

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.agent import root_agent

load_dotenv()

NGROK_URL = "74dccb.ngrok-free.app"
PLIVO_AUTH_ID = ""
PLIVO_AUTH_TOKEN = ""
PLIVO_PHONE_NUMBER = ""

APP_NAME = "ADK Streaming example"

#
# FastAPI web app
#

app = FastAPI()

plivo_client = plivo.RestClient(auth_id=PLIVO_AUTH_ID, auth_token=PLIVO_AUTH_TOKEN)


@app.post("/plivo/outbound")
def make_outbound_call(to: str):
    response = plivo_client.calls.create(
        from_=PLIVO_PHONE_NUMBER,
        to_=to,
        answer_url=f"https://{NGROK_URL}/plivo/answer",  # where Plivo asks for XML
        answer_method="GET",
    )
    return response


@app.get("/plivo/answer")
async def plivo_answer():
    #  <Record recordSession="true" maxLength="86400"/>
    xml_response = f"""
    <?xml version="1.0" encoding="UTF-8"?>
        <Response>
            <Stream streamTimeout="86400" keepCallAlive="true" bidirectional="true" contentType="audio/x-l16;rate=16000" audioTrack="inbound" >
            wss://{NGROK_URL}/plivo/stream
            </Stream>
        </Response>
    """
    return Response(
        content=xml_response,
        media_type="application/xml",
    )


@app.websocket("/plivo/stream")
async def plivo_stream(websocket: WebSocket):
    await websocket.accept()
    print("Plivo stream connected")

    # Read initial 'start' message from Plivo to learn streamId / media format
    try:
        start_msg: dict = await websocket.receive_json()
    except WebSocketDisconnect:
        print("Plivo websocket closed before start event.")
        return

    # Plivo sends an event "start" with 'start' object having streamId and mediaFormat.
    # Example: {"sequenceNumber":0,"event":"start","start":{"callId": "...","streamId":"...","mediaFormat":{"encoding":"audio/x-l16","sampleRate":8000}, ...}}
    stream_info = start_msg.get("start", {}) or {}
    stream_id = str(stream_info.get("streamId") or start_msg.get("streamId"))
    media_format = stream_info.get("mediaFormat", {}) or {}
    plivo_encoding = media_format.get("encoding")
    plivo_sample_rate = int(media_format.get("sampleRate", 16000))

    print(
        f"Plivo stream started: stream_id={stream_id} encoding={plivo_encoding} sample_rate={plivo_sample_rate}"
    )

    # Start Google ADK live session
    live_events, live_request_queue = await start_agent_session(
        user_id=(stream_id or uuid.uuid4())
    )

    # Start tasks
    agent_to_client_task = asyncio.create_task(
        agent_to_plivo(websocket, live_events, stream_id)
    )

    client_to_agent_task = asyncio.create_task(
        plivo_to_agent(websocket, live_request_queue)
    )

    # Wait until the websocket is disconnected or an error occurs
    tasks: list[asyncio.Task[None]] = [agent_to_client_task, client_to_agent_task]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    # for t in pending:
    #     t.cancel()

    # Close LiveRequestQueue
    live_request_queue.close()

    if websocket.client_state == WebSocketState.CONNECTED:
        await websocket.close()

    # Disconnected
    print("Client disconnected")


async def start_agent_session(
    user_id,
) -> tuple[AsyncGenerator[Event, None], LiveRequestQueue]:
    """Starts an agent session"""

    # Create a Runner
    runner = InMemoryRunner(
        app_name=APP_NAME,
        agent=root_agent,
    )

    # Create a Session
    session = await runner.session_service.create_session(
        app_name=APP_NAME,
        user_id=user_id,  # Replace with actual user ID
    )

    # Set response modality
    # modality = "AUDIO"
    run_config = RunConfig(
        # response_modalities=[modality],
        streaming_mode=StreamingMode.BIDI,
        # max_llm_calls=0,
        session_resumption=types.SessionResumptionConfig(),
        speech_config=types.SpeechConfig(
            voice_config=types.VoiceConfig(
                prebuilt_voice_config=types.PrebuiltVoiceConfig(
                    voice_name="Kore",
                ),
            ),
        ),
    )

    # Create a LiveRequestQueue for this session
    live_request_queue = LiveRequestQueue()

    # Start agent session
    live_events = runner.run_live(
        session=session,
        live_request_queue=live_request_queue,
        run_config=run_config,
    )
    print("Call Connected")

    return live_events, live_request_queue


async def agent_to_plivo(
    websocket: WebSocket,
    live_events: AsyncGenerator[Event, None],
    stream_id: str,
) -> None:
    """Agent to client communication"""
    try:
        async for event in live_events:
            print(f"Received Event: {event}")
            # If the turn complete or interrupted, send it
            if event.interrupted:
                message = {
                    "event": "clearAudio",
                    "streamId": stream_id,
                }
                await websocket.send_json(message)
                # print(f"[AGENT TO CLIENT]: {message}")
                continue

            # Read the Content and its first Part
            part: Part | None = None

            if event and event.content and event.content.parts:
                # print(f"EVENT PART: {event.content.parts}")
                part = event.content.parts[0]

            if not part:
                continue

            audio_data = part.inline_data and part.inline_data.data

            if audio_data:
                message = {
                    "event": "playAudio",
                    "media": {
                        "contentType": "audio/x-l16",
                        "sampleRate": 24000,
                        "payload": base64.b64encode(audio_data).decode("utf-8"),
                    },
                }
                await websocket.send_json(message)
                print("[AGENT->PLIVO] Sent audio chunk")
    except Exception as e:
        print(f"Exception Occurred From Gemini Side: {e}")
        raise


async def plivo_to_agent(
    websocket: WebSocket, live_request_queue: LiveRequestQueue
) -> None:
    """Plivo to agent communication"""
    while True:
        data: dict = await websocket.receive_json()
        if websocket.client_state == WebSocketState.CONNECTED:
            data = await websocket.receive_json()

        if data and data.get("event") == "media":
            audio_b64 = data["media"]["payload"]
            decoded = base64.b64decode(audio_b64)
            live_request_queue.send_realtime(
                Blob(
                    data=decoded,
                    mime_type="audio/pcm;rate=16000",
                )
            )
            print(f"[PLIVO->AGENT] Received audio: {len(decoded)} bytes")

        elif data.get("event") in ["start", "stop"]:
            print(f"[PLIVO EVENT] {data}")
