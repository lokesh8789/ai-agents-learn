import asyncio
import base64
from typing import AsyncGenerator
import uuid
import httpx

from dotenv import load_dotenv

from fastapi.websockets import WebSocketState
import plivo  # type: ignore

from google.genai.types import (
    Part,
    Blob,
)

from google.adk.runners import InMemoryRunner
from google.adk.agents import LiveRequestQueue
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.genai import types
from google.adk.events import Event

from fastapi import (
    FastAPI,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)

from enum import Enum
from pydantic import BaseModel, Field
from app.agent import root_agent

load_dotenv()

EXOTEL_API_KEY: str = ""
EXOTEL_API_TOKEN: str = ""
EXOTEL_SUB_DOMAIN: str = "api.exotel.com"
EXOTEL_SID: str = ""
EXOTEL_APP_ID: str = ""
EXOTEL_CALLER_ID: str = ""

APP_NAME = "ADK Streaming example"


class EventType(str, Enum):
    connected = "connected"
    start = "start"
    media = "media"
    dtmf = "dtmf"
    stop = "stop"
    mark = "mark"
    clear = "clear"


class MediaFormat(BaseModel):
    encoding: str
    sample_rate: int
    bit_rate: str | None = None


# === Incoming Events ===
class ConnectedEvent(BaseModel):
    event: EventType


class StartPayload(BaseModel):
    stream_sid: str
    call_sid: str
    account_sid: str
    from_: str = Field(..., alias="from")  # alias for reserved word
    to: str
    custom_parameters: dict[str, str] | None = None
    media_format: MediaFormat


class StartEvent(BaseModel):
    event: EventType
    sequence_number: int
    stream_sid: str
    start: StartPayload


class MediaPayloadIn(BaseModel):
    payload: str  # base64 PCM


class MediaEventIn(BaseModel):
    event: EventType
    sequence_number: int
    stream_sid: str
    media: MediaPayloadIn


class DtmfPayload(BaseModel):
    digit: str
    duration: int | None = None


class DtmfEvent(BaseModel):
    event: EventType
    sequence_number: int
    stream_sid: str
    dtmf: DtmfPayload


class StopPayload(BaseModel):
    reason: str | None = None


class StopEvent(BaseModel):
    event: EventType
    sequence_number: int
    stream_sid: str
    stop: StopPayload | None = None


class MarkPayloadIn(BaseModel):
    name: str


class MarkEventIn(BaseModel):
    event: EventType
    sequence_number: int
    stream_sid: str
    mark: MarkPayloadIn


class ClearEventIn(BaseModel):
    event: EventType
    sequence_number: int
    stream_sid: str


# === Outgoing Events ===
class MediaPayloadOut(BaseModel):
    payload: str


class MediaEventOut(BaseModel):
    event: EventType = EventType.media
    stream_sid: str
    media: MediaPayloadOut


class MarkPayloadOut(BaseModel):
    name: str


class MarkEventOut(BaseModel):
    event: EventType = EventType.mark
    stream_sid: str
    mark: MarkPayloadOut


class ClearEventOut(BaseModel):
    event: EventType = EventType.clear
    stream_sid: str


#
# FastAPI web app
#

app = FastAPI()


@app.post("/exotel/outbound")
async def make_outbound_call(to: str):
    base_url = (
        f"https://{EXOTEL_SUB_DOMAIN}/v1/Accounts/{EXOTEL_SID}/Calls/connect.json"
    )

    body = {
        "From": to,
        "CallerId": EXOTEL_CALLER_ID,
        "Url": f"http://my.exotel.com/{EXOTEL_SID}/exoml/start_voice/{EXOTEL_APP_ID}",
    }

    # Basic auth header
    credentials = f"{EXOTEL_API_KEY}:{EXOTEL_API_TOKEN}"
    basic_auth_value = "Basic " + base64.b64encode(credentials.encode("utf-8")).decode(
        "utf-8"
    )

    headers = {
        "Authorization": basic_auth_value,
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                base_url,
                data=body,  # form-data (multipart/form-data)
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code, detail=e.response.text
            )


@app.websocket("/exotel/stream")
async def exotel_stream(websocket: WebSocket):
    print("Exotel stream connected")
    await websocket.accept()

    try:
        # 1. First event should be "connected"
        data: dict = await websocket.receive_json()
        if data.get("event") == "connected":
            print("Got connected event:", data)
        else:
            print("Unexpected first event:", data)

        # 2. Next event should be "start"
        data = await websocket.receive_json()
        start_event = StartEvent(**data)
        print(
            f"Stream started: {start_event.start.stream_sid}, "
            f"rate={start_event.start.media_format.sample_rate}"
        )

        if start_event.event != EventType.start:
            print(f"Unexpected first event: {start_event.event}")
            await websocket.close()
            return

    except WebSocketDisconnect:
        print("Exotel websocket closed before start event.")
        return

    stream_id = start_event.stream_sid or str(uuid.uuid4())
    sample_rate = start_event.start.media_format.sample_rate
    print(f"Stream started: stream_id={stream_id}, sample_rate={sample_rate}")

    # Start agent session
    live_events, live_request_queue = await start_agent_session(stream_id)

    # Launch tasks
    agent_to_exotel_task = asyncio.create_task(
        agent_to_exotel(websocket, live_events, stream_id)
    )
    exotel_to_agent_task = asyncio.create_task(
        exotel_to_agent(websocket, live_request_queue)
    )

    tasks = [agent_to_exotel_task, exotel_to_agent_task]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    live_request_queue.close()

    if websocket.client_state == WebSocketState.CONNECTED:
        await websocket.close()

    print("Exotel disconnected")


async def start_agent_session(
    user_id: str,
) -> tuple[AsyncGenerator[Event, None], LiveRequestQueue]:
    runner = InMemoryRunner(
        app_name=APP_NAME,
        agent=root_agent,
    )

    session = await runner.session_service.create_session(
        app_name=APP_NAME,
        user_id=user_id,
    )

    run_config = RunConfig(
        streaming_mode=StreamingMode.BIDI,
        session_resumption=types.SessionResumptionConfig(),
        speech_config=types.SpeechConfig(
            voice_config=types.VoiceConfig(
                prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name="Kore"),
            ),
        ),
    )

    live_request_queue = LiveRequestQueue()

    live_events = runner.run_live(
        session=session,
        live_request_queue=live_request_queue,
        run_config=run_config,
    )
    print("Agent session started")
    return live_events, live_request_queue


async def agent_to_exotel(
    websocket: WebSocket, live_events: AsyncGenerator[Event, None], stream_id: str
) -> None:
    try:
        async for event in live_events:
            if event.interrupted:
                clear_msg = ClearEventOut(stream_sid=stream_id)
                await websocket.send_json(clear_msg.model_dump(by_alias=True))
                continue

            part: Part | None = None
            if event.content and event.content.parts:
                part = event.content.parts[0]

            if part and part.inline_data and part.inline_data.data:
                audio_data = part.inline_data.data
                msg = MediaEventOut(
                    stream_sid=stream_id,
                    media=MediaPayloadOut(
                        payload=base64.b64encode(audio_data).decode()
                    ),
                )
                await websocket.send_json(msg.model_dump(by_alias=True))
                print("[AGENT->EXOTEL] Sent audio chunk")

    except Exception as e:
        print(f"Exception in agent_to_exotel: {e}")
        raise


async def exotel_to_agent(
    websocket: WebSocket, live_request_queue: LiveRequestQueue
) -> None:
    while True:
        data: dict = await websocket.receive_json()
        event_type = data.get("event")

        if event_type == EventType.media:
            evt = MediaEventIn(**data)
            decoded = base64.b64decode(evt.media.payload)
            live_request_queue.send_realtime(
                Blob(data=decoded, mime_type="audio/pcm;rate=16000")
            )
            print(f"[EXOTEL->AGENT] Received {len(decoded)} bytes")

        elif event_type == EventType.dtmf:
            evt = DtmfEvent(**data)
            print(f"[DTMF] {evt.dtmf.digit}")

        elif event_type == EventType.mark:
            evt = MarkEventIn(**data)
            print(f"[MARK] {evt.mark.name}")

        elif event_type == EventType.stop:
            evt = StopEvent(**data)
            print(f"[STOP] Call ended {evt.stop.reason}")
            break


def _resample_audio(self, audio_data: bytes, from_rate: int, to_rate: int) -> bytes:
    """Resample audio between different sample rates"""
    if from_rate == to_rate:
        return audio_data

    try:
        # Use the media resampler for high-quality resampling
        from app.media_resampler import MediaResampler

        resampler = MediaResampler()

        resampled = resampler.resample_audio(
            audio_data=audio_data,
            from_rate=from_rate,
            to_rate=to_rate,
            channels=1,
            sample_width=2,
        )

        if resampled:
            print(f"🔄 RESAMPLED AUDIO: {from_rate}Hz → {to_rate}Hz")
            return resampled
        else:
            print("⚠️ RESAMPLING FAILED, using original audio")
            return audio_data

    except Exception as e:
        print(f"❌ Error resampling audio: {e}")
        return audio_data
