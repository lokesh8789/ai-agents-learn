from dotenv import load_dotenv

import asyncio
from livekit import agents
from livekit import api, rtc
import subprocess
import numpy as np
from livekit.rtc import (
    Room,
    RemoteAudioTrack,
    RemoteTrackPublication,
    RemoteParticipant,
    AudioStream,
    AudioFrame,
)
from livekit.agents import (
    NOT_GIVEN,
    AgentFalseInterruptionEvent,
    AgentSession,
    Agent,
    RoomInputOptions,
    get_job_context,
    function_tool,
    RunContext,
    JobProcess,
)
from livekit.plugins import (
    openai,
    cartesia,
    deepgram,
    noise_cancellation,
    silero,
    google,
)
from livekit.plugins.turn_detector.multilingual import MultilingualModel
from google.genai import types
import json
import os
import random
import threading

load_dotenv(".env.local")
SIP_OUTBOUND_TRUNK_ID = os.getenv("SIP_OUTBOUND_TRUNK_ID")
LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET")
LIVEKIT_URL = os.getenv("LIVEKIT_URL")


instruction = """

"""


class Assistant(Agent):
    def __init__(self) -> None:
        super().__init__(instructions=instruction)
        # keep reference to the participant for transfers
        self.participant: rtc.RemoteParticipant | None = None

    def set_participant(self, participant: rtc.RemoteParticipant):
        self.participant = participant

    async def hangup(self):
        """Helper function to hang up the call by deleting the room"""

        job_ctx = get_job_context()
        await job_ctx.api.room.delete_room(
            api.DeleteRoomRequest(
                room=job_ctx.room.name,
            )
        )

    @function_tool()
    async def end_call(self, ctx: RunContext):
        """Called when the user wants to end the call"""
        print("ending the call")

        # let the agent finish speaking
        # current_speech = ctx.session.current_speech
        # if current_speech:
        #     await current_speech.wait_for_playout()
        await ctx.wait_for_playout()
        await self.hangup()


def get_dial_info(ctx: agents.JobContext) -> dict:
    dial_info = {}
    if ctx.job.metadata:
        try:
            dial_info = json.loads(ctx.job.metadata)
        except json.JSONDecodeError:
            print(f"Invalid metadata: {ctx.job.metadata}")
            dial_info = {}
    return dial_info


async def initiate_outbound_call(
    ctx: agents.JobContext, phone_number, session: AgentSession, agent: Assistant
):
    participant_identity = phone_number
    session_started = asyncio.create_task(
        session.start(
            room=ctx.room,
            agent=agent,
            room_input_options=RoomInputOptions(
                # For telephony applications, use `BVCTelephony` instead for best results
                noise_cancellation=noise_cancellation.BVCTelephony(),
            ),
        )
    )
    try:
        await session_started
        await ctx.api.sip.create_sip_participant(
            api.CreateSIPParticipantRequest(
                room_name=ctx.room.name,
                sip_trunk_id=SIP_OUTBOUND_TRUNK_ID,
                sip_call_to=phone_number,
                participant_identity=participant_identity,
                wait_until_answered=True,
            )
        )
        participant = await ctx.wait_for_participant(identity=participant_identity)
        print(f"participant joined: {participant.identity}")

        agent.set_participant(participant)

        print("call picked up successfully")
    except api.TwirpError as e:
        print(
            f"error creating SIP participant: {e.message}, "
            f"SIP status: {e.metadata.get('sip_status_code')} "
            f"{e.metadata.get('sip_status')}"
        )
        ctx.shutdown()


async def initiate_inbound_call(
    ctx: agents.JobContext, session: AgentSession, agent: Assistant
):
    await session.start(
        room=ctx.room,
        agent=agent,
        room_input_options=RoomInputOptions(
            # For telephony applications, use `BVCTelephony` instead for best results
            noise_cancellation=noise_cancellation.BVCTelephony(),
        ),
    )
    # participant = list(ctx.room.remote_participants.values())[0]
    # agent.set_participant(participant)


async def entrypoint(ctx: agents.JobContext):
    # await ctx.connect()
    dial_info = get_dial_info(ctx)
    phone_number = dial_info.get("phone_number")

    session = AgentSession(
        # llm=openai.realtime.RealtimeModel(
        #     voice="coral"
        # )
        llm=google.beta.realtime.RealtimeModel(
            voice="Kore", model="gemini-2.5-flash-native-audio-preview-09-2025"
        ),
        # stt=deepgram.STT(model="nova-3", language="multi"),
        # llm=openai.LLM(model="gpt-4o-mini"),
        # tts=cartesia.TTS(
        #     model="sonic-2",
        #     # voice="f786b574-3e8534c02",
        #     language="hi",
        # ),
        # stt=google.STT(),
        # llm=google.LLM(),
        # tts=google.TTS(),
        # tts =google.beta.GeminiTTS(
        #     model="gemini-2.5-flash-preview-tts",
        #     voice_name="Zephyr",
        #     instructions="Speak in a friendly and engaging tone.",
        # ),
        # stt=openai.STT(model="gpt-4o-transcribe"),
        # tts=openai.TTS(model="gpt-4o-mini-tts", voice="alloy"),
        # vad=ctx.proc.userdata["vad"],
        # turn_detection=MultilingualModel(),
        # preemptive_generation=True,
    )

    def _on_agent_false_interruption(ev: AgentFalseInterruptionEvent) -> None:
        session.generate_reply(instructions=ev.extra_instructions or NOT_GIVEN)

    session.on(event="agent_false_interruption", callback=_on_agent_false_interruption)

    agent = Assistant()

    # recorder = Recorder(ctx.room.name)
    # recorder.attach(ctx.room)

    if phone_number is not None:
        await initiate_outbound_call(ctx, phone_number, session, agent)
    else:
        await initiate_inbound_call(ctx, session, agent)

    await session.generate_reply(
        instructions="Greet the user and offer your assistance.",
        # allow_interruptions=False,
    )


async def run_schedule():
    async with api.LiveKitAPI(
        url=LIVEKIT_URL,
        api_key=LIVEKIT_API_KEY,
        api_secret=LIVEKIT_API_SECRET,
    ) as lkapi:
        phone_number = "+915"
        room_name = f"scheduled-{''.join(str(random.randint(0,9)) for _ in range(8))}"
        print(f"📞 [Scheduler] Outbound call to {phone_number}")
        try:
            await lkapi.agent_dispatch.create_dispatch(
                api.CreateAgentDispatchRequest(
                    agent_name="Vaani",
                    room=room_name,
                    metadata=json.dumps({"phone_number": phone_number}),
                )
            )

            # await lkapi.sip.create_sip_outbound_trunk(
            #     create=api.CreateSIPOutboundTrunkRequest(
            #         trunk=api.SIPOutboundTrunkInfo(
            #             destination_country="in",
            #             address="",
            #             auth_username="",
            #             auth_password="",
            #             numbers=[""],
            #             name="Indian Outbound Trunk",
            #         )
            #     )
            # )
        except Exception as e:
            print(f"⚠️ Failed to dispatch call: {e}")


def prewarm(proc: JobProcess):
    proc.userdata["vad"] = silero.VAD.load()


if __name__ == "__main__":
    # threading.Thread(target=run_schedule, daemon=True).start()
    agents.cli.run_app(
        agents.WorkerOptions(
            entrypoint_fnc=entrypoint,
            agent_name="John",
            # prewarm_fnc=prewarm,
        ),
        hot_reload=False,
    )
    # asyncio.run(run_schedule())


# def call_schedule():
#     # Run forever in a separate thread
#     print("Call Schduler Get Called", flush=True)
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)

#     async def run_schedule():
#         async with api.LiveKitAPI(
#             url=LIVEKIT_URL,
#             api_key=LIVEKIT_API_KEY,
#             api_secret=LIVEKIT_API_SECRET,
#         ) as lkapi:
#             await asyncio.sleep(30)  # run every 30s
#             phone_number = ""
#             room_name = (
#                 f"scheduled-{''.join(str(random.randint(0,9)) for _ in range(8))}"
#             )
#             print(f"📞 [Scheduler] Outbound call to {phone_number}")

#             try:
#                 await lkapi.agent_dispatch.create_dispatch(
#                     api.CreateAgentDispatchRequest(
#                         agent_name="John",
#                         room=room_name,
#                         metadata=json.dumps({"phone_number": phone_number}),
#                     )
#                 )
#             except Exception as e:
#                 print(f"⚠️ Failed to dispatch call: {e}")

#     loop.run_until_complete(run_schedule())
#     loop.close()


# DOWNLOADS_DIR = os.path.expanduser("~/Downloads")


# class Recorder:
#     def __init__(self, room_name: str):
#         self.room_name = room_name
#         self.output_file = os.path.join(DOWNLOADS_DIR, f"{room_name}.mp3")

#         # ffmpeg process: expects PCM16 (s16le)
#         self.ffmpeg = subprocess.Popen(
#             [
#                 "ffmpeg",
#                 "-y",
#                 "-f",
#                 "s16le",  # we will feed int16 PCM
#                 "-ar",
#                 "48000",
#                 "-ac",
#                 "1",
#                 "-i",
#                 "-",  # stdin
#                 "-codec:a",
#                 "libmp3lame",
#                 "-ar",
#                 "48000",
#                 "-ac",
#                 "1",
#                 self.output_file,
#             ],
#             stdin=subprocess.PIPE,
#         )

#     def attach(self, room: Room):
#         def on_track(
#             track: RemoteAudioTrack,
#             pub: RemoteTrackPublication,
#             participant: RemoteParticipant,
#         ):
#             print(f"🎙️ Subscribed to audio from {participant.identity}")

#             stream = AudioStream(track)

#             async def read_audio():
#                 async for event in stream:
#                     frame: AudioFrame = event.frame

#                     # Convert memoryview -> numpy float32 array
#                     float32_array = np.frombuffer(frame.data, dtype=np.float32)

#                     # Scale to PCM16
#                     pcm16 = (float32_array * 32767).astype(np.int16).tobytes()

#                     # Send to ffmpeg
#                     self.ffmpeg.stdin.write(pcm16)

#             _ = asyncio.create_task(read_audio())

#         room.on("track_subscribed", on_track)

#     def close(self):
#         if self.ffmpeg.stdin:
#             self.ffmpeg.stdin.close()
#         self.ffmpeg.wait()
#         print(f"💾 Saved recording: {self.output_file}")
