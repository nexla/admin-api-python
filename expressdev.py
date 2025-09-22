
from snowglobe.client import CompletionRequest, CompletionFunctionOutputs
import logging
import websockets
import json
from openai import AsyncOpenAI

LOGGER = logging.getLogger(__name__)
socket_cache = {}
openai_client = AsyncOpenAI()

async def acompletion(request: CompletionRequest) -> CompletionFunctionOutputs:
    """
    When dealing with a realtime socket, we need to create a socket for each conversation.
    We store the socket in a cache and reuse it for the same conversation_id so that we can maintain the conversation context.
    Swap out the websocket client for your preferred realtime client.

    Args:
        request (CompletionRequest): The request object containing messages for the test.

    Returns:
        CompletionFunctionOutputs: The response object with the generated content.
    """
    conversation_id = request.get_conversation_id()
    
    if conversation_id not in socket_cache:
        socket = await websockets.connect(
            "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01&modalities=text",
            additional_headers={
                "Authorization": f"Bearer {openai_client.api_key}",
                "OpenAI-Beta": "realtime=v1"
            }
        )
        socket_cache[conversation_id] = socket
    else:
        socket = socket_cache[conversation_id]
    
    # Send user message
    messages = request.to_openai_messages()
    user_message = messages[-1]["content"]
    
    await socket.send(json.dumps({
        "type": "conversation.item.create",
        "session": {
                "modalities": ["text"],  # Only text, no audio
        },
        "item": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": user_message}]
        }
    }))
    
    await socket.send(json.dumps({"type": "response.create"}))
    
    # Get response
    response_content = ""
    async for message in socket:
        data = json.loads(message)
        if data.get("type") == "response.audio_transcript.delta":
            response_content += data.get("delta", "")
        elif data.get("type") == "response.done":
            break
    
    return CompletionFunctionOutputs(response=response_content)
