import trio

async def requester(request_send_channel):
    for i in range(3):
        # Create a unique response channel for this request
        response_send_channel, response_receive_channel = trio.open_memory_channel(0)
        
        request = {"id": i, "data": f"Request {i}", "response_channel": response_send_channel}
        
        await request_send_channel.send(request)
        print(f"Requester: Sent request {i}")
        
        # Await the response on the dedicated channel
        response = await response_receive_channel.receive()
        print(f"Requester: Received response for request {response['id']}: {response['result']}")

async def responder(request_receive_channel):
    async for request in request_receive_channel:
        print(f"Responder: Received request {request['id']}: {request['data']}")
        
        # Process the request
        result = f"Processed {request['data']}"
        response = {"id": request['id'], "result": result}
        
        # Send the response back through the dedicated channel
        await request['response_channel'].send(response)
        await request['response_channel'].aclose() # Close the response channel after sending

async def main():
    request_send_channel, request_receive_channel = trio.open_memory_channel(0)
    
    async with trio.open_nursery() as nursery:
        nursery.start_soon(requester, request_send_channel)
        nursery.start_soon(responder, request_receive_channel)

if __name__ == "__main__":
    trio.run(main)