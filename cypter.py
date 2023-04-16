import asyncio
from asyncio.queues import Queue


async def read_input_file(input_file: str, input_queue: Queue):
    with open(input_file, mode='r') as f:
        while True:
            chunk = f.readline()
            if not chunk:
                await input_queue.put(None)
                input_queue.task_done()
                break
            await input_queue.put(chunk.strip())
    return None

async def encrypt(chunk):
    if chunk is None:
        return None
    return ('e%s' % chunk)

async def process_chunk(input_queue: Queue, output_file: str, chunk_size: int):
    with open(output_file, mode='w') as f:
        while True:
            tasks = []
            for i in range (chunk_size):
                chunk = await input_queue.get()
                if chunk is not None:
                    tasks.append( asyncio.create_task(encrypt(chunk)) )
            results = await asyncio.gather(*tasks)

            for res in results:
                if(res is None):
                    print("processing chunks done")
                    input_queue.task_done()
                    return None
                f.write(res + '\n')
            results.clear()
            print("input_queue.qsize() %s" % input_queue.qsize())
            if input_queue.qsize() <= 1:
                input_queue.task_done()
                return None
    return None

async def main():
    input_file = 'input.txt'
    output_file = 'output.txt'
    chunk_size = 3

    input_queue = asyncio.Queue(maxsize=chunk_size)

    input_task = asyncio.create_task(read_input_file(input_file, input_queue))
    process_task = asyncio.create_task(process_chunk(input_queue, output_file, chunk_size))

    await asyncio.gather(input_task, process_task)

asyncio.run(main())