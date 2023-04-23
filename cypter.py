import sys
import os
import mmap
import asyncio
from asyncio.queues import Queue

def mmap_read_file_chunks(fh, size):
    while True:
        # file's object current position from the start
        startat = fh.tell()
        # offset from current postion -->1
        fh.seek(size, 1)
        yield startat, fh.tell() - startat

async def mmap_read_input_file(input_file: str, input_queue: Queue, chunk_size: int):
    file_stats = os.stat(input_file)
    if file_stats.st_size < chunk_size:
        chunk_size = file_stats.st_size

    with open(input_file, 'rb', ) as fh:
        #read data with mmap
        with mmap.mmap(fh.fileno(), length=0, access=mmap.ACCESS_COPY) as mmap_read:
            for seek_from, seek_size in mmap_read_file_chunks(mmap_read, chunk_size):
                # chunks start
                mmap_read.seek(seek_from)
                # chunk end
                chunk = fh.read(seek_size)

                if len(chunk) == 0:
                    # no more chunks... stop p
                    await input_queue.put(None)
                    input_queue.task_done()
                    break
                    
                await input_queue.put(chunk)
    return None

#processing chunk
async def encrypt(chunk):
    if chunk is None:
        return None
#    return ('e%s' % chunk)
    return (chunk)

async def process_chunk(input_queue: Queue, output_file: str, chunk_size: int):
    with open(output_file, mode='wb') as f:
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
                f.write(res)
            results.clear()
            print("input_queue.qsize() %s" % input_queue.qsize())
            if input_queue.qsize() <= 1:
                input_queue.task_done()
                return None
    return None

async def main():
    input_file = 'large_file'
    output_file = 'output.txt'
    chunk_size = 3

    input_queue = asyncio.Queue(maxsize=chunk_size)

    input_task = asyncio.create_task(mmap_read_input_file(input_file, input_queue, 10000000))
    process_task = asyncio.create_task(process_chunk(input_queue, output_file, chunk_size))

    await asyncio.gather(input_task, process_task)

asyncio.run(main())
