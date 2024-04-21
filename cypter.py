import os
import mmap
import time
import asyncio
from asyncio.queues import Queue


def mmap_read_file_chunks(fh, size):
    while True:
        # file's object current position from the start
        start_pos = fh.tell()
        # offset from current position -->1
        fh.seek(size, 1)
        yield start_pos, fh.tell() - start_pos


async def read_input_file(input_file: str, input_queue: Queue, file_data_chunk_size: int):
    file_stats = os.stat(input_file)
    if file_stats.st_size < file_data_chunk_size:
        file_data_chunk_size = file_stats.st_size

    with open(input_file, 'rb', ) as fh:
        # read data with mmap
        with mmap.mmap(fh.fileno(), length=0, access=mmap.ACCESS_COPY) as mmap_read:
            for seek_from, seek_size in mmap_read_file_chunks(mmap_read, file_data_chunk_size):
                # chunks start
                mmap_read.seek(seek_from)
                # chunk end
                chunk = fh.read(seek_size)

                if chunk is None or len(chunk) == 0:
                    # no more chunks... stop p
                    await input_queue.put(None)
                    break

                await input_queue.put(chunk)

    print("read done")
    await input_queue.put(None)
    return None


# processing chunk from file
def process_file_chunk(file_data_chunk):
    if file_data_chunk is None:
        return None
    # TODO: implement process data, encrypt, send to db, etc.
    return file_data_chunk


async def processor(input_queue: Queue, output_queue: Queue):
    while True:
        file_data_chunk = await input_queue.get()
        input_queue.task_done()
        if file_data_chunk is not None:
            # print("More chunks to process")
            await output_queue.put(process_file_chunk(file_data_chunk))
        else:
            break
    print("processor done")
    await output_queue.put(None)
    return None


async def write_output_file(input_queue: Queue, output_file: str):
    file_size = 0
    with open(output_file, 'a+b') as file:
        while True:
            chunk = await input_queue.get();
            input_queue.task_done()
            if chunk is not None and len(chunk) != 0:
                file_size += len(chunk)
                with mmap.mmap(file.fileno(), file_size) as mm:
                    mm.write(chunk)
                    mm.flush()
            else:
                print("write done2")
                return None

    print("write done")
    return None


def prepare_output_file(output_file: str):
    with open(output_file, 'w') as f:
        f.truncate(0)


async def process_file():
    start = time.time()
    input_file = 'input_file'
    output_file = 'output_file'
    queue_size = 10
    file_data_chunk_size = 10000000
    input_queue = asyncio.Queue(maxsize=queue_size)
    output_queue = asyncio.Queue(maxsize=queue_size)

    prepare_output_file(output_file)

    write_task = asyncio.create_task(write_output_file(output_queue, output_file))
    input_task = asyncio.create_task(read_input_file(input_file, input_queue, file_data_chunk_size))
    processor_task = asyncio.create_task(processor(input_queue, output_queue))

    await asyncio.gather(input_task, processor_task, write_task)

    end = time.time() - start
    print("Time difference:", end, "seconds")


if __name__ == "__main__":
    asyncio.run(process_file())
