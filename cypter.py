import os
import mmap
import time
import asyncio
from asyncio.queues import Queue


def mmap_read_file_chunks(fh, size):
    while True:
        # Record the current position in the file from the start
        start_pos = fh.tell()
        # Move the file pointer forward by 'size' bytes from the current position
        fh.seek(size, 1)
        # Calculate and yield the start position and the number of bytes read
        yield start_pos, fh.tell() - start_pos


async def read_input_file(input_file: str, input_queue: Queue, file_data_chunk_size: int):
    # Retrieve file statistics
    file_stats = os.stat(input_file)
    # Adjust chunk size if it exceeds the file size
    if file_stats.st_size < file_data_chunk_size:
        file_data_chunk_size = file_stats.st_size

    # Open the input file for reading in binary mode
    with open(input_file, "rb") as fh:
        # Memory-map the file for efficient reading
        with mmap.mmap(fh.fileno(), length=0, access=mmap.ACCESS_COPY) as mmap_read:
            # Iterate over chunks using memory-mapped file
            for seek_from, seek_size in mmap_read_file_chunks(
                mmap_read, file_data_chunk_size
            ):
                # Move to the starting position of the chunk
                mmap_read.seek(seek_from)
                # Read the chunk data from the file
                chunk = fh.read(seek_size)

                # Check if the chunk is empty
                if chunk is None or len(chunk) == 0:
                    # No more chunks, signal the end
                    await input_queue.put(None)
                    break

                # Put the chunk into the input queue for processing
                await input_queue.put(chunk)

    # Indicate that reading is done
    print("Read operation completed")
    # Signal the end of input by putting None into the queue
    await input_queue.put(None)
    return None


                            
def process_file_chunk(file_data_chunk):
    # Processchunk of data from the file
    if file_data_chunk is None:
        # Return None if the chunk is empty
        return None
    # TODO: Implement data processing (e.g., encryption, sending todatabase, etc.)
    # For now, simply return the chunk as is
    return file_data_chunk


async def processor(input_queue: Queue, output_queue: Queue):
    # Continuously process data from the input queue and write results to the output queue
    while True:
        # Retrievechunk of data from the input queue
        file_data_chunk = await input_queue.get()
        # Mark the task as done to maintain the queue's integrity
        input_queue.task_done()

        # Check if there is data to process
        if file_data_chunk is not None:
            # Process the data chunk and put the result into the output queue
            await output_queue.put(process_file_chunk(file_data_chunk))
        else:
            # If there's no more data, exit the loop
            break
    
    # Indicate that processing is complete
    print("Processor completed")
    # Put None into the output queue to signal the end of processing
    await output_queue.put(None)
    return None


async def write_output_file(input_queue: Queue, output_file: str):
    # Initialize the file size
    file_size = 0
    # Open the output file in append and binary modes
    with open(output_file, "a+b") as file:
        # Continuously write data from the input queue to the output file
        while True:
            # Retrievechunk of data from the input queue
            chunk = await input_queue.get()
            # Mark the task as done to maintain the queue's integrity
            input_queue.task_done()

            # Check if the chunk is not empty
            if chunk is not None and len(chunk) != 0:
                # Update the file size
                file_size += len(chunk)
                # Memory-map the file and write the chunk to it
                with mmap.mmap(file.fileno(), file_size) as mm:
                    mm.write(chunk)
                    mm.flush()
            else:
                # If there's no more data, exit the loop
                print("Write operation completed")
                return None

    # This point should not be reached if the loop is exited correctly,
    # but if it does, it indicates the completion of writing
    print("Write operation completed")
    return None


def prepare_output_file(output_file: str):
    # Prepare the output file by removing any existing data
    with open(output_file, "w") as f:
        f.truncate(0)


async def process_file():
    # Orchestrates the entire process by creating input and output queues,
    # preparing the output file, and initiating tasks for reading input,
    # processing, and writing output.

    start = time.time()

    # Define input and output file paths
    input_file = 'input_file'
    output_file = 'output_file'

    # Define queue size and file chunk size
    queue_size = 10
    file_data_chunk_size = 10000000

    # Create input and output queues
    input_queue = asyncio.Queue(maxsize=queue_size)
    output_queue = asyncio.Queue(maxsize=queue_size)

    # Prepare the output file by clearing existing data
    prepare_output_file(output_file)

    # Create tasks for reading input, processing, and writing output
    write_task = asyncio.create_task(write_output_file(output_queue, output_file))
    input_task = asyncio.create_task(
        read_input_file(input_file, input_queue, file_data_chunk_size)
    )
    processor_task = asyncio.create_task(processor(input_queue, output_queue))
    # Wait for all tasks to complete
    await asyncio.gather(input_task, processor_task, write_task)

    # Calculate and print the total execution time
    end = time.time() - start
    print("Total execution time:", end, "seconds")


if __name__ == "__main__":
    asyncio.run(process_file())
