# crypter

This snippet code provides aproach for encrypting the huge or large files (let say with size around 50-100 GB) .

The basic idea is reading (one single thread) the input file (with mmap) by chunks and appending them to QUEUE_A. 
From the QUEUE_A the chunks are continually read and processed (e.g. encrypted or decrypted), and the processing result is written to the QUEUE_B.
On the QUEUE_B, the listener continually reads the processed chunks and writes them to the output file.


