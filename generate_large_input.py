import os
#generates large file
with open('large_file', 'wb') as fout:
    for a in range(50):
        fout.write(os.urandom((1024 * 1024 * 1024)))
