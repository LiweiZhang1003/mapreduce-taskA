"""
Yield fixed-size batches of lines to avoid loading the whole file.
"""
def line_batches(handle, batch_size=50_000):
    batch = []
    for line in handle:
        batch.append(line)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
