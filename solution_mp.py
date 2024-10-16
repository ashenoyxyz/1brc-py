import os
import multiprocessing as mp

FILE_PATH = "/opt/datasets/1brc/measurements.txt"

def process_chunk(file_path, start, end):
    # print(f'[Start] Processing chunk: {start},{end}')
    stats = {}
    offset = start
    with open(file_path, "r") as f:
        f.seek(start)
        for row in f:
            if offset >= end:
                break

            offset += len(row)
            parts = row.split(";")
            city, temp = parts[0], float(parts[1])
            if city not in stats:
                # max, min, sum, count
                stats[city] = [temp, temp, temp, 1]
            else:
                city_stats = stats[city]
                city_stats[0] = max(city_stats[0], temp)
                city_stats[1] = min(city_stats[1], temp)
                city_stats[2] += temp
                city_stats[3] += 1

    # print(f'[End] Processing chunk: {start},{end}')
    return stats

def start():
    file_size = os.path.getsize(FILE_PATH)
    cpu_count = mp.cpu_count()
    chunk_size = file_size // cpu_count
    chunks = []
    with open(FILE_PATH, "rb") as f:
        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(chunk_start + chunk_size, file_size-1)
            f.seek(chunk_end)
            while f.read(1) != b"\n":
                chunk_end -= 1
                f.seek(chunk_end)
            chunks.append((FILE_PATH, chunk_start, chunk_end))
            chunk_start = chunk_end + 1

    # for chunk in chunks:
    #     print(chunk)

    # Map
    results = []
    with mp.Pool(cpu_count) as p:
        results = p.starmap(process_chunk, chunks)

    # Reduce
    aggr_results = {}
    for result in results:
        for city, stats in result.items():
            if city not in aggr_results:
                aggr_results[city] = stats
            else:
                city_stats = aggr_results[city]
                city_stats[0] = max(city_stats[0], stats[0])
                city_stats[1] = min(city_stats[1], stats[1])
                city_stats[2] += stats[2]
                city_stats[3] += stats[3]

    output = []
    for key, value in sorted(aggr_results.items(), key=lambda x:x[0]):
        output.append(f'{key}={value[1]}/{value[2]/value[3]:0.1f}/{value[0]}')
    print("{" + ", ".join(output) + "}")

if __name__ == '__main__':
    start()
