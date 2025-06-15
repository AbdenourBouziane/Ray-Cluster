import ray
import time
import numpy as np
import math

# Connect to your running Ray cluster
ray.init()

# Define a function to perform on each data point 
def process_data(x):
    # A non-trivial calculation
    return math.log(math.sqrt(abs(x)))

# Decorate the function to make it a Ray remote task 
@ray.remote
def process_data_remote(x):
    return math.log(math.sqrt(abs(x)))

# 1. Create a large dataset 
dataset = np.random.rand(1_000_000) * 100

# --- Sequential Processing ---
print("Starting sequential data processing...")
start_time_seq = time.time()
results_seq = [process_data(x) for x in dataset]
end_time_seq = time.time()
print(f"Sequential processing time: {end_time_seq - start_time_seq:.2f} seconds")
print("-" * 20)

# --- Parallel Processing with Ray ---
print("Starting Ray parallel data processing...")
start_time_ray = time.time()
result_refs = [process_data_remote.remote(x) for x in dataset]
results_ray = ray.get(result_refs)
end_time_ray = time.time()
print(f"Ray parallel processing time: {end_time_ray - start_time_ray:.2f} seconds")
print("-" * 20)

print(f"Processed {len(dataset)} items.")
print("Processing complete.")

# Disconnect from Ray
ray.shutdown()