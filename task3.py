import ray
import time
import platform

# Connect to your running Ray cluster
ray.init()

@ray.remote(num_gpus=1)
def gpu_task(task_id):
    """
    A simulated task that requests a GPU.
    Ray manages the resource, but the code inside must use it.
    """
    print(f"Task {task_id} is starting and has been assigned a GPU.")
    # Simulate GPU work
    time.sleep(3)
    node = platform.node()
    print(f"Task {task_id} on node '{node}' finished its 'GPU' work.")
    return f"Task {task_id} complete"

# Ensure you started Ray with GPU support available
# On a single machine, Ray automatically detects available GPUs.
print("Launching tasks that require GPU resources...")
print("Check the Ray Dashboard to see GPU allocation.")

# Launch several tasks that each request one GPU 
# Ray will queue them if not enough GPUs are free.
gpu_task_refs = [gpu_task.remote(i) for i in range(4)]

# Wait for the results
results = ray.get(gpu_task_refs)
print("\nResults from GPU tasks:")
for r in results:
    print(r)

print("\n GPU task demonstration complete.")
print("Note: Your code inside the function must use libraries like PyTorch or TensorFlow to actually run on the GPU hardware.") [cite: 43]

# Disconnect from Ray
ray.shutdown()