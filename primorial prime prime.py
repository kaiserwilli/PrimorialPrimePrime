import gmpy2
from multiprocessing import Pool, cpu_count, Manager
import time
from datetime import datetime, timedelta

# Function to calculate the primorial of a given prime (e.g., 12539#)
def calculate_primorial(prime):
    primorial = gmpy2.mpz(1)
    current_prime = gmpy2.mpz(2)  # Start with the first prime
    
    # Multiply primes until we reach the given prime
    print(f"Calculating the primorial for {prime}#...")
    while current_prime <= prime:
        primorial *= current_prime
        current_prime = gmpy2.next_prime(current_prime)
    print(f"Primorial calculation for {prime}# completed.")
    return primorial

# Function to check if p_n# + p_{n+1} is prime
def check_prime_task(args):
    prime, next_prime, cumulative_product = args
    result = cumulative_product + next_prime
    is_prime = gmpy2.is_prime(result)
    return (prime, next_prime, result, is_prime)

# Function to process results and write them to file
def write_results(results_array, file, time_taken, avg_time_per_task, batch_end_time):
    print("Writing results to file...")
    last_prime_checked, _, last_result, _ = results_array[-1]  # Get the last prime from the batch
    last_digits = len(str(last_result))  # Calculate the number of digits of the last result
    batch_end_str = batch_end_time.strftime('%Y-%m-%d %H:%M:%S')  # Convert end time to string

    # Write batch summary to the file
    file.write(f"Batch completed. Last checked: {last_prime_checked}# ({last_digits} digits) at {batch_end_str} "
               f"Time elapsed: {time_taken:.2f}s, Avg time per check: {avg_time_per_task:.4f}s\n")
    file.flush()  # Ensure the write is immediately committed

    # Write individual results
    for res in results_array:
        prime, next_prime, result, is_prime = res
        if is_prime:
            # If prime, write the full primorial result
            status = 'Prime'
            file.write(f"{prime}# + {next_prime} = {result} ({status})\n")
        else:
            # If not prime, write a simplified message
            status = 'Not Prime'
            # file.write(f"{prime}# + {next_prime} ({status})\n")
        file.flush()

    print("Results written successfully.")

if __name__ == "__main__":
    # Set up multiprocessing
    num_workers = cpu_count()
    task_limit = num_workers * 5  # Number of tasks to track at a time
    print(f"Detected {num_workers} CPU cores for parallel processing.")
    manager = Manager()
    results_array = manager.list()  # Shared array to store results

    # Get user input for the starting prime number
    start_prime = int(input("Enter the prime number to start from (e.g., 2 for the first prime): "))

    # Calculate the cumulative primorial for the input prime number
    print(f"Calculating the primorial for {start_prime}#...")
    cumulative_product = calculate_primorial(start_prime)
    print(f"Primorial for {start_prime}# calculated. Starting from prime {start_prime}.")

    # Open file for writing results
    with open("primorial_prime_prime_results.txt", "a") as file:
        print("Opening file for writing results.")
        # Create a pool of workers
        pool = Pool(num_workers)
        print(f"Pool of {num_workers} workers created.")

        # Start checking primes dynamically
        task_counter = 0
        tasks_in_progress = []
        completed_tasks = []
        batch_start_time = time.time()  # Time at the start of the batch

        print("Starting dynamic prime checking...")

        # Start initial batch of tasks
        prime = gmpy2.mpz(start_prime)  # Start from the prime given by the user
        next_prime = gmpy2.next_prime(prime)  # Calculate the first next_prime

        for _ in range(task_limit):
            task_args = (prime, next_prime, cumulative_product)
            tasks_in_progress.append(pool.apply_async(check_prime_task, args=(task_args,)))
            prime = next_prime  # Move to next prime
            cumulative_product *= prime  # Update cumulative product after task creation
            next_prime = gmpy2.next_prime(prime)  # Prepare the next prime
            task_counter += 1

        while task_counter < 100000:  # Adjust the range as needed
            # Check for completed tasks
            for task in tasks_in_progress[:]:
                if task.ready():
                    result = task.get()  # Get result from the task
                    completed_tasks.append(result)
                    tasks_in_progress.remove(task)  # Remove from in-progress tasks

                    # Report the number of completed tasks waiting to be written out
                    num_completed_tasks = len(completed_tasks)
                    print(f"{num_completed_tasks} tasks completed and waiting to be written out.")

                    # Start a new task
                    task_args = (prime, next_prime, cumulative_product)
                    tasks_in_progress.append(pool.apply_async(check_prime_task, args=(task_args,)))
                    prime = next_prime  # Move to next prime
                    cumulative_product *= prime  # Update cumulative product
                    next_prime = gmpy2.next_prime(prime)  # Prepare next prime
                    task_counter += 1
                    print(f"A task completed and a new one started at: {datetime.now()}")

            # Check if the first 32 tasks in the list are completed before writing
            if len(completed_tasks) >= task_limit:
                # Check if the first `task_limit` tasks in completed_tasks are done
                if len(completed_tasks) >= task_limit:
                    batch_end_time = datetime.now()  # Time when batch completes
                    time_taken = time.time() - batch_start_time
                    avg_time_per_task = time_taken / task_limit

                    print(f"Batch completed in {time_taken:.2f} seconds. Avg time per task: {avg_time_per_task:.4f}s")
                    estimated_next_batch_time = avg_time_per_task * task_limit
                    next_batch_end_time = datetime.now() + timedelta(seconds=estimated_next_batch_time)
                    print(f"Estimated time for next batch: {estimated_next_batch_time:.2f} seconds")
                    print(f"Estimated end time for next batch: {next_batch_end_time.strftime('%Y-%m-%d %H:%M:%S')}")

                    # Write results and log timing information
                    write_results(completed_tasks[:task_limit], file, time_taken, avg_time_per_task, batch_end_time)

                    # Clear the first task_limit completed tasks
                    del completed_tasks[:task_limit]
                    batch_start_time = time.time()  # Reset batch start time for the next batch

            time.sleep(1)  # Adjusted to 1-second sleep to avoid too frequent checking

        # Write any remaining results before closing
        if completed_tasks:
            print(f"Writing remaining {len(completed_tasks)} completed tasks to file...")
            batch_end_time = datetime.now()
            time_taken = time.time() - batch_start_time
            avg_time_per_task = time_taken / len(completed_tasks)
            write_results(completed_tasks, file, time_taken, avg_time_per_task, batch_end_time)

        print("All tasks completed, closing pool.")
        pool.close()
        pool.join()
