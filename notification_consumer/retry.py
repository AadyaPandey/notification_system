import time


def retry(operation, retries=3, delay=2):
    """
    Retries the given operation if it raises an exception.

    Args:
        operation: Function to execute.
        retries: Number of attempts.
        delay: Delay (seconds) between retries.
    """

    for attempt in range(1, retries + 1):
        try:
            return operation()

        except Exception as e:
            print(f"\nAttempt {attempt} failed: {e}")

            if attempt == retries:
                print("\nMaximum retries reached.")
                raise

            print(f"Retrying in {delay} seconds...\n")
            time.sleep(delay)