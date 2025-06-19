import httpx
import asyncio
import functools
import traceback

def retry_httpx(max_retries=3, initial_delay=1):
    """Decorator to retry HTTP requests on timeout or errors."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retry_delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)  # Call the original function
                except httpx.ReadTimeout:
                    if attempt < max_retries - 1:
                        print(f"Timeout in {func.__name__}. Retrying {attempt + 1}/{max_retries} after {retry_delay}s...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print(f"Final retry failed for {func.__name__}. Skipping request.")
                        return None
                except httpx.HTTPStatusError as e:
                    status_code = e.response.status_code
                    if 500 <= status_code < 600 and attempt < max_retries - 1:
                        print(f"Server error {status_code} in {func.__name__}. Retrying {attempt + 1}/{max_retries} after {retry_delay}s...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        print(f"HTTP error {status_code} in {func.__name__}: {e.response.text}. Skipping request.")
                        return None
                except Exception as e:
                    print(f"Unexpected error in {func.__name__}: {e}\n{traceback.format_exc()}. Skipping request.")
                    return None
        return wrapper
    return decorator
