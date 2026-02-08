"""
Thread-safe rate limiter for parallel API calls.

This module provides a token bucket rate limiter that can be shared across
parallel workers to ensure API rate limits are respected.
"""
import threading
import time
from typing import Dict, Any


class TokenBucketRateLimiter:
    """
    Thread-safe token bucket rate limiter.
    
    This rate limiter allows bursts of API calls up to the rate limit,
    then throttles requests to maintain the average rate over time.
    
    Parameters
    ----------
    rate_limit_per_min : int
        Maximum number of API calls allowed per minute
    """
    
    def __init__(self, rate_limit_per_min: int):
        self.rate_limit_per_min = rate_limit_per_min
        self.tokens = rate_limit_per_min
        self.max_tokens = rate_limit_per_min
        self.last_update = time.time()
        self.lock = threading.Lock()
        
        # Calculate token refill rate (tokens per second)
        self.refill_rate = rate_limit_per_min / 60.0
    
    def _refill_tokens(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        
        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.refill_rate
        self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
        self.last_update = now
    
    def acquire(self, num_tokens: int = 1) -> None:
        """
        Acquire tokens for API calls. Blocks if insufficient tokens available.
        
        Parameters
        ----------
        num_tokens : int, default=1
            Number of tokens (API calls) to acquire
        """
        with self.lock:
            while True:
                self._refill_tokens()
                
                if self.tokens >= num_tokens:
                    # We have enough tokens, consume them and return
                    self.tokens -= num_tokens
                    return
                else:
                    # Not enough tokens, calculate wait time
                    tokens_needed = num_tokens - self.tokens
                    wait_time = tokens_needed / self.refill_rate
                    
                    # Release lock while sleeping to allow other threads to check
                    self.lock.release()
                    
                    # Sleep for a portion of the wait time, then retry
                    # (using smaller sleep intervals for better responsiveness)
                    sleep_time = min(wait_time, 1.0)
                    print(f"Rate limit: waiting {sleep_time:.2f}s for {num_tokens} tokens...")
                    time.sleep(sleep_time)
                    
                    # Re-acquire lock for next iteration
                    self.lock.acquire()
    
    def get_available_tokens(self) -> float:
        """
        Get the current number of available tokens.
        
        Returns
        -------
        float
            Number of available tokens
        """
        with self.lock:
            self._refill_tokens()
            return self.tokens


# Global rate limiter instance (shared across all workers)
_global_rate_limiter = None
_rate_limiter_lock = threading.Lock()


def get_rate_limiter(config: Dict[str, Any] = None) -> TokenBucketRateLimiter:
    """
    Get or create the global rate limiter instance.
    
    This ensures all parallel workers share the same rate limiter.
    
    Parameters
    ----------
    config : Dict[str, Any], optional
        Configuration dictionary containing rate_limit_per_min.
        Only used when creating a new rate limiter.
    
    Returns
    -------
    TokenBucketRateLimiter
        The global rate limiter instance
    """
    global _global_rate_limiter
    
    with _rate_limiter_lock:
        if _global_rate_limiter is None:
            if config is None:
                from project_eden.db.data_ingestor import load_config
                config = load_config()
            
            rate_limit = config["api"]["rate_limit_per_min"]
            _global_rate_limiter = TokenBucketRateLimiter(rate_limit)
            print(f"Initialized rate limiter: {rate_limit} calls/min")
        
        return _global_rate_limiter


def reset_rate_limiter():
    """Reset the global rate limiter (useful for testing)."""
    global _global_rate_limiter
    with _rate_limiter_lock:
        _global_rate_limiter = None

