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

    The bucket starts empty so that no burst of calls can happen at startup.
    Tokens accumulate at ``rate_limit_per_min / 60`` tokens per second up to
    a maximum of ``rate_limit_per_min``, enforcing a true per-minute ceiling.

    A ``threading.Condition`` is used instead of a manual lock-release/sleep/
    re-acquire pattern so that waiting threads are woken precisely when new
    tokens arrive, eliminating the race window that allowed multiple threads to
    simultaneously consume the same tokens.

    Parameters
    ----------
    rate_limit_per_min : int
        Maximum number of API calls allowed per minute
    """

    def __init__(self, rate_limit_per_min: int):
        self.rate_limit_per_min = rate_limit_per_min
        # Start with an empty bucket so no initial burst is possible.
        self.tokens = 0.0
        self.max_tokens = float(rate_limit_per_min)
        self.last_update = time.time()
        # Condition wraps the underlying lock; use self._cond.acquire/release
        # (or "with self._cond") everywhere instead of a bare Lock.
        self._cond = threading.Condition(threading.Lock())

        # Token refill rate (tokens per second)
        self.refill_rate = rate_limit_per_min / 60.0

    # ------------------------------------------------------------------
    # Internal helpers – must be called with self._cond held
    # ------------------------------------------------------------------

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time (lock must be held)."""
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_update = now

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, num_tokens: int = 1) -> None:
        """
        Acquire tokens for API calls. Blocks until sufficient tokens are
        available.

        Parameters
        ----------
        num_tokens : int, default=1
            Number of tokens (API calls) to acquire
        """
        if num_tokens > self.max_tokens:
            raise ValueError(
                f"Requested {num_tokens} tokens but bucket maximum is "
                f"{int(self.max_tokens)}.  Reduce the number of simultaneous "
                "API calls or increase rate_limit_per_min."
            )

        with self._cond:
            while True:
                self._refill_tokens()

                if self.tokens >= num_tokens:
                    self.tokens -= num_tokens
                    return

                # Calculate how long until enough tokens are available and
                # wait with a timeout so we re-check after that interval.
                tokens_needed = num_tokens - self.tokens
                wait_time = tokens_needed / self.refill_rate
                print(
                    f"Rate limit: waiting {wait_time:.2f}s for "
                    f"{num_tokens} token(s)..."
                )
                # Condition.wait() atomically releases the lock, sleeps for
                # at most `wait_time` seconds, then re-acquires it before
                # returning – no race window.
                self._cond.wait(timeout=wait_time)

    def get_available_tokens(self) -> float:
        """
        Return the current number of available tokens.

        Returns
        -------
        float
            Number of available tokens
        """
        with self._cond:
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

