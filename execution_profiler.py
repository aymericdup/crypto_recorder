import time
from functools import wraps
from typing import Optional, Callable, List
import statistics

class ExecutionTimeProfiler:
    """Timing decorator as a class"""

    def __init__(self, func: Optional[Callable] = None, verbose : bool = True):
        
        if func is not None:
            # Used as @Timer without parentheses
            self.func = func
            wraps(func)(self)
        else:
            # Used as @Timer() with parentheses
            self.func = None

        self.total_time = 0
        self.times = []
        self.call_count = 0
        self.verbose = verbose

    @property
    def avg_time(self) -> float:
        return self.total_time / self.call_count if self.call_count > 0 else 0
    
    @property
    def min_time(self) -> float:
        return min(self.times) if self.times else 0
    
    @property
    def max_time(self) -> float:
        return max(self.times) if self.times else 0
    
    @property
    def median_time(self) -> float:
        return statistics.median(self.times) if self.times else 0

    def __call__(self, *args, **kwargs):
        if self.func is None:
            # Called with arguments: @Timer(verbose=True)
            func = args[0]
            self.func = func
            wraps(func)(self)
            return self
        
        start = time.perf_counter()
        result = self.func(*args, **kwargs)
        end = time.perf_counter()
        elapsed = end - start

        # track some statistics
        self.times.append(elapsed)
        self.total_time += elapsed
        self.call_count += 1

        if self.verbose: print(f"{self.func.__name__} took {elapsed:.4f}s")
        return result
    
    def stats(self):

        n = len(self.times)
        if n < 1 : return {}

        return {
            'nb_calls': self.nb_call,
            'total_time' : self.total_time,
            'avg': self.avg_time,
            'min': self.min_time,
            'max': self.max_time,
            'median': self.median_time
        }
    
    def __str__(self):
        return (
            f"{self.function_name} Statistics:\n"
            f"  Calls: {self.call_count}\n"
            f"  Total: {self.total_time:.4f}s\n"
            f"  Avg:   {self.avg_time:.4f}s\n"
            f"  Min:   {self.min_time:.4f}s\n"
            f"  Max:   {self.max_time:.4f}s\n"
            f"  Median: {self.median_time:.4f}s"
        )
    
    def reset(self):
        """Reset all features"""
        self.total_time = 0
        self.times.clear()
        self.call_count = 0
        