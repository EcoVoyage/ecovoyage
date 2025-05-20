"""Directed Acyclic Graph (DAG) implementation for EcoVoyage workflows."""

import os
import requests
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Callable, Any, Optional, Set
import concurrent.futures


class Task:
    """Represents a task in the workflow DAG."""
    
    def __init__(self, name: str, callable_fn: Callable, **kwargs):
        """Initialize a Task with a name, function, and parameters.
        
        Args:
            name: Unique name for the task
            callable_fn: Function to execute
            **kwargs: Parameters to pass to the callable function
        """
        self.name = name
        self.callable_fn = callable_fn
        self.kwargs = kwargs
        self.dependencies: List[Task] = []
        self.result = None
        self.done = False
        self.error = None
    
    def add_dependency(self, task: 'Task') -> 'Task':
        """Add a dependency to this task.
        
        Args:
            task: Task that must complete before this task executes
            
        Returns:
            self for method chaining
        """
        self.dependencies.append(task)
        return self
        
    def execute(self) -> Any:
        """Execute the task's function with its parameters.
        
        Returns:
            The result of the function call
        """
        if self.done:
            return self.result
            
        try:
            self.result = self.callable_fn(**self.kwargs)
            self.done = True
        except Exception as e:
            self.error = e
            self.done = True
            print(f"❌ Task '{self.name}' failed: {e}")
            raise
            
        return self.result


class DAG:
    """Directed Acyclic Graph for workflow management."""
    
    def __init__(self, name: str):
        """Initialize a DAG.
        
        Args:
            name: Name of the DAG
        """
        self.name = name
        self.tasks: Dict[str, Task] = {}
        
    def add_task(self, task: Task) -> Task:
        """Add a task to the DAG.
        
        Args:
            task: Task to add
            
        Returns:
            The added task
        """
        if task.name in self.tasks:
            raise ValueError(f"Task with name '{task.name}' already exists in DAG")
        self.tasks[task.name] = task
        return task
        
    def get_task(self, name: str) -> Task:
        """Get a task by name.
        
        Args:
            name: Name of the task
            
        Returns:
            The task
        """
        if name not in self.tasks:
            raise ValueError(f"Task '{name}' not found in DAG")
        return self.tasks[name]
        
    def _get_all_dependencies(self, task: Task, visited: Set[str] = None) -> Set[str]:
        """Get all dependencies for a task recursively.
        
        Args:
            task: Task to get dependencies for
            visited: Set of visited task names
            
        Returns:
            Set of all dependency task names
        """
        if visited is None:
            visited = set()
            
        if task.name in visited:
            return visited
            
        visited.add(task.name)
        for dep in task.dependencies:
            self._get_all_dependencies(dep, visited)
            
        return visited
        
    def validate(self) -> bool:
        """Validate the DAG for cycles.
        
        Returns:
            True if DAG is valid
            
        Raises:
            ValueError: If a cycle is detected
        """
        # For each task, check if there's a path from any of its
        # dependencies back to itself
        for task_name, task in self.tasks.items():
            # For each direct dependency
            path = set()
            for dep in task.dependencies:
                # If we can reach the task from its dependency, there's a cycle
                self._check_cycle(dep, task, path)
        return True
        
    def _check_cycle(self, task: Task, target: Task, path: Set[str]) -> None:
        """Check if there's a path from task to target (which would create a cycle).
        
        Args:
            task: The task to check from
            target: The target task that should not be reachable
            path: Set of task names already visited in this path
            
        Raises:
            ValueError: If a cycle is detected
        """
        if task.name in path:
            # We've already checked this task in the current path
            return
        
        if task.name == target.name:
            # We found a path back to the target - cycle detected
            raise ValueError(f"Cycle detected including task '{task.name}'")
        
        # Add the current task to the path
        path.add(task.name)
        
        # Check all dependencies of the current task
        for dep in task.dependencies:
            self._check_cycle(dep, target, path)
        
    def execute(self, max_workers: Optional[int] = None) -> Dict[str, Any]:
        """Execute the DAG.
        
        Args:
            max_workers: Maximum number of concurrent workers
            
        Returns:
            Dictionary of task results
        """
        self.validate()
        
        # Reset all tasks
        for task in self.tasks.values():
            task.done = False
            task.result = None
            task.error = None
            
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Keep track of futures
            futures = {}
            pending_tasks = list(self.tasks.values())
            
            while pending_tasks or futures:
                # Submit ready tasks
                for task in list(pending_tasks):
                    deps_done = all(dep.done for dep in task.dependencies)
                    deps_success = all(dep.error is None for dep in task.dependencies)
                    
                    if deps_done and deps_success:
                        future = executor.submit(task.execute)
                        futures[future] = task
                        pending_tasks.remove(task)
                    elif deps_done and not deps_success:
                        # Mark as done with error if dependencies failed
                        task.done = True
                        task.error = ValueError(f"Dependency failed for task '{task.name}'")
                        pending_tasks.remove(task)
                
                # Process completed futures
                if futures:
                    try:
                        # Use a small timeout to process as tasks complete
                        done, _ = concurrent.futures.wait(
                            futures.keys(), 
                            timeout=0.1,
                            return_when=concurrent.futures.FIRST_COMPLETED
                        )
                        
                        for future in done:
                            task = futures.pop(future)
                            try:
                                results[task.name] = future.result()
                            except Exception as e:
                                print(f"❌ Task '{task.name}' failed: {e}")
                                results[task.name] = None
                    except TimeoutError:
                        # No tasks completed in this iteration, continue
                        pass
        
        return results


# Utility functions for download tasks
def validate_directory(dir_path: str) -> bool:
    """Ensure directory exists, create if not.
    
    Args:
        dir_path: Directory path
        
    Returns:
        True if successful
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        print(f"📁 Created directory: {dir_path}")
    return True

def check_feed_update(url: str, local_path: str) -> bool:
    """Check if remote feed is newer than local copy.
    
    Args:
        url: URL of the feed
        local_path: Local path to save feed
        
    Returns:
        True if update needed
    """
    try:
        # Get remote metadata
        response = requests.head(url, timeout=10)
        response.raise_for_status()
        
        last_modified = response.headers.get('Last-Modified')
        
        if not last_modified:
            print(f"⚠️ No last-modified header for {os.path.basename(local_path)} - will download")
            return True

        remote_time = parsedate_to_datetime(last_modified)
        local_exists = os.path.exists(local_path)

        if not local_exists:
            print(f"➡️ New feed: {os.path.basename(local_path)}")
            return True

        local_time = datetime.fromtimestamp(
            os.path.getmtime(local_path), tz=timezone.utc
        )
        
        if remote_time > local_time:
            print(f"🔄 Update available: {os.path.basename(local_path)} "
                  f"(Remote: {remote_time.date()} vs Local: {local_time.date()})")
            return True
        
        print(f"✓ {os.path.basename(local_path)} is up-to-date")
        return False

    except requests.exceptions.RequestException as e:
        print(f"🚨 Error checking {url}: {e}")
        return False

def download_feed(url: str, local_path: str, needs_update: bool) -> bool:
    """Download a feed if it needs updating.
    
    Args:
        url: URL of the feed
        local_path: Local path to save feed
        needs_update: Whether the feed needs updating
        
    Returns:
        True if download successful or not needed
    """
    if not needs_update:
        return True
        
    try:
        print(f"⏳ Downloading {os.path.basename(local_path)}...")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        # Get server timestamp before writing file
        last_modified = response.headers.get('Last-Modified')
        remote_time = parsedate_to_datetime(last_modified).timestamp() if last_modified else None

        # Stream write with progress
        total_size = int(response.headers.get('Content-Length', 0))
        downloaded = 0
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\r📥 {progress:.1f}% complete", end="")

        # Preserve server timestamp if available
        if remote_time:
            os.utime(local_path, (remote_time, remote_time))

        print(f"\n✅ Saved {os.path.basename(local_path)} ({downloaded//1024} KB)")
        return True

    except Exception as e:
        print(f"\n❌ Failed to download {url}: {e}")
        return False

def create_download_dag(feeds: List[Dict[str, str]], max_workers: int = 3) -> DAG:
    """Create a DAG for downloading feeds.
    
    Args:
        feeds: List of feed configurations (url and local_path)
        max_workers: Maximum number of concurrent downloads
        
    Returns:
        Configured DAG
    """
    dag = DAG("feed_download")
    
    # Create directory validation tasks
    dir_tasks = {}
    
    for i, feed in enumerate(feeds):
        dir_path = os.path.dirname(feed["local_path"])
        dir_task_name = f"validate_dir_{dir_path}"
        
        # Reuse existing directory task if it exists
        if dir_path in dir_tasks:
            dir_task = dir_tasks[dir_path]
        else:
            dir_task = Task(
                name=dir_task_name,
                callable_fn=validate_directory,
                dir_path=dir_path
            )
            dag.add_task(dir_task)
            dir_tasks[dir_path] = dir_task
        
        # Create check task
        check_task = Task(
            name=f"check_{i}",
            callable_fn=check_feed_update,
            url=feed["url"],
            local_path=feed["local_path"]
        )
        check_task.add_dependency(dir_task)
        dag.add_task(check_task)
        
        # Create download task
        download_task = Task(
            name=f"download_{i}",
            callable_fn=download_feed,
            url=feed["url"],
            local_path=feed["local_path"],
            needs_update=None  # Will be set from check_task result
        )
        download_task.add_dependency(check_task)
        dag.add_task(download_task)
    
    return dag

def run_download_dag(feeds: List[Dict[str, str]], max_workers: int = 3) -> Dict[str, Any]:
    """Run the download DAG.
    
    Args:
        feeds: List of feed configurations
        max_workers: Maximum number of concurrent downloads
        
    Returns:
        Dictionary of task results
    """
    dag = create_download_dag(feeds, max_workers)
    
    print(f"🔄 Running download DAG with {len(feeds)} feeds and {max_workers} workers")
    
    results = dag.execute(max_workers=max_workers)
    
    # Update download tasks with check results
    for i in range(len(feeds)):
        check_result = dag.get_task(f"check_{i}").result
        dag.get_task(f"download_{i}").kwargs["needs_update"] = check_result
    
    # Execute again with updated parameters
    results = dag.execute(max_workers=max_workers)
    
    # Count results
    updated = sum(1 for i in range(len(feeds)) 
                 if results.get(f"check_{i}") and results.get(f"download_{i}"))
    skipped = sum(1 for i in range(len(feeds)) 
                 if not results.get(f"check_{i}") and results.get(f"download_{i}"))
    errors = sum(1 for i in range(len(feeds))
                if dag.get_task(f"download_{i}").error is not None)
    
    print(f"\n📋 Results: {updated} updated, {skipped} current, {errors} errors")
    
    return results 