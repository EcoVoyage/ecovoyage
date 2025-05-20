"""Tests for the DAG implementation."""

import unittest
from unittest.mock import patch, MagicMock
import os
import tempfile

from ecovoyage.dag import Task, DAG, validate_directory


class TestDAG(unittest.TestCase):
    """Test cases for DAG implementation."""
    
    def test_task_creation(self):
        """Test that tasks can be created correctly."""
        mock_fn = MagicMock(return_value="test_result")
        task = Task("test_task", mock_fn, arg1="test", arg2=123)
        
        self.assertEqual(task.name, "test_task")
        self.assertEqual(task.kwargs, {"arg1": "test", "arg2": 123})
        self.assertFalse(task.done)
        self.assertIsNone(task.result)
        self.assertIsNone(task.error)
        
    def test_task_execution(self):
        """Test that tasks execute correctly."""
        mock_fn = MagicMock(return_value="test_result")
        task = Task("test_task", mock_fn, arg1="test", arg2=123)
        
        result = task.execute()
        
        mock_fn.assert_called_once_with(arg1="test", arg2=123)
        self.assertEqual(result, "test_result")
        self.assertTrue(task.done)
        self.assertEqual(task.result, "test_result")
        self.assertIsNone(task.error)
        
    def test_task_error_handling(self):
        """Test that task errors are handled correctly."""
        mock_fn = MagicMock(side_effect=ValueError("test error"))
        task = Task("test_task", mock_fn)
        
        with self.assertRaises(ValueError):
            task.execute()
        
        self.assertTrue(task.done)
        self.assertIsNone(task.result)
        self.assertIsInstance(task.error, ValueError)
        self.assertEqual(str(task.error), "test error")
        
    def test_dag_creation(self):
        """Test DAG creation."""
        dag = DAG("test_dag")
        
        self.assertEqual(dag.name, "test_dag")
        self.assertEqual(dag.tasks, {})
        
    def test_dag_add_task(self):
        """Test adding tasks to a DAG."""
        dag = DAG("test_dag")
        task1 = Task("task1", lambda: "result1")
        task2 = Task("task2", lambda: "result2")
        
        dag.add_task(task1)
        dag.add_task(task2)
        
        self.assertEqual(len(dag.tasks), 2)
        self.assertEqual(dag.tasks["task1"], task1)
        self.assertEqual(dag.tasks["task2"], task2)
        
    def test_dag_get_task(self):
        """Test getting a task from a DAG."""
        dag = DAG("test_dag")
        task = Task("task1", lambda: "result")
        
        dag.add_task(task)
        
        self.assertEqual(dag.get_task("task1"), task)
        
    def test_dag_validate(self):
        """Test DAG validation."""
        dag = DAG("test_dag")
        task1 = Task("task1", lambda: "result1")
        task2 = Task("task2", lambda: "result2")
        
        dag.add_task(task1)
        dag.add_task(task2)
        
        task2.add_dependency(task1)
        
        self.assertTrue(dag.validate())
        
    def test_dag_validate_cycle(self):
        """Test DAG validation with a cycle."""
        dag = DAG("test_dag")
        task1 = Task("task1", lambda: "result1")
        task2 = Task("task2", lambda: "result2")
        
        dag.add_task(task1)
        dag.add_task(task2)
        
        task2.add_dependency(task1)
        task1.add_dependency(task2)
        
        with self.assertRaises(ValueError):
            dag.validate()
            
    def test_dag_execute(self):
        """Test DAG execution."""
        dag = DAG("test_dag")
        
        # Create mock functions
        mock1 = MagicMock(return_value="result1")
        mock2 = MagicMock(return_value="result2")
        mock3 = MagicMock(return_value="result3")
        
        # Create tasks
        task1 = Task("task1", mock1)
        task2 = Task("task2", mock2)
        task3 = Task("task3", mock3)
        
        # Add dependencies
        task2.add_dependency(task1)
        task3.add_dependency(task2)
        
        # Add tasks to DAG
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        # Execute DAG
        results = dag.execute()
        
        # Check results
        self.assertEqual(results["task1"], "result1")
        self.assertEqual(results["task2"], "result2")
        self.assertEqual(results["task3"], "result3")
        
        # Check that functions were called in the right order
        mock1.assert_called_once()
        mock2.assert_called_once()
        mock3.assert_called_once()
        
    def test_validate_directory(self):
        """Test directory validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "test_dir")
            
            # Check that directory doesn't exist yet
            self.assertFalse(os.path.exists(test_dir))
            
            # Validate directory
            result = validate_directory(test_dir)
            
            # Check that directory now exists
            self.assertTrue(os.path.exists(test_dir))
            self.assertTrue(result)
            
    def test_dependency_chain(self):
        """Test a chain of dependencies in a DAG."""
        dag = DAG("test_dag")
        
        # Create tasks
        task1 = Task("task1", lambda: 1)
        task2 = Task("task2", lambda x: x * 2, x=None)  # Will be set from task1
        task3 = Task("task3", lambda x: x * 3, x=None)  # Will be set from task2
        
        # Add tasks to DAG
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        # Add dependencies
        task2.add_dependency(task1)
        task3.add_dependency(task2)
        
        # Execute once to get task1 result
        dag.execute()
        
        # Update task2 with task1 result
        task2.kwargs["x"] = task1.result
        
        # Execute again to get task2 result
        dag.execute()
        
        # Update task3 with task2 result
        task3.kwargs["x"] = task2.result
        
        # Execute again to get task3 result
        results = dag.execute()
        
        # Check final results
        self.assertEqual(results["task1"], 1)
        self.assertEqual(results["task2"], 2)
        self.assertEqual(results["task3"], 6)


if __name__ == "__main__":
    unittest.main() 