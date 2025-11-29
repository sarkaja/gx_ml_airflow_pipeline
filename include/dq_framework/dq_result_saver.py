# dq_result_saver.py 
from __future__ import annotations

import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging 

logger = logging.getLogger("airflow.task")

class DQResultSaver:
    """
    Saves Data Quality validation results to PostgreSQL database.
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize the DQ result saver.
        
        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
    
    def _get_connection(self):
        """Create database connection."""
        return psycopg2.connect(self.connection_string)
    
    def _calculate_statistics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate statistics from results in case of missing.
        
        Args:
            results: Validation results dictionary
            
        Returns:
            Dictionary with calculated statistics
        """
        results_list = results.get('results', [])
        
        total_expectations = len(results_list)
        successful_expectations = sum(1 for r in results_list if r.get('success', False))
        failed_expectations = total_expectations - successful_expectations
        success_rate = (successful_expectations / total_expectations * 100) if total_expectations > 0 else 0
        
        return {
            'evaluated_expectations': total_expectations,
            'successful_expectations': successful_expectations,
            'unsuccessful_expectations': failed_expectations,
            'success_percent': success_rate
        }
    
    def analyze_validation_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze validation results and extract key metrics.
        
        Args:
            results: Validation results dictionary
            
        Returns:
            Dictionary with analysis metrics
        """
        logger.info(f"Full results: {results}")
        # Calculate statistics - either from results or manually
        if 'statistics' in results and results['statistics']:
            stats = results['statistics']
        else:
            stats = self._calculate_statistics(results)
            
        total_expectations = stats.get('evaluated_expectations', 0)
        successful_expectations = stats.get('successful_expectations', 0)
        failed_expectations = stats.get('unsuccessful_expectations', 0)
        success_rate = stats.get('success_percent', 0.0)
        
        failed_columns = set()
        critical_failures = 0
        warning_failures = 0
        key_columns_results = {}
        unexpected_count_total = 0

        # Important columns for monitoring
        key_columns = {
            'checking_account_status', 'credit_history', 'purpose', 
            'credit_amount', 'duration_in_month', 'age_in_years',
            'savings_account_bonds', 'foreign_worker', 'category'
        }
        
        for result in results.get('results', []):
            expectation_config = result.get('expectation_config', {})
            kwargs = expectation_config.get('kwargs', {})
            column = kwargs.get('column')
            result_inner = result.get('result', {})
            
            unexpected_count_total += result_inner.get('unexpected_count',0)
            if not result.get('success', False):
                severity = result.get('expectation_config', {}).get('severity', 'critical')
                
                if severity == 'critical':
                    critical_failures += 1
                else:
                    warning_failures += 1
                
                if column:
                    failed_columns.add(column)
                    
                    if column in key_columns:
                        expectation_type = expectation_config.get('type', '')
                        key_columns_results[column] = {
                            'expectation_type': expectation_type,
                            'success': False,
                            'severity': severity
                        }
        
        # Check completeness (all expect_column_values_to_not_be_null)
        completeness_ok = all(
            result.get('success', False) 
            for result in results.get('results', [])
            if 'expect_column_values_to_not_be_null' in result.get('expectation_config', {}).get('type', '')
        )
        
        # Check consistency (all expect_column_values_to_be_in_set)
        consistency_ok = all(
            result.get('success', False)
            for result in results.get('results', [])
            if 'expect_column_values_to_be_in_set' in result.get('expectation_config', {}).get('type', '')
        )
        
        total_examined_values = total_expectations*self._get_record_count(results)
        real_success_rate = ((total_examined_values - unexpected_count_total) / total_examined_values)*100
        logger.info(f"Success rate with proportion: {real_success_rate}")

        return {
            'total_expectations': total_expectations,
            'successful_expectations': successful_expectations,
            'failed_expectations': failed_expectations,
            'success_rate': success_rate,
            'overall_success': results.get('success', False),
            'completeness_ok': completeness_ok,
            'consistency_ok': consistency_ok,
            'failed_columns': list(failed_columns),
            'critical_failures': critical_failures,
            'warning_failures': warning_failures,
            'key_columns_validation': key_columns_results,
            'records_processed': self._get_record_count(results)
        }
    
    def _get_record_count(self, results: Dict[str, Any]) -> Optional[int]:
        """
        Get processed record count from results.
        
        Args:
            results: Validation results dictionary
            
        Returns:
            Number of processed records or None
        """
        for result in results.get('results', []):
            result_data = result.get('result', {})
            if 'element_count' in result_data:
                return result_data['element_count']
        return None
    
    def save_results(self, 
                    dataset_id: str,
                    run_id: str, 
                    expectation_suite_name: str,
                    validation_results: Dict[str, Any],
                    validation_duration: Optional[float] = None) -> bool:
        """
        Save validation results to database.
        
        Args:
            dataset_id: Dataset identifier
            run_id: Validation run identifier
            expectation_suite_name: Name of expectation suite
            validation_results: Validation results dictionary
            validation_duration: Validation duration in seconds
            
        Returns:
            True if save successful, False otherwise
        """
        try:
            logger.info(f"Saving DQ results to database: {dataset_id}, {run_id}")
            
            analysis = self.analyze_validation_results(validation_results)
            
            logger.info(f"Analysis: {analysis['successful_expectations']}/{analysis['total_expectations']} expectations passed ({analysis['success_rate']:.1f}%)")
            
            # Database connection
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    insert_query = """
                    INSERT INTO public.dq_results (
                        dataset_id, dq_run_id, expectation_suite_name,
                        total_expectations, successful_expectations, failed_expectations,
                        success_rate, overall_success, completeness_ok, consistency_ok,
                        failed_columns, critical_failures, warning_failures,
                        key_columns_validation, validation_duration_seconds, records_processed
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_query, (
                        dataset_id,
                        run_id,
                        expectation_suite_name,
                        analysis['total_expectations'],
                        analysis['successful_expectations'],
                        analysis['failed_expectations'],
                        analysis['success_rate'],
                        analysis['overall_success'],
                        analysis['completeness_ok'],
                        analysis['consistency_ok'],
                        analysis['failed_columns'],
                        analysis['critical_failures'],
                        analysis['warning_failures'],
                        json.dumps(analysis['key_columns_validation']),
                        validation_duration,
                        analysis['records_processed']
                    ))
                    
                    conn.commit()
                    logger.info(f"DQ results successfully saved to database")
                    return True
                    
        except Exception as e:
            logger.error(f"Error saving DQ results: {e}")
            return False

    def get_recent_failures(self, dataset_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent failures for a dataset.
        
        Args:
            dataset_id: Dataset identifier
            limit: Maximum number of results to return
            
        Returns:
            List of failure records
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT dq_run_id, expectation_suite_name, failed_columns, 
                               critical_failures, success_rate, created_at
                        FROM public.dq_results 
                        WHERE dataset_id = %s AND overall_success = false
                        ORDER BY created_at DESC 
                        LIMIT %s
                    """, (dataset_id, limit))
                    
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                    
        except Exception as e:
            logger.error(f"Error reading failures: {e}")
            return []