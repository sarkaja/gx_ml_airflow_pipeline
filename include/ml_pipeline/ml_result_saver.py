# include/ml_pipeline/ml_result_saver.py
from __future__ import annotations

import json
from typing import Dict, Any, List
import logging
import os
from scripts.get_environment_config import ProjectConfig
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger("airflow.task")


class MLResultSaver:
    """
    Save machine learning results to PostgreSQL database.
    """

    def __init__(self):
        """
        Initialize ML result saver.

        Args:
            connection_string: PostgreSQL connection string / SQLAlchemy URL
        """
        cfg = ProjectConfig()
        self.connection_string = os.getenv('AIRFLOW_CONN_DQ_POSTGRES')
        self.run_id = cfg.get_run_id()

        # SQLAlchemy engine + Session factory
        self.engine = create_engine(self.connection_string)
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

    def _get_connection(self):
        """
        Create database session.

        Returns:
            SQLAlchemy Session object
        """
        return self.SessionLocal()

    def save_ml_results(
        self,
        dataset_id: str,
        ml_results: Dict[str, Any],
        test_size: float = 0.2,
        cv_folds: int = 5,
        random_state: int = 42
    ) -> bool:
        """
        Save ML evaluation results to database.

        Args:
            dataset_id: Dataset identifier
            ml_results: Complete ML evaluation results
            test_size: Test set proportion
            cv_folds: Number of cross-validation folds
            random_state: Random seed used

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Saving ML results to database: {dataset_id}")

            with self._get_connection() as session:
                # Save each model's results
                for test_result in ml_results.get('test_results', []):
                    model_name = test_result['model']

                    # Get cross-validation results
                    cv_results = ml_results['cross_validation_results'].get(model_name, {})
                    is_best_model = (ml_results.get('best_model') == model_name)

                    # Prepare data for insertion
                    insert_data = self._prepare_insert_data(
                        dataset_id, model_name, test_result,
                        cv_results, is_best_model, test_size, cv_folds, random_state
                    )

                    self._execute_insert(session, insert_data)
                    logger.info(f"Saved results for {model_name}")

                session.commit()
                logger.info(f"All ML results saved successfully for run: {dataset_id}")
                return True

        except Exception as e:
            logger.error(f"Error saving ML results: {e}")
            return False

    def _prepare_insert_data(
        self,
        dataset_id: str,
        model_name: str,
        test_result: Dict[str, Any],
        cv_results: Dict[str, Any],
        is_best_model: bool,
        test_size: float,
        cv_folds: int,
        random_state: int
    ) -> Dict[str, Any]:
        """
        Prepare data for database insertion.

        Args:
            dataset_id: Dataset identifier
            model_name: Name of the model
            test_result: Test set evaluation results
            cv_results: Cross-validation results
            is_best_model: Whether this is the best model
            test_size: Test set proportion
            cv_folds: Number of CV folds
            random_state: Random seed

        Returns:
            Dictionary with prepared data
        """
        return {
            'dataset_id': dataset_id,
            'run_id': self.run_id,
            'model_name': model_name,
            'accuracy': float(test_result['accuracy']),
            'precision': float(test_result['precision']),
            'recall': float(test_result['recall']),
            'f1_score': float(test_result['f1_score']),
            'roc_auc': float(test_result['roc_auc']),
            'cv_accuracy_mean': float(cv_results.get('accuracy', {}).get('mean', 0)),
            'cv_accuracy_std': float(cv_results.get('accuracy', {}).get('std', 0)),
            'cv_precision_mean': float(cv_results.get('precision', {}).get('mean', 0)),
            'cv_precision_std': float(cv_results.get('precision', {}).get('std', 0)),
            'cv_recall_mean': float(cv_results.get('recall', {}).get('mean', 0)),
            'cv_recall_std': float(cv_results.get('recall', {}).get('std', 0)),
            'cv_f1_mean': float(cv_results.get('f1', {}).get('mean', 0)),
            'cv_f1_std': float(cv_results.get('f1', {}).get('std', 0)),
            'cv_roc_auc_mean': float(cv_results.get('roc_auc', {}).get('mean', 0)),
            'cv_roc_auc_std': float(cv_results.get('roc_auc', {}).get('std', 0)),
            'confusion_matrix': json.dumps(test_result.get('confusion_matrix', [])),
            'classification_report': test_result.get('classification_report', ''),
            'is_best_model': is_best_model,
            'test_size': float(test_size),
            'cv_folds': cv_folds,
            'random_state': random_state
        }

    def _execute_insert(self, session, data: Dict[str, Any]):
        """
        Execute INSERT query with provided data.

        Args:
            session: SQLAlchemy Session
            data: Data to insert
        """
        insert_query = text("""
            INSERT INTO public.ml_results (
                dataset_id, run_id, model_name, accuracy, precision, recall, 
                f1_score, roc_auc, cv_accuracy_mean, cv_accuracy_std, 
                cv_precision_mean, cv_precision_std, cv_recall_mean, cv_recall_std,
                cv_f1_mean, cv_f1_std, cv_roc_auc_mean, cv_roc_auc_std,
                confusion_matrix, classification_report, is_best_model,
                test_size, cv_folds, random_state
            ) VALUES (
                :dataset_id, :run_id, :model_name, :accuracy, :precision, 
                :recall, :f1_score, :roc_auc, :cv_accuracy_mean, :cv_accuracy_std,
                :cv_precision_mean, :cv_precision_std, :cv_recall_mean, :cv_recall_std,
                :cv_f1_mean, :cv_f1_std, :cv_roc_auc_mean, :cv_roc_auc_std,
                :confusion_matrix, :classification_report, :is_best_model,
                :test_size, :cv_folds, :random_state
            )
        """)

        session.execute(insert_query, data)

    def get_best_models(self, dataset_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get best models for a dataset.

        Args:
            dataset_id: Dataset identifier
            limit: Maximum number of results to return

        Returns:
            List of best model records
        """
        try:
            query = text("""
                SELECT model_name, accuracy, roc_auc, created_at
                FROM public.ml_results 
                WHERE dataset_id = :dataset_id AND is_best_model = true
                ORDER BY created_at DESC 
                LIMIT :limit
            """)

            with self._get_connection() as session:
                result = session.execute(query, {"dataset_id": dataset_id, "limit": limit})
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"Error reading best models: {e}")
            return []

    def get_model_comparison(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Get model comparison for a specific run.

        Args:
            dataset_id: Dataset identifier

        Returns:
            List of model results for the run
        """
        try:
            query = text("""
                SELECT model_name, accuracy, precision, recall, 
                       f1_score, roc_auc, is_best_model
                FROM public.ml_results 
                WHERE dataset_id = :dataset_id AND run_id = :run_id
                ORDER BY accuracy DESC
            """)

            with self._get_connection() as session:
                result = session.execute(
                    query,
                    {"dataset_id": dataset_id, "run_id": self.run_id}
                )
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"Error reading model comparison: {e}")
            return []

    def get_performance_history(
        self,
        dataset_id: str,
        model_name: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get performance history for a specific model.

        Args:
            dataset_id: Dataset identifier
            model_name: Model name
            limit: Maximum number of results to return

        Returns:
            List of historical performance records
        """
        try:
            query = text("""
                SELECT run_id, accuracy, roc_auc, created_at
                FROM public.ml_results 
                WHERE dataset_id = :dataset_id AND model_name = :model_name
                ORDER BY created_at DESC 
                LIMIT :limit
            """)

            with self._get_connection() as session:
                result = session.execute(
                    query,
                    {"dataset_id": dataset_id, "model_name": model_name, "limit": limit}
                )
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"Error reading performance history: {e}")
            return []
