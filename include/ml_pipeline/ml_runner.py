# include/ml_pipeline/ml_runner.py
from __future__ import annotations

import pandas as pd
from datetime import datetime
import os
from typing import Dict, Any, List
import logging

from .data_preprocessor import DataPreprocessor, load_csv_as_df
from .model_factory import ModelFactory
from .model_evaluator import ModelEvaluator
from .ml_result_saver import MLResultSaver
from .stats_check import evaluate_remediation_bias

logger = logging.getLogger("airflow.task")


class MLRunner:
    """
    End-to-end machine learning pipeline runner for the German Credit dataset.
    Handles preprocessing, model training, evaluation, and optional persistence.
    """

    def __init__(self, test_size: float = 0.2, random_state: int = 42, cv_folds: int = 5):
        """
        Initialize the ML pipeline runner.

        Args:
            test_size: Proportion of samples for the test split.
            random_state: Random seed for reproducibility.
            cv_folds: Number of folds for cross-validation.
        """
        self.test_size = test_size
        self.random_state = random_state
        self.cv_folds = cv_folds
        self.preprocessor = None
        self.models = {}
        self.results = {}

    def load_and_preprocess_data(self, df: pd.DataFrame) -> tuple:
        """
        Prepare data for ML: target extraction, imputation, encoding, and splitting.

        Args:
            df: Input dataframe.

        Returns:
            Tuple of (X_train, X_test, y_train, y_test, feature_preprocessor)

        Raises:
            ValueError: If the data is malformed or missing required columns.
        """
        logger.info(f"Data loaded: {df.shape}")

        preprocessor = DataPreprocessor(
            test_size=self.test_size,
            random_state=self.random_state,
        )

        X, y = preprocessor.prepare_target(df)
        X = preprocessor.basic_imputation(X)

        preprocessor.identify_feature_types(X)
        preprocessor.create_preprocessor()

        x_train, x_test, y_train, y_test = preprocessor.split_data(X, y)
        self.preprocessor = preprocessor

        return x_train, x_test, y_train, y_test, preprocessor.preprocessor

    def train_models(self, preprocessor, x_train: pd.DataFrame, y_train: pd.Series) -> Dict[str, Any]:
        """
        Train all supported ML models.

        Args:
            preprocessor: Feature transformation pipeline.
            x_train: Training features.
            y_train: Training target.

        Returns:
            Dictionary of trained model objects.
        """
        models = {}

        # Logistic Regression
        lr_model = ModelFactory.create_logistic_regression(preprocessor)
        lr_model.fit(x_train, y_train)
        models["logistic_regression"] = lr_model
        logger.info("Logistic Regression trained")

        # Random Forest
        rf_model = ModelFactory.create_random_forest(preprocessor)
        rf_model.fit(x_train, y_train)
        models["random_forest"] = rf_model
        logger.info("Random Forest trained")

        self.models = models
        return models

    def evaluate_pipeline(
        self,
        models: Dict[str, Any],
        x_test: pd.DataFrame,
        y_test: pd.Series,
        x_full: pd.DataFrame,
        y_full: pd.Series,
        dataset_id: str,
    ) -> Dict[str, Any]:
        """
        Evaluate trained models using test split and cross-validation.

        Args:
            models: Dictionary of trained models.
            x_test: Test features.
            y_test: Test target.
            x_full: Full feature dataset.
            y_full: Full target dataset.
            dataset_id: Dataset identifier.

        Returns:
            Dictionary containing test metrics, CV results, and best model summary.
        """
        evaluator = ModelEvaluator(cv_folds=self.cv_folds, random_state=self.random_state)

        # Test set comparison
        test_results = evaluator.compare_models(models, x_test, y_test, dataset_id)

        # Cross-validation results
        cv_results = {
            model_name: evaluator.cross_validate_model(model, x_full, y_full, model_name)
            for model_name, model in models.items()
        }

        results = {
            "dataset": dataset_id,
            "test_results": test_results,
            "cross_validation_results": cv_results,
            "best_model": test_results[0]["model"],
            "best_accuracy": test_results[0]["accuracy"],
        }

        logger.info(f"ML evaluation completed for {dataset_id}")
        logger.info(
            f"Best model: {results['best_model']} "
            f"(Accuracy: {results['best_accuracy']:.3f})"
        )

        self.results = results
        return results

    def run_full_pipeline(self, df: pd.DataFrame, dataset_id: str = None) -> Dict[str, Any]:
        """
        Execute the complete machine learning pipeline:
        preprocessing → training → evaluation → drift analysis.

        Args:
            df: Input dataframe.
            dataset_id: Dataset identifier (optional).

        Returns:
            Dictionary with ML evaluation metrics.

        Raises:
            Exception: If any pipeline component fails.
        """
        logger.info(f"Starting ML pipeline for: {dataset_id}")

        try:
            x_train, x_test, y_train, y_test, preprocessor = self.load_and_preprocess_data(df)

            # Full dataset (for CV + drift analysis)
            x_full, y_full = DataPreprocessor().prepare_target(df)
            x_full = DataPreprocessor().basic_imputation(x_full)

            # Train models
            models = self.train_models(preprocessor, x_train, y_train)

            # Evaluate models
            results = self.evaluate_pipeline(
                models, x_test, y_test, x_full, y_full, dataset_id
            )

            # Bias & drift analysis
            logger.info("Evaluating remediation distribution drift...")
            drift_report = evaluate_remediation_bias(x_full)

            for col, metrics in drift_report.items():
                dominant_growth = metrics.get("dominant_growth", 0)
                psi = metrics.get("psi", 0)
                kl = metrics.get("kl_divergence", 0)

                if abs(dominant_growth) > 0.10:
                    logger.warning(
                        f"[{col}] dominant value increased by {dominant_growth:.2f} → potential bias"
                    )
                if psi > 0.25:
                    logger.warning(f"[{col}] PSI={psi:.3f} → strong distribution drift")
                if kl > 0.2:
                    logger.warning(f"[{col}] KL divergence={kl:.3f} → significant change in distribution")

            logger.info(f"ML pipeline completed successfully for {dataset_id}")
            return results

        except Exception as e:
            logger.error(f"ML pipeline failed for {dataset_id}: {e}")
            raise

    def run_full_pipeline_with_saving(
        self,
        df: pd.DataFrame,
        dataset_id: str = None,
        save_to_db: bool = True,
    ) -> Dict[str, Any]:
        """
        Run the full ML pipeline and optionally store results in the database.

        Args:
            df: Input dataframe.
            dataset_id: Dataset identifier.
            save_to_db: Whether to persist ML results to PostgreSQL.

        Returns:
            Dictionary containing evaluation results and metadata.

        Raises:
            Exception: If saving or evaluation fails.
        """
        logger.info(f"Starting ML pipeline with saving: {dataset_id}")

        try:
            results = self.run_full_pipeline(df, dataset_id)

            if save_to_db:
                saver = MLResultSaver()
                success = saver.save_ml_results(
                    dataset_id=dataset_id,
                    ml_results=results,
                    test_size=self.test_size,
                    cv_folds=self.cv_folds,
                    random_state=self.random_state,
                )

                if success:
                    logger.info("ML results saved to database")
                    logger.info(
                        f"Best model saved: {results['best_model']} "
                        f"(Accuracy: {results['best_accuracy']:.3f})"
                    )
                else:
                    logger.warning("Failed to save ML results to database")

            return results

        except Exception as e:
            logger.error(f"ML pipeline with saving failed: {e}")
            raise
