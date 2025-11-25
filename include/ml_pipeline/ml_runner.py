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

logger = logging.getLogger("airflow.task")

class MLRunner:
    """
    Main ML pipeline runner for German Credit dataset.
    """
    
    def __init__(self, test_size: float = 0.2, random_state: int = 42, cv_folds: int = 5):
        """
        Initialize ML pipeline runner.
        
        Args:
            test_size: Proportion of test set
            random_state: Random seed for reproducibility
            cv_folds: Number of cross-validation folds
        """
        self.test_size = test_size
        self.random_state = random_state
        self.cv_folds = cv_folds
        self.preprocessor = None
        self.models = {}
        self.results = {}
    
    def load_and_preprocess_data(self, csv_path: str) -> tuple:
        """
        Load and preprocess data from CSV file.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Tuple of (x_train, x_test, y_train, y_test, preprocessor)
            
        Raises:
            FileNotFoundError: If CSV file doesn't exist
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        # Load data
        df = load_csv_as_df(csv_path, normalize_colnames=True)
        logger.info(f"Data loaded: {df.shape}")
        
        # Initialize preprocessor
        preprocessor = DataPreprocessor(
            test_size=self.test_size, 
            random_state=self.random_state
        )
        
        # Prepare target and features
        x, y = preprocessor.prepare_target(df)
        preprocessor.identify_feature_types(x)
        preprocessor.create_preprocessor()
        
        x_train, x_test, y_train, y_test = preprocessor.split_data(x, y)
        
        self.preprocessor = preprocessor
        return x_train, x_test, y_train, y_test, preprocessor.preprocessor
    
    def train_models(self, preprocessor, x_train: pd.DataFrame, y_train: pd.Series) -> Dict[str, Any]:
        """
        Train multiple models.
        
        Args:
            preprocessor: Feature preprocessor
            x_train: Training features
            y_train: Training target
            
        Returns:
            Dictionary of trained models
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
    
    def evaluate_pipeline(self, models: Dict[str, Any], x_test: pd.DataFrame, 
                         y_test: pd.Series, x_full: pd.DataFrame, y_full: pd.Series,
                         dataset_name: str) -> Dict[str, Any]:
        """
        Evaluate all models and return comprehensive results.
        
        Args:
            models: Dictionary of trained models
            x_test: Test features
            y_test: Test target
            x_full: Full dataset features (for CV)
            y_full: Full dataset target (for CV)
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary with evaluation results
        """
        evaluator = ModelEvaluator(cv_folds=self.cv_folds, random_state=self.random_state)
        
        # Test set evaluation
        test_results = evaluator.compare_models(models, x_test, y_test, dataset_name)
        
        # Cross-validation for each model
        cv_results = {}
        for model_name, model in models.items():
            cv_results[model_name] = evaluator.cross_validate_model(
                model, x_full, y_full, model_name
            )
        
        results = {
            "dataset": dataset_name,
            "test_results": test_results,
            "cross_validation_results": cv_results,
            "best_model": test_results[0]["model"],
            "best_accuracy": test_results[0]["accuracy"]
        }
        
        logger.info(f"ML pipeline evaluation completed for {dataset_name}")
        logger.info(f"Best model: {results['best_model']} "
                   f"(Accuracy: {results['best_accuracy']:.3f})")
        
        self.results = results
        return results
    
    def run_full_pipeline(self, csv_path: str, dataset_name: str = None) -> Dict[str, Any]:
        """
        Run complete ML pipeline from data loading to evaluation.
        
        Args:
            csv_path: Path to CSV file
            dataset_name: Name of the dataset (optional)
            
        Returns:
            Complete evaluation results
        """
        if dataset_name is None:
            dataset_name = os.path.basename(csv_path).replace('.csv', '')
        
        logger.info(f"Starting ML pipeline for: {dataset_name}")
        
        try:
            x_train, x_test, y_train, y_test, preprocessor = self.load_and_preprocess_data(csv_path)
            
            # Full dataset for cross-validation
            df = load_csv_as_df(csv_path)
            x_full, y_full = DataPreprocessor().prepare_target(df)
            
            # Train models
            models = self.train_models(preprocessor, x_train, y_train)
            
            # Evaluate models
            results = self.evaluate_pipeline(
                models, x_test, y_test, x_full, y_full, dataset_name
            )
            
            logger.info(f"ML pipeline completed successfully for {dataset_name}")
            return results
            
        except Exception as e:
            logger.error(f"ML pipeline failed for {dataset_name}: {e}")
            raise

    def run_full_pipeline_with_saving(self, csv_path: str, dataset_name: str = None, 
                                    shared_run_id: str = 'run_id', save_to_db: bool = True) -> Dict[str, Any]:
        """
        Run complete ML pipeline with database saving.
        
        Args:
            csv_path: Path to CSV file
            dataset_name: Name of the dataset (optional)
            run_id: shared run id with DQ
            save_to_db: Whether to save results to database
            
        Returns:
            Complete evaluation results
        """
        if dataset_name is None:
            dataset_name = os.path.basename(csv_path).replace('.csv', '')
        
        dq_conn = os.environ["AIRFLOW_CONN_DQ_POSTGRES"]
        # Run id based on time - dynamic
        #run_id = f"ml_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Starting ML pipeline with saving: {dataset_name}, {shared_run_id}")
        
        try:
            results = self.run_full_pipeline(csv_path, dataset_name)
            
            if save_to_db:
                saver = MLResultSaver(dq_conn)
                success = saver.save_ml_results(
                    dataset_id=dataset_name,
                    run_id=shared_run_id,
                    ml_results=results,
                    test_size=self.test_size,
                    cv_folds=self.cv_folds,
                    random_state=self.random_state
                )
                
                if success:
                    logger.info(f"ML results saved to database: {shared_run_id}")

                    best_model = results["best_model"]
                    best_accuracy = results["best_accuracy"]
                    logger.info(f"Best model saved: {best_model} (Accuracy: {best_accuracy:.3f})")
                else:
                    logger.warning("Failed to save ML results to database")
            
            results['shared_run_id'] = shared_run_id
            return results
            
        except Exception as e:
            logger.error(f"ML pipeline with saving failed: {e}")
            raise