# include/ml_pipeline/model_evaluator.py
from __future__ import annotations

import pandas as pd
import numpy as np
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score,
    accuracy_score, precision_score, recall_score, f1_score
)
from sklearn.model_selection import StratifiedKFold, cross_validate
from typing import Dict, Any, List
import logging
import json

logger = logging.getLogger("airflow.task")

class ModelEvaluator:
    """
    Evaluate machine learning models with comprehensive metrics.
    """
    
    def __init__(self, cv_folds: int = 5, random_state: int = 42):
        """
        Initialize model evaluator.
        
        Args:
            cv_folds: Number of cross-validation folds
            random_state: Random seed for reproducibility
        """
        self.cv_folds = cv_folds
        self.random_state = random_state
  
    def evaluate_model(self, model, x_test: pd.DataFrame, y_test: pd.Series, 
                      model_name: str, dataset_name: str) -> Dict[str, Any]:
        """
        Evaluate model on test set.
        
        Args:
            model: Trained model
            x_test: Test features
            y_test: Test target
            model_name: Name of the model
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary with evaluation metrics
        """
        # Generate predictions
        y_pred = model.predict(x_test)
        y_proba = model.predict_proba(x_test)[:, 1]
        
        # Calculate metrics
        metrics = {
            "dataset": dataset_name,
            "model": model_name,
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, average="weighted"),
            "recall": recall_score(y_test, y_pred, average="weighted"),
            "f1_score": f1_score(y_test, y_pred, average="weighted"),
            "roc_auc": roc_auc_score(y_test, y_proba),
            "confusion_matrix": confusion_matrix(y_test, y_pred).tolist()
        }
        
        # Classification report 
        metrics["classification_report"] = classification_report(
            y_test, y_pred, target_names=["good(0)", "bad(1)"], output_dict=False
        )
        
        logger.info(f"{model_name} evaluation completed on {dataset_name}")
        logger.info(f"Accuracy: {metrics['accuracy']:.3f}, ROC-AUC: {metrics['roc_auc']:.3f}")
        
        return metrics
    
    def cross_validate_model(self, model, x: pd.DataFrame, y: pd.Series, 
                           model_name: str) -> Dict[str, Any]:
        """
        Perform cross-validation on model.
        
        Args:
            model: Model to evaluate
            x: Features
            y: Target
            model_name: Name of the model
            
        Returns:
            Dictionary with cross-validation results
        """
        cv = StratifiedKFold(
            n_splits=self.cv_folds, 
            shuffle=True, 
            random_state=self.random_state
        )
        
        scoring = {
            "accuracy": "accuracy",
            "precision": "precision_weighted",
            "recall": "recall_weighted", 
            "f1": "f1_weighted",
            "roc_auc": "roc_auc",
        }
        
        cv_results = cross_validate(
            model, x, y, 
            cv=cv, 
            scoring=scoring, 
            n_jobs=-1,
            return_train_score=False
        )
        
        # CV results agg
        cv_summary = {}
        for metric_name in scoring.keys():
            scores = cv_results[f"test_{metric_name}"]
            cv_summary[metric_name] = {
                "mean": float(np.mean(scores)),
                "std": float(np.std(scores)),
                "all_scores": scores.tolist()
            }
        
        logger.info(f"{model_name} cross-validation completed")
        logger.info(f"CV Accuracy: {cv_summary['accuracy']['mean']:.3f} ± {cv_summary['accuracy']['std']:.3f}")
        
        return cv_summary
    
    def compare_models(self, models: Dict[str, Any], x_test: pd.DataFrame, 
                      y_test: pd.Series, dataset_name: str) -> List[Dict[str, Any]]:
        """
        Compare multiple models on the same test set.
        
        Args:
            models: Dictionary of model names to trained models
            x_test: Test features
            y_test: Test target
            dataset_name: Name of the dataset
            
        Returns:
            List of evaluation results for each model
        """
        results = []
        
        for model_name, model in models.items():
            metrics = self.evaluate_model(model, x_test, y_test, model_name, dataset_name)
            results.append(metrics)
        
        results.sort(key=lambda x: x["accuracy"], reverse=True)
        
        logger.info(f"Model comparison completed for {dataset_name}")
        logger.info(f"Best model: {results[0]['model']} (Accuracy: {results[0]['accuracy']:.3f})")
        
        return results