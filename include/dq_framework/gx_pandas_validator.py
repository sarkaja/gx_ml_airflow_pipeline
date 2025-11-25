# gx_pandas_validator.py
from __future__ import annotations

from typing import Any, Dict, Optional

import great_expectations as gx
from great_expectations.core import ExpectationSuite


class PandasYamlValidator:
    """
    A Great Expectations validator for pandas DataFrames using expectation configurations.

    """
    
    def __init__(self):
        """
        Initialize the validator with a Great Expectations context and pandas data source.
        
        Creates or reuses a pandas datasource named "pandas_ds" for DataFrame validation.
        """
        self.context = gx.get_context()
        self.data_source = self._get_or_create_pandas_datasource("pandas_ds")

    def _get_or_create_pandas_datasource(self, name: str):
        """
        Get an existing pandas datasource or create a new one if it doesn't exist.
        
        Args:
            name (str): The name of the datasource to retrieve or create.
            
        Returns:
            Datasource: The existing or newly created pandas datasource.
        """
        try:
            return self.context.data_sources.get(name)
        except Exception:
            # Create new if doesn't exist
            return self.context.data_sources.add_pandas(name=name)

    def _get_or_create_suite(
        self,
        suite_name: str,
        suite_meta: Optional[dict] = None
    ) -> ExpectationSuite:
        """
        Retrieve an existing expectation suite or create a new one.
        
        Args:
            suite_name (str): The name of the expectation suite.
            suite_meta (Optional[dict]): Metadata to attach to the suite.
            
        Returns:
            ExpectationSuite: The existing or newly created expectation suite.
        """
        try:
            suite = self.context.suites.get(suite_name)
            if suite_meta:
                suite.meta.update(suite_meta)
            return suite
        except Exception:
            # Create new suite if doesn't exist
            suite = gx.ExpectationSuite(name=suite_name, meta=suite_meta or {})
            return self.context.suites.add(suite)

    def _get_or_create_data_asset(self, data_source, data_asset_name: str):
        """
        Get an existing data asset or create a new dataframe asset.
        
        Args:
            data_source: The datasource containing the data asset.
            data_asset_name (str): The name of the data asset.
            
        Returns:
            DataAsset: The existing or newly created dataframe data asset.
        """
        try:
            return data_source.get_asset(data_asset_name)
        except Exception:
            return data_source.add_dataframe_asset(name=data_asset_name)

    def validate(
        self,
        df,
        expectations,                 
        config,
        run_id=None,
        result_format="COMPLETE"
    ) -> Dict[str, Any]:
        """
        Validate a pandas DataFrame against a set of expectations.
        
        Args:
            df (pandas.DataFrame): The DataFrame to validate.
            expectations (list): List of Expectation configuration objects or instances.
            config: Configuration object containing:
                - expectation_suite_name (str): Name for the expectation suite
                - data_asset_name (str): Name for the data asset
                - batch_definition_name (str): Name for the batch definition
            run_id (Optional[str]): Unique identifier for this validation run.
            result_format (str): Level of detail in results. Defaults to "COMPLETE".
                
        Returns:
            Dict[str, Any]: Validation results in JSON-serializable format including:
                - success (bool): Overall validation status
                - results (list): Detailed results for each expectation
                - statistics (dict): Summary statistics
                - meta (dict): Metadata about the validation
                - id (str): The run_id provided or generated
                
        """
        suite = self._get_or_create_suite(config.expectation_suite_name)
        suite.expectations = []
        for expectation in expectations:
            suite.add_expectation(expectation)

        self.context.suites.add_or_update(suite)

        data_asset = self._get_or_create_data_asset(
            self.data_source, 
            config.data_asset_name
        )

        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            name=config.batch_definition_name
        )
        batch = batch_definition.get_batch({"dataframe": df})

        validator = self.context.get_validator(
            batch=batch,
            expectation_suite=suite,
        )

        validation_results = validator.validate(
            run_id=run_id,
            result_format={
                "result_format": result_format,
            },
        )

        out = validation_results.to_json_dict()
        out["id"] = run_id

        return out