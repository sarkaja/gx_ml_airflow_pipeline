# include/database/postgres_manager.py
from __future__ import annotations
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

logger = logging.getLogger(__name__)
# dq_result_saver.py


import logging
import os 
from scripts.get_environment_config import ProjectConfig
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger("airflow.task")



class PostgresManager:
    def __init__(self):
        cfg = ProjectConfig()
        self.connection_string = os.getenv('AIRFLOW_CONN_DQ_POSTGRES')
        self.run_id = cfg.get_run_id()
        self.engine = create_engine(self.connection_string)
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine,
        )

    def _get_connection(self):
        """Vytvoří SQLAlchemy session (stejný styl jako v DQResultSaver)."""
        return self.SessionLocal()


    def save_clean_data(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        data_source: str,
        table_name: str 
    ) -> bool:
        """
        Save cleaned dataset into the raw_german_credit_data relational table.

        Args:
            df: DataFrame with the cleaned or original data
            dataset_id: Version of dataset ("initial_load_v1"...)
            data_source: Source of data ("raw", "cleaned", "original", ...)
            table_name: Target DB table (default: public.raw_german_credit_data)

        Returns:
            True if save was successful, False otherwise.
        """

        try:
            logger.info(f"Saving cleaned data into table {table_name}")

            # 1) Expected columns in DB
            required_columns = [
                "checking_account_status",
                "duration_in_month",
                "credit_history",
                "purpose",
                "credit_amount",
                "savings_account_bonds",
                "employment",
                "installment",
                "status_n_sex",
                "other_debtors_guarantors",
                "residence",
                "property",
                "age_in_years",
                "other_installment_plans",
                "housing",
                "existing_credits_no",
                "job",
                "liability_responsibles",
                "telephone",
                "foreign_worker",
                "category",
            ]

            # 2) Check for missing columns
            missing_cols = [c for c in required_columns if c not in df.columns]
            if missing_cols:
                raise ValueError(f"DataFrame is missing required columns: {missing_cols}")

            df_to_save = df[required_columns].copy()

            # 3) Convert DataFrame rows → list of dicts (for executemany bulk insert)
            records = df_to_save.to_dict(orient="records")

            if not records:
                logger.warning("No records to save (DataFrame is empty).")
                return True

            # 4) Build INSERT query (normal relational table)
            insert_query = text(f"""
                INSERT INTO public.{table_name} (
                    dataset_id,
                    data_source,
                    checking_account_status,
                    duration_in_month,
                    credit_history,
                    purpose,
                    credit_amount,
                    savings_account_bonds,
                    employment,
                    installment,
                    status_n_sex,
                    other_debtors_guarantors,
                    residence,
                    property,
                    age_in_years,
                    other_installment_plans,
                    housing,
                    existing_credits_no,
                    job,
                    liability_responsibles,
                    telephone,
                    foreign_worker,
                    category,
                    run_id
                )
                VALUES (
                    :dataset_id,
                    :data_source,
                    :checking_account_status,
                    :duration_in_month,
                    :credit_history,
                    :purpose,
                    :credit_amount,
                    :savings_account_bonds,
                    :employment,
                    :installment,
                    :status_n_sex,
                    :other_debtors_guarantors,
                    :residence,
                    :property,
                    :age_in_years,
                    :other_installment_plans,
                    :housing,
                    :existing_credits_no,
                    :job,
                    :liability_responsibles,
                    :telephone,
                    :foreign_worker,
                    :category,
                    :run_id
                )
            """)

            # 5) Add metadata to each record
            for r in records:
                r["dataset_id"] = dataset_id
                r["data_source"] = data_source
                r["run_id"] = self.run_id

            # 6) Execute bulk insert
            with self._get_connection() as session:
                session.execute(insert_query, records)
                session.commit()

            logger.info(f"Successfully saved {len(records)} rows into {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error saving cleaned data: {e}")
            return False

    def load_selected_columns(
        self,
        dataset_id: str = None,
        run_id: str = None,
        data_source: str = None,
        corruption_scenario: str = None,
        corruption_type: str = None,
        table_name: str = None
    ) -> pd.DataFrame:
        """
        Load only selected columns from the given table into a DataFrame.

        Args:
            table_name: Name of the table to read from.
            dataset_id: Optional filter for dataset version.
            corruption_type: Optional filter for corrupted dataset type.
            run_id: Optional filter for pipeline run ID.

        Returns:
            Pandas DataFrame with only the requested columns.
        """

        selected_columns = [
            "checking_account_status",
            "duration_in_month",
            "credit_history",
            "purpose",
            "credit_amount",
            "savings_account_bonds",
            "employment",
            "installment",
            "status_n_sex",
            "other_debtors_guarantors",
            "residence",
            "property",
            "age_in_years",
            "other_installment_plans",
            "housing",
            "existing_credits_no",
            "job",
            "liability_responsibles",
            "telephone",
            "foreign_worker",
            "category"
        ]

        try:
            logger.info(f"Loading selected columns from table {table_name}")

            # Build SELECT query
            cols = ", ".join(selected_columns)
            query = f"SELECT {cols} FROM {table_name} WHERE 1=1"
            params = {}

            if dataset_id:
                query += " AND dataset_id = :dataset_id"
                params["dataset_id"] = dataset_id

            if corruption_type:
                query += " AND corruption_type = :corruption_type"
                params["corruption_type"] = corruption_type

            if run_id:
                query += " AND run_id = :run_id"
                params["run_id"] = run_id
            
            if corruption_scenario:
                query += " AND corruption_scenario = :corruption_scenario"
                params["corruption_scenario"] = corruption_scenario

            if corruption_type:
                query += " AND corruption_type = :corruption_type"
                params["corruption_type"] = corruption_type

            query += " ORDER BY id DESC LIMIT 1000"

            sql = text(query)

            # Execute & load into DataFrame
            with self._get_connection() as session:
                result = session.execute(sql, params)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

            logger.info(f"Loaded {len(df)} rows from {table_name}")
            df=df.replace({"NaN": None, "[null]": None})
            return df

        except Exception as e:
            logger.error(f"Error loading selected columns: {e}")
            return pd.DataFrame()

    def load_clean_data(
        self,
        dataset_id: str = None,
        run_id: str = None,
        data_source: str = None,
        corruption_scenario: str = None,
        corruption_type: str = None,
        table_name: str = None
    ) -> pd.DataFrame:
        """
        Load cleaned/processed credit data into a pandas DataFrame.

        Args:
            dataset_id: Optional filter for dataset version
            run_id: Optional filter for run identifier
            data_source: Optional filter for data source
            table_name: Table to load from (default: raw_german_credit_data)

        Returns:
            DataFrame containing the requested data.
        """

        try:
            logger.info(f"Loading data from table {table_name}")

            # 1) Base SELECT query
            query = f"SELECT * FROM public.{table_name} WHERE 1=1"
            params = {}

            # 2) Add filters if provided
            if dataset_id:
                query += " AND dataset_id = :dataset_id"
                params["dataset_id"] = dataset_id

            if run_id:
                query += " AND run_id = :run_id"
                params["run_id"] = run_id

            if data_source:
                query += " AND data_source = :data_source"
                params["data_source"] = data_source
            
            if corruption_scenario:
                query += " AND corruption_scenario = :corruption_scenario"
                params["corruption_scenario"] = corruption_scenario

            if corruption_type:
                query += " AND corruption_type = :corruption_type"
                params["corruption_type"] = corruption_type

            query += " ORDER BY id ASC"  # optional ordering

            sql = text(query)

            # 3) Execute & read into DataFrame
            with self._get_connection() as session:
                result = session.execute(sql, params)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

            logger.info(f"Loaded {len(df)} rows from database.")
            df=df.replace({"NaN": None, "[null]": None})
            return df

        except Exception as e:
            logger.error(f"Error loading data from {table_name}: {e}")
            return pd.DataFrame() 

    

    def save_corrupted_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        corruption_scenario: str,
        corruption_type: str,
        dataset_id: str,
    ) -> bool:
        """
        Save corrupted dataset into a relational table with original_row_id mapping.

        Args:
            df: DataFrame with corrupted data. Expected columns:
                - id (original row id)
                - corruption_scenario
                - data_source
                - checking_account_status
                - duration_in_month
                - credit_history
                - purpose
                - credit_amount
                - savings_account_bonds
                - employment
                - installment
                - status_n_sex
                - other_debtors_guarantors
                - residence
                - property
                - age_in_years
                - other_installment_plans
                - housing
                - existing_credits_no
                - job
                - liability_responsibles
                - telephone
                - foreign_worker
                - category
                - loaded_at
                - run_id
            table_name: Target DB table name (e.g. 'public.corrupted_german_credit_data')
            dataset_id: Version string to store in target table
            corruption_type: Type of corruption applied (to store in target table)

        Returns:
            True if save was successful, False otherwise.
        """
        def _safe_int(v):
            return None if pd.isna(v) else int(v)

        def _safe_str(v):
            return None if pd.isna(v) else v
        try:
            logger.info(f"Saving corrupted data into table {table_name}")

            # 1) Expected columns in INPUT DF
            required_input_columns = [
                "id",
                "checking_account_status",
                "duration_in_month",
                "credit_history",
                "purpose",
                "credit_amount",
                "savings_account_bonds",
                "employment",
                "installment",
                "status_n_sex",
                "other_debtors_guarantors",
                "residence",
                "property",
                "age_in_years",
                "other_installment_plans",
                "housing",
                "existing_credits_no",
                "job",
                "liability_responsibles",
                "telephone",
                "foreign_worker",
                "category",
                "run_id",
            ]

            missing_cols = [c for c in required_input_columns if c not in df.columns]
            if missing_cols:
                raise ValueError(f"DataFrame is missing required columns: {missing_cols}")
            logger.info(f"Saving corrupted data into table {df}")
            # 2) Připravíme records pro insert
            records = []
            for _, row in df.iterrows():
                rec = {
                    # metadata
                    "corruption_scenario": corruption_scenario,
                    "corruption_type": corruption_type,
                    "dataset_id": dataset_id,
                    
                    "original_row_id": _safe_int(row["id"]),

                    # data columns
                    "checking_account_status": _safe_str(row["checking_account_status"]),
                    "duration_in_month": _safe_int(row["duration_in_month"]),
                    "credit_history": _safe_str(row["credit_history"]),
                    "purpose": _safe_str(row["purpose"]),
                    "credit_amount": _safe_int(row["credit_amount"]),
                    "savings_account_bonds": _safe_str(row["savings_account_bonds"]),
                    "employment": _safe_str(row["employment"]),
                    "installment": _safe_int(row["installment"]),
                    "status_n_sex": _safe_str(row["status_n_sex"]),
                    "other_debtors_guarantors": _safe_str(row["other_debtors_guarantors"]),
                    "residence": _safe_int(row["residence"]),
                    "property": _safe_str(row["property"]),
                    "age_in_years": _safe_int(row["age_in_years"]),
                    "other_installment_plans": _safe_str(row["other_installment_plans"]),
                    "housing": _safe_str(row["housing"]),
                    "existing_credits_no": _safe_int(row["existing_credits_no"]),
                    "job": _safe_str(row["job"]),
                    "liability_responsibles": _safe_int(row["liability_responsibles"]),
                    "telephone": _safe_str(row["telephone"]),
                    "foreign_worker": _safe_str(row["foreign_worker"]),
                    "category": _safe_int(row["category"]),
                    "run_id": self.run_id
                }
                records.append(rec)

            if not records:
                logger.warning("No records to save (DataFrame is empty).")
                return True

            # 3) INSERT query – žádné dataset_id/data_source z df, jen argumenty
            insert_query = text(f"""
                INSERT INTO {table_name} (
                    dataset_id,
                    corruption_scenario,
                    corruption_type,
                    original_row_id,
                    checking_account_status,
                    duration_in_month,
                    credit_history,
                    purpose,
                    credit_amount,
                    savings_account_bonds,
                    employment,
                    installment,
                    status_n_sex,
                    other_debtors_guarantors,
                    residence,
                    property,
                    age_in_years,
                    other_installment_plans,
                    housing,
                    existing_credits_no,
                    job,
                    liability_responsibles,
                    telephone,
                    foreign_worker,
                    category,
                    run_id
                )
                VALUES (
                    :dataset_id,
                    :corruption_scenario,
                    :corruption_type,
                    :original_row_id,
                    :checking_account_status,
                    :duration_in_month,
                    :credit_history,
                    :purpose,
                    :credit_amount,
                    :savings_account_bonds,
                    :employment,
                    :installment,
                    :status_n_sex,
                    :other_debtors_guarantors,
                    :residence,
                    :property,
                    :age_in_years,
                    :other_installment_plans,
                    :housing,
                    :existing_credits_no,
                    :job,
                    :liability_responsibles,
                    :telephone,
                    :foreign_worker,
                    :category,
                    :run_id
                )
            """)

            # 4) Bulk insert
            with self._get_connection() as session:
                session.execute(insert_query, records)
                session.commit()

            logger.info(f"Successfully saved {len(records)} corrupted rows into {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error saving corrupted data into {table_name}: {e}")
            return False
        

    def _update_dataset_metadata(
        self,
        table_name: str,
        version: str,
        row_count: int,
        column_count: int
    ):
        """
        Aktualizace/insert do dataset_metadata pomocí SQLAlchemy session + text dotazů
        (stejný pattern jako v DQResultSaver).
        """
        try:
            table_to_type = {
                'raw_german_credit_data': 'raw',
                'corrupted_german_credit_data': 'corrupted',
                'remediated_german_credit_data': 'remediated'
            }
            dataset_type = table_to_type.get(table_name, 'unknown')
            dataset_name = f"{dataset_type}_{version}"

            with self._get_connection() as session:
                check_query = text(
                    "SELECT id FROM dataset_metadata WHERE dataset_name = :name"
                )
                existing = session.execute(
                    check_query,
                    {'name': dataset_name}
                ).fetchone()

                if existing:
                    update_query = text("""
                        UPDATE dataset_metadata
                        SET row_count = :row_count,
                            column_count = :col_count,
                            updated_at = NOW()
                        WHERE dataset_name = :name
                    """)
                    session.execute(
                        update_query,
                        {
                            'row_count': row_count,
                            'col_count': column_count,
                            'name': dataset_name
                        }
                    )
                else:
                    insert_query = text("""
                        INSERT INTO dataset_metadata
                        (dataset_name, dataset_type, row_count, column_count, current_version)
                        VALUES (:name, :type, :row_count, :col_count, :version)
                    """)
                    session.execute(
                        insert_query,
                        {
                            'name': dataset_name,
                            'type': dataset_type,
                            'row_count': row_count,
                            'col_count': column_count,
                            'version': version
                        }
                    )

                session.commit()
        except Exception as e:
            logger.error(f"Could not update dataset metadata: {e}")

 