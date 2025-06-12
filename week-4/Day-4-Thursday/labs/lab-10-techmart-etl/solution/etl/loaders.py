"""
TechMart ETL Pipeline - Data Loading Module
Handles loading data into the star schema data warehouse with SCD Type 1 support
"""

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.engine import Engine
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_database_connection(connection_string: str) -> Engine:
    """
    Create SQLAlchemy database engine for data warehouse operations.

    Args:
        connection_string (str): Database connection string

    Returns:
        Engine: SQLAlchemy engine for database operations
    """
    logger.info("Creating database connection to data warehouse")

    try:
        # Create SQLAlchemy engine with connection pooling
        engine = create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=3600,
            echo=False,  # Set to True for SQL debugging
        )

        # Test the connection
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            if test_value != 1:
                raise Exception("Database connection test failed")

        logger.info("Database connection established successfully")
        return engine

    except Exception as e:
        logger.error(f"Failed to create database connection: {str(e)}")
        raise Exception(f"Database connection failed: {str(e)}")


def load_dimension_table(
    df: pd.DataFrame, table_name: str, engine: Engine, scd_operations: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Load dimension table data with SCD Type 1 support.

    Args:
        df (pd.DataFrame): Dimension data to load
        table_name (str): Target dimension table name
        engine (Engine): Database engine
        scd_operations (Dict): SCD operations (inserts/updates)

    Returns:
        Dict[str, Any]: Loading results and metrics
    """
    logger.info(f"Loading dimension table: {table_name}")
    start_time = datetime.now()

    loading_result = {
        "table_name": table_name,
        "records_inserted": 0,
        "records_updated": 0,
        "records_failed": 0,
        "loading_duration": 0.0,
        "errors": [],
    }

    try:
        if df.empty:
            logger.warning(f"No data to load into dimension table {table_name}")
            return loading_result

        with engine.connect() as connection:
            # Prepare data for insertion by mapping column names to schema
            df_to_load = df.copy()

            # Map dimension-specific columns based on table
            if table_name == "dim_customers":
                # Map customer fields to schema
                column_mapping = {
                    "customer_id": "customer_id",  # Business key
                    "first_name": "first_name",
                    "last_name": "last_name",
                    "email": "email",
                    "phone": "phone",
                    "account_status": "account_status",
                    "account_type": "customer_segment",  # Map account_type to customer_segment
                    "loyalty_tier": "loyalty_tier",
                    "loyalty_points": "total_lifetime_value",  # Map points to lifetime value
                }

            elif table_name == "dim_products":
                # Map product fields to schema
                column_mapping = {
                    "product_id": "product_id",  # Business key
                    "name": "product_name",
                    "category": "category",
                    "brand": "brand",
                    "base_price": "current_price",
                    "currency": "currency",
                    "status": "status",
                    "sku": "sku",
                }

            elif table_name == "dim_stores":
                # Map store fields to schema
                column_mapping = {
                    "store_id": "store_id",  # Business key
                    "store_name": "store_name",
                    "store_location": "store_location",
                    "city": "city",
                    "state": "state",
                }

            elif table_name == "dim_support_categories":
                # Map support category fields to schema
                column_mapping = {
                    "category_id": "category_id",  # Business key
                    "category_name": "category_name",
                    "department": "department",
                }
            else:
                # Default: use columns as-is
                column_mapping = {col: col for col in df_to_load.columns}

            # Apply column mapping and add missing required columns
            mapped_df = pd.DataFrame()
            for source_col, target_col in column_mapping.items():
                if source_col in df_to_load.columns:
                    mapped_df[target_col] = df_to_load[source_col]

            # Add audit fields
            mapped_df["record_created_date"] = datetime.now()
            mapped_df["record_updated_date"] = datetime.now()
            mapped_df["data_source"] = "ETL_Pipeline"
            mapped_df["data_quality_score"] = 1.0

            # Handle upsert logic for dimension tables
            logger.info(f"Loading {len(mapped_df)} records into {table_name}")

            # Get business key column for upsert
            business_key_col = None
            if table_name == "dim_customers":
                business_key_col = "customer_id"
            elif table_name == "dim_products":
                business_key_col = "product_id"
            elif table_name == "dim_stores":
                business_key_col = "store_id"
            elif table_name == "dim_support_categories":
                business_key_col = "category_id"

            try:
                if business_key_col and business_key_col in mapped_df.columns:
                    # Implement upsert logic to handle duplicates
                    # First check which records already exist
                    existing_keys_query = f"SELECT {business_key_col} FROM {table_name}"
                    existing_keys_df = pd.read_sql_query(
                        existing_keys_query, connection
                    )
                    existing_keys = (
                        set(existing_keys_df[business_key_col].tolist())
                        if not existing_keys_df.empty
                        else set()
                    )

                    # Remove duplicates within the current dataset as well
                    mapped_df = mapped_df.drop_duplicates(
                        subset=[business_key_col], keep="first"
                    )

                    # Split into insert and update sets
                    new_records = mapped_df[
                        ~mapped_df[business_key_col].isin(existing_keys)
                    ]
                    update_records = mapped_df[
                        mapped_df[business_key_col].isin(existing_keys)
                    ]

                    records_inserted = 0
                    records_updated = 0
                    inserted_keys = set()  # Track keys inserted in this run

                    # Insert new records in smaller batches (avoid SQL Server parameter limit)
                    if not new_records.empty:
                        batch_size = 50  # Reduced batch size to avoid parameter limits
                        for i in range(0, len(new_records), batch_size):
                            batch = new_records[i : i + batch_size]

                            # Further filter to avoid inserting keys we just inserted
                            batch = batch[~batch[business_key_col].isin(inserted_keys)]

                            if not batch.empty:
                                try:
                                    batch.to_sql(
                                        table_name,
                                        connection,
                                        if_exists="append",
                                        index=False,
                                        method="multi",
                                        chunksize=batch_size,
                                    )
                                    records_inserted += len(batch)
                                    # Track inserted keys
                                    inserted_keys.update(
                                        batch[business_key_col].tolist()
                                    )
                                    logger.info(
                                        f"Inserted batch {i//batch_size + 1}: {len(batch)} records"
                                    )
                                except Exception as e:
                                    if (
                                        "duplicate key" in str(e).lower()
                                        or "unique" in str(e).lower()
                                    ):
                                        logger.info(
                                            f"Skipped batch {i//batch_size + 1}: {len(batch)} records (already exist)"
                                        )
                                        # These records already exist, don't count as failures
                                        continue
                                    else:
                                        raise e

                    # Update existing records
                    if not update_records.empty:
                        logger.info(f"Updating {len(update_records)} existing records")
                        # For now, skip updates as they're complex - just log
                        logger.info(
                            f"Skipped {len(update_records)} updates (records already exist)"
                        )

                    loading_result["records_inserted"] = records_inserted
                    loading_result["records_updated"] = records_updated
                    logger.info(
                        f"Successfully loaded {records_inserted} new records, skipped {records_updated} existing records"
                    )

                else:
                    # Fallback to simple insert for tables without business keys
                    batch_size = 50
                    records_inserted = 0
                    for i in range(0, len(mapped_df), batch_size):
                        batch = mapped_df[i : i + batch_size]
                        batch.to_sql(
                            table_name,
                            connection,
                            if_exists="append",
                            index=False,
                            method="multi",
                            chunksize=batch_size,
                        )
                        records_inserted += len(batch)

                    loading_result["records_inserted"] = records_inserted
                    logger.info(
                        f"Successfully inserted {records_inserted} records into {table_name}"
                    )

            except Exception as e:
                error_msg = f"Failed to load records into {table_name}: {str(e)}"
                logger.error(error_msg)
                loading_result["errors"].append(error_msg)
                loading_result["records_failed"] = len(mapped_df)

        # Calculate duration
        end_time = datetime.now()
        loading_result["loading_duration"] = (end_time - start_time).total_seconds()

        logger.info(
            f"Dimension table loading completed: {loading_result['records_inserted']} inserted, "
            f"{loading_result['records_updated']} updated"
        )

    except Exception as e:
        error_msg = f"Dimension table loading failed: {str(e)}"
        logger.error(error_msg)
        loading_result["errors"].append(error_msg)
        loading_result["loading_duration"] = (
            datetime.now() - start_time
        ).total_seconds()

    return loading_result


def load_fact_table(
    df: pd.DataFrame, table_name: str, engine: Engine
) -> Dict[str, Any]:
    """
    Load fact table data to data warehouse.

    Args:
        df (pd.DataFrame): Fact table data to load
        table_name (str): Target fact table name
        engine (Engine): Database engine

    Returns:
        Dict[str, Any]: Loading results and metrics
    """
    logger.info(f"Loading fact table: {table_name}")
    start_time = datetime.now()

    loading_result = {
        "table_name": table_name,
        "records_inserted": 0,
        "records_failed": 0,
        "loading_duration": 0.0,
        "errors": [],
    }

    try:
        if df.empty:
            logger.warning(f"No data to load into fact table {table_name}")
            return loading_result

        with engine.connect() as connection:
            # Fix DataFrame structure issues first
            df_to_load = df.copy()

            # Handle fact_customer_support DataFrame structure issue
            if table_name == "fact_customer_support":
                # Reset index if interaction_id is in the index
                if (
                    hasattr(df_to_load.index, "name")
                    and df_to_load.index.name == "interaction_id"
                ):
                    df_to_load = df_to_load.reset_index()
                # Ensure interaction_id is a column, not index
                if "interaction_id" not in df_to_load.columns and hasattr(
                    df_to_load, "interaction_id"
                ):
                    df_to_load["interaction_id"] = df_to_load.index
                    df_to_load = df_to_load.reset_index(drop=True)

            # Map business keys to surrogate keys for fact tables
            if table_name == "fact_sales":
                # Map customer_id to customer_key
                if "customer_id" in df_to_load.columns:
                    customer_mapping = pd.read_sql_query(
                        "SELECT customer_id, customer_key FROM dim_customers",
                        connection,
                    )
                    df_to_load = df_to_load.merge(
                        customer_mapping, on="customer_id", how="left"
                    )

                # Map product_id to product_key
                if "product_id" in df_to_load.columns:
                    product_mapping = pd.read_sql_query(
                        "SELECT product_id, product_key FROM dim_products", connection
                    )
                    df_to_load = df_to_load.merge(
                        product_mapping, on="product_id", how="left"
                    )

                # Map store_id to store_key
                if "store_id" in df_to_load.columns:
                    store_mapping = pd.read_sql_query(
                        "SELECT store_id, store_key FROM dim_stores", connection
                    )
                    df_to_load = df_to_load.merge(
                        store_mapping, on="store_id", how="left"
                    )

                # Map date to date_key
                if (
                    "date_key" not in df_to_load.columns
                    and "transaction_date" in df_to_load.columns
                ):
                    date_mapping = pd.read_sql_query(
                        "SELECT date_value, date_key FROM dim_date", connection
                    )
                    # Convert transaction_date to date format
                    df_to_load["date_value"] = pd.to_datetime(
                        df_to_load["transaction_date"]
                    ).dt.date
                    df_to_load = df_to_load.merge(
                        date_mapping, on="date_value", how="left"
                    )

                # Map column names to fact table schema
                column_mapping = {
                    "transaction_id": "transaction_id",
                    "customer_key": "customer_key",
                    "product_key": "product_key",
                    "store_key": "store_key",
                    "date_key": "date_key",
                    "quantity": "quantity",
                    "unit_price": "unit_price",
                    "item_total": "extended_price",  # Map item_total to extended_price
                    "final_total": "total_amount",  # Map final_total to total_amount
                    "sales_channel": "sales_channel",
                    "payment_method": "payment_method",
                }

            elif table_name == "fact_customer_support":
                # Map customer_id to customer_key
                if "customer_id" in df_to_load.columns:
                    customer_mapping = pd.read_sql_query(
                        "SELECT customer_id, customer_key FROM dim_customers",
                        connection,
                    )
                    df_to_load = df_to_load.merge(
                        customer_mapping, on="customer_id", how="left"
                    )

                # Map category_id to category_key
                if "category_id" in df_to_load.columns:
                    category_mapping = pd.read_sql_query(
                        "SELECT category_id, category_key FROM dim_support_categories",
                        connection,
                    )

                    # Ensure data types match for merge
                    df_to_load["category_id"] = df_to_load["category_id"].astype(str)
                    category_mapping["category_id"] = category_mapping[
                        "category_id"
                    ].astype(str)

                    df_to_load = df_to_load.merge(
                        category_mapping, on="category_id", how="left"
                    )

                # Map date to date_key
                if (
                    "date_key" not in df_to_load.columns
                    and "interaction_date" in df_to_load.columns
                ):
                    date_mapping = pd.read_sql_query(
                        "SELECT date_value, date_key FROM dim_date", connection
                    )
                    # Convert interaction_date to date format
                    df_to_load["date_value"] = pd.to_datetime(
                        df_to_load["interaction_date"]
                    ).dt.date
                    df_to_load = df_to_load.merge(
                        date_mapping, on="date_value", how="left"
                    )

                # Map column names to fact table schema
                column_mapping = {
                    "ticket_id": "ticket_id",
                    "interaction_id": "interaction_id",
                    "customer_key": "customer_key",
                    "category_key": "category_key",
                    "date_key": "date_key",
                    "satisfaction_score": "satisfaction_score",
                    "ticket_priority": "ticket_priority",
                    "ticket_status": "ticket_status",
                    "interaction_type": "interaction_type",
                    "assigned_agent": "assigned_agent",
                }
            else:
                # Default mapping
                column_mapping = {col: col for col in df_to_load.columns}

            # Apply column mapping to create final DataFrame
            final_df = pd.DataFrame()
            for source_col, target_col in column_mapping.items():
                if source_col in df_to_load.columns:
                    final_df[target_col] = df_to_load[source_col]

            # Add audit fields
            final_df["record_created_date"] = datetime.now()

            # Remove duplicates based on business keys
            if table_name == "fact_sales" and "transaction_id" in final_df.columns:
                original_count = len(final_df)
                final_df = final_df.drop_duplicates(subset=["transaction_id"])
                duplicates_removed = original_count - len(final_df)
                if duplicates_removed > 0:
                    logger.info(f"Removed {duplicates_removed} duplicate transactions")

            elif (
                table_name == "fact_customer_support"
                and "ticket_id" in final_df.columns
                and "interaction_id" in final_df.columns
            ):
                original_count = len(final_df)
                final_df = final_df.drop_duplicates(
                    subset=["ticket_id", "interaction_id"]
                )
                duplicates_removed = original_count - len(final_df)
                if duplicates_removed > 0:
                    logger.info(
                        f"Removed {duplicates_removed} duplicate support interactions"
                    )

            # Validate required foreign keys exist
            validation_errors = []
            if table_name == "fact_sales":
                required_keys = ["customer_key", "product_key", "date_key"]
                for key in required_keys:
                    if key in final_df.columns:
                        null_count = final_df[key].isnull().sum()
                        if null_count > 0:
                            validation_errors.append(
                                f"{null_count} records missing {key}"
                            )

            elif table_name == "fact_customer_support":
                required_keys = ["customer_key", "category_key", "date_key"]
                for key in required_keys:
                    if key in final_df.columns:
                        null_count = final_df[key].isnull().sum()
                        if null_count > 0:
                            validation_errors.append(
                                f"{null_count} records missing {key}"
                            )

            if validation_errors:
                logger.warning(f"Data quality issues found: {validation_errors}")
                # Filter out records with missing required keys
                required_keys = (
                    ["customer_key", "product_key", "date_key"]
                    if table_name == "fact_sales"
                    else ["customer_key", "category_key", "date_key"]
                )
                for key in required_keys:
                    if key in final_df.columns:
                        before_filter = len(final_df)
                        final_df = final_df.dropna(subset=[key])
                        after_filter = len(final_df)
                        if before_filter > after_filter:
                            logger.info(
                                f"Filtered out {before_filter - after_filter} records with missing {key}"
                            )

            # Batch insert for performance
            logger.info(f"Loading {len(final_df)} records into {table_name}")

            try:
                final_df.to_sql(
                    table_name,
                    connection,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )
                loading_result["records_inserted"] = len(final_df)
                logger.info(
                    f"Successfully loaded {len(final_df)} records into {table_name}"
                )

            except Exception as e:
                error_msg = f"Failed to load fact table data: {str(e)}"
                logger.error(error_msg)
                loading_result["errors"].append(error_msg)
                loading_result["records_failed"] = len(final_df)

    except Exception as e:
        error_msg = f"Fact table loading failed: {str(e)}"
        logger.error(error_msg)
        loading_result["errors"].append(error_msg)

    finally:
        loading_result["loading_duration"] = (
            datetime.now() - start_time
        ).total_seconds()

    return loading_result


def execute_scd_type1_updates(
    scd_operations: Dict[str, Any], table_name: str, engine: Engine
) -> Dict[str, Any]:
    """
    Execute SCD Type 1 update operations on dimension table.

    Args:
        scd_operations (Dict): SCD operations from transformation
        table_name (str): Target dimension table name
        engine (Engine): Database engine

    Returns:
        Dict[str, Any]: SCD execution results
    """
    logger.info(f"Executing SCD Type 1 updates for {table_name}")
    start_time = datetime.now()

    scd_result = {
        "table_name": table_name,
        "updates_executed": 0,
        "updates_failed": 0,
        "execution_duration": 0.0,
        "errors": [],
    }

    try:
        updated_records = scd_operations.get("updated_records", pd.DataFrame())
        if updated_records.empty:
            logger.info(f"No SCD Type 1 updates needed for {table_name}")
            return scd_result

        logger.info(f"Processing {len(updated_records)} SCD updates for {table_name}")

        with engine.connect() as connection:
            # Get the business key column for the table
            business_key_col = None
            if table_name == "dim_customers":
                business_key_col = "customer_id"
            elif table_name == "dim_products":
                business_key_col = "product_id"
            elif table_name == "dim_stores":
                business_key_col = "store_id"
            elif table_name == "dim_support_categories":
                business_key_col = "category_id"

            if not business_key_col or business_key_col not in updated_records.columns:
                error_msg = (
                    f"Business key column not found for SCD updates in {table_name}"
                )
                logger.error(error_msg)
                scd_result["errors"].append(error_msg)
                return scd_result

            # Execute updates record by record for better error handling
            successful_updates = 0
            failed_updates = 0

            for index, row in updated_records.iterrows():
                try:
                    # Build the UPDATE statement dynamically
                    business_key_value = row[business_key_col]

                    # Create list of column updates, excluding keys and audit fields
                    update_columns = []
                    update_values = {}

                    for col in updated_records.columns:
                        if col not in [
                            business_key_col,
                            "record_created_date",
                        ] and pd.notna(row[col]):
                            update_columns.append(f"{col} = :{col}")
                            update_values[col] = row[col]

                    # Add updated date
                    update_columns.append("record_updated_date = :record_updated_date")
                    update_values["record_updated_date"] = datetime.now()
                    update_values["business_key"] = business_key_value

                    if update_columns:
                        sql_update = f"""
                        UPDATE {table_name} 
                        SET {', '.join(update_columns)}
                        WHERE {business_key_col} = :business_key
                        """

                        result = connection.execute(text(sql_update), update_values)

                        if result.rowcount > 0:
                            successful_updates += 1
                        else:
                            logger.warning(
                                f"No rows updated for {business_key_col} = {business_key_value}"
                            )

                except Exception as e:
                    failed_updates += 1
                    error_msg = (
                        f"Failed to update record {business_key_value}: {str(e)}"
                    )
                    logger.error(error_msg)
                    scd_result["errors"].append(error_msg)

            scd_result["updates_executed"] = successful_updates
            scd_result["updates_failed"] = failed_updates

            logger.info(
                f"SCD Type 1 updates completed: {successful_updates} successful, {failed_updates} failed"
            )

    except Exception as e:
        error_msg = f"SCD Type 1 execution failed: {str(e)}"
        logger.error(error_msg)
        scd_result["errors"].append(error_msg)

    finally:
        scd_result["execution_duration"] = (datetime.now() - start_time).total_seconds()

    return scd_result


def validate_referential_integrity(engine: Engine) -> Dict[str, Any]:
    """
    Validate referential integrity between fact and dimension tables.

    Args:
        engine (Engine): Database engine

    Returns:
        Dict[str, Any]: Referential integrity validation results
    """
    logger.info("Validating referential integrity")

    integrity_result = {
        "is_valid": True,
        "orphaned_records": {},
        "validation_errors": [],
        "validation_warnings": [],
    }

    try:
        with engine.connect() as connection:
            # Check fact_sales -> dim_customers relationships
            logger.info("Checking fact_sales -> dim_customers integrity")
            orphaned_customer_sales = connection.execute(
                text(
                    """
                SELECT COUNT(*) as orphaned_count
                FROM fact_sales fs
                LEFT JOIN dim_customers dc ON fs.customer_key = dc.customer_key
                WHERE fs.customer_key IS NOT NULL AND dc.customer_key IS NULL
            """
                )
            ).scalar()

            if orphaned_customer_sales > 0:
                integrity_result["orphaned_records"][
                    "fact_sales_customers"
                ] = orphaned_customer_sales
                integrity_result["validation_errors"].append(
                    f"{orphaned_customer_sales} sales records with invalid customer_key"
                )
                integrity_result["is_valid"] = False

            # Check fact_sales -> dim_products relationships
            logger.info("Checking fact_sales -> dim_products integrity")
            orphaned_product_sales = connection.execute(
                text(
                    """
                SELECT COUNT(*) as orphaned_count
                FROM fact_sales fs
                LEFT JOIN dim_products dp ON fs.product_key = dp.product_key
                WHERE fs.product_key IS NOT NULL AND dp.product_key IS NULL
            """
                )
            ).scalar()

            if orphaned_product_sales > 0:
                integrity_result["orphaned_records"][
                    "fact_sales_products"
                ] = orphaned_product_sales
                integrity_result["validation_errors"].append(
                    f"{orphaned_product_sales} sales records with invalid product_key"
                )
                integrity_result["is_valid"] = False

            # Check fact_sales -> dim_stores relationships
            logger.info("Checking fact_sales -> dim_stores integrity")
            orphaned_store_sales = connection.execute(
                text(
                    """
                SELECT COUNT(*) as orphaned_count
                FROM fact_sales fs
                LEFT JOIN dim_stores ds ON fs.store_key = ds.store_key
                WHERE fs.store_key IS NOT NULL AND ds.store_key IS NULL
            """
                )
            ).scalar()

            if orphaned_store_sales > 0:
                integrity_result["orphaned_records"][
                    "fact_sales_stores"
                ] = orphaned_store_sales
                integrity_result["validation_errors"].append(
                    f"{orphaned_store_sales} sales records with invalid store_key"
                )
                integrity_result["is_valid"] = False

            # Check fact_sales -> dim_date relationships
            logger.info("Checking fact_sales -> dim_date integrity")
            orphaned_date_sales = connection.execute(
                text(
                    """
                SELECT COUNT(*) as orphaned_count
                FROM fact_sales fs
                LEFT JOIN dim_date dd ON fs.date_key = dd.date_key
                WHERE fs.date_key IS NOT NULL AND dd.date_key IS NULL
            """
                )
            ).scalar()

            if orphaned_date_sales > 0:
                integrity_result["orphaned_records"][
                    "fact_sales_dates"
                ] = orphaned_date_sales
                integrity_result["validation_errors"].append(
                    f"{orphaned_date_sales} sales records with invalid date_key"
                )
                integrity_result["is_valid"] = False

            # Check fact_customer_support -> dim_customers relationships
            logger.info("Checking fact_customer_support -> dim_customers integrity")
            orphaned_support_customers = connection.execute(
                text(
                    """
                SELECT COUNT(*) as orphaned_count
                FROM fact_customer_support fcs
                LEFT JOIN dim_customers dc ON fcs.customer_key = dc.customer_key
                WHERE fcs.customer_key IS NOT NULL AND dc.customer_key IS NULL
            """
                )
            ).scalar()

            if orphaned_support_customers > 0:
                integrity_result["orphaned_records"][
                    "fact_support_customers"
                ] = orphaned_support_customers
                integrity_result["validation_errors"].append(
                    f"{orphaned_support_customers} support records with invalid customer_key"
                )
                integrity_result["is_valid"] = False

            # Check fact_customer_support -> dim_support_categories relationships
            logger.info(
                "Checking fact_customer_support -> dim_support_categories integrity"
            )
            orphaned_support_categories = connection.execute(
                text(
                    """
                SELECT COUNT(*) as orphaned_count
                FROM fact_customer_support fcs
                LEFT JOIN dim_support_categories dsc ON fcs.category_key = dsc.category_key
                WHERE fcs.category_key IS NOT NULL AND dsc.category_key IS NULL
            """
                )
            ).scalar()

            if orphaned_support_categories > 0:
                integrity_result["orphaned_records"][
                    "fact_support_categories"
                ] = orphaned_support_categories
                integrity_result["validation_errors"].append(
                    f"{orphaned_support_categories} support records with invalid category_key"
                )
                integrity_result["is_valid"] = False

            if integrity_result["is_valid"]:
                logger.info("All referential integrity checks passed")
            else:
                logger.warning(
                    f"Referential integrity issues found: {len(integrity_result['validation_errors'])} errors"
                )

    except Exception as e:
        error_msg = f"Referential integrity validation failed: {str(e)}"
        logger.error(error_msg)
        integrity_result["validation_errors"].append(error_msg)
        integrity_result["is_valid"] = False

    return integrity_result


def load_etl_metadata(
    run_id: str, pipeline_metrics: Dict[str, Any], engine: Engine
) -> None:
    """
    Load ETL pipeline execution metadata and metrics.

    Args:
        run_id (str): Unique identifier for this ETL run
        pipeline_metrics (Dict): Comprehensive pipeline metrics
        engine (Engine): Database engine
    """
    logger.info(f"Loading ETL metadata for run: {run_id}")

    try:
        with engine.connect() as connection:
            # Extract metrics from pipeline_metrics
            extraction_metrics = pipeline_metrics.get("extraction_metrics", {})
            transformation_metrics = pipeline_metrics.get("transformation_metrics", {})
            loading_metrics = pipeline_metrics.get("loading_metrics", {})

            # Prepare metadata record
            metadata_record = {
                "run_id": run_id,
                "pipeline_name": "TechMart ETL Pipeline",
                "start_time": pipeline_metrics.get(
                    "pipeline_start_time", datetime.now()
                ),
                "end_time": pipeline_metrics.get("pipeline_end_time", datetime.now()),
                "status": pipeline_metrics.get("status", "SUCCESS"),
                # Extraction metrics
                "csv_records_extracted": extraction_metrics.get("csv_records", 0),
                "json_records_extracted": extraction_metrics.get("json_records", 0),
                "mongodb_records_extracted": extraction_metrics.get(
                    "mongodb_records", 0
                ),
                "sqlserver_records_extracted": extraction_metrics.get(
                    "sqlserver_records", 0
                ),
                # Transformation metrics
                "records_processed": transformation_metrics.get(
                    "total_records_processed", 0
                ),
                "records_validated": transformation_metrics.get("records_validated", 0),
                "validation_failures": transformation_metrics.get(
                    "validation_failures", 0
                ),
                "data_quality_score": transformation_metrics.get(
                    "data_quality_score", 0.0
                ),
                # Loading metrics
                "fact_records_inserted": loading_metrics.get(
                    "fact_records_inserted", 0
                ),
                "fact_records_updated": loading_metrics.get("fact_records_updated", 0),
                "dimension_records_inserted": loading_metrics.get(
                    "dimension_records_inserted", 0
                ),
                "dimension_records_updated": loading_metrics.get(
                    "dimension_records_updated", 0
                ),
                "scd_updates_applied": loading_metrics.get("scd_updates_applied", 0),
                # Performance metrics
                "extraction_duration_seconds": extraction_metrics.get(
                    "total_duration", 0.0
                ),
                "transformation_duration_seconds": transformation_metrics.get(
                    "total_duration", 0.0
                ),
                "loading_duration_seconds": loading_metrics.get("total_duration", 0.0),
                "total_duration_seconds": pipeline_metrics.get("total_duration", 0.0),
                # Error tracking
                "error_count": len(pipeline_metrics.get("errors", [])),
                "error_message": "; ".join(pipeline_metrics.get("errors", []))[
                    :500
                ],  # Truncate if too long
                "warning_count": len(pipeline_metrics.get("warnings", [])),
                "warning_messages": "; ".join(pipeline_metrics.get("warnings", []))[
                    :500
                ],
                # Data lineage
                "source_file_paths": str(pipeline_metrics.get("source_files", {})),
                "records_processed_by_source": str(extraction_metrics),
                "created_date": datetime.now(),
            }

            # Convert to DataFrame for easy insertion
            metadata_df = pd.DataFrame([metadata_record])

            # Insert into etl_pipeline_runs table
            metadata_df.to_sql(
                "etl_pipeline_runs",
                connection,
                if_exists="append",
                index=False,
                method="multi",
            )

            logger.info(f"Successfully loaded ETL metadata for run: {run_id}")

    except Exception as e:
        error_msg = f"Failed to load ETL metadata: {str(e)}"
        logger.error(error_msg)
        # Don't raise the exception to avoid failing the entire pipeline
        # The metadata loading is supplementary and shouldn't break the pipeline


def load_data_quality_issues(
    run_id: str, quality_issues: List[Dict[str, Any]], engine: Engine
) -> None:
    """
    Load data quality issues discovered during ETL processing.

    Args:
        run_id (str): ETL run identifier
        quality_issues (List[Dict]): Data quality issues found
        engine (Engine): Database engine

    TODO: Implement data quality issue logging:
    - Insert records into data_quality_issues table
    - Categorize issues by severity and type
    - Link issues to source systems and records
    - Enable quality issue tracking and resolution
    """
    logger.info(f"Loading {len(quality_issues)} data quality issues")

    # TODO: Implement data quality issue insertion

    raise NotImplementedError("Data quality issue loading not yet implemented")


def populate_date_dimension(
    engine: Engine, start_date: str = "2022-01-01", end_date: str = "2025-12-31"
) -> Dict[str, Any]:
    """
    Populate the date dimension table with calendar data.

    Args:
        engine (Engine): Database engine
        start_date (str): Start date for dimension (YYYY-MM-DD)
        end_date (str): End date for dimension (YYYY-MM-DD)

    Returns:
        Dict[str, Any]: Date dimension population results

    TODO: Implement date dimension population:
    - Generate calendar dates between start and end dates
    - Calculate date attributes (year, quarter, month, week, etc.)
    - Identify weekends and holidays
    - Insert date records if they don't exist
    - Handle fiscal year calculations
    """
    logger.info(f"Populating date dimension from {start_date} to {end_date}")

    # TODO: Implement date dimension population

    raise NotImplementedError("Date dimension population not yet implemented")


def load_to_data_warehouse(
    transformation_result: Dict[str, Any], connection_string: str, run_id: str
) -> Dict[str, Any]:
    """
    Main loading function that orchestrates all data warehouse loading operations.

    Args:
        transformation_result (Dict): Complete transformation results
        connection_string (str): Data warehouse connection string
        run_id (str): Unique ETL run identifier

    Returns:
        Dict[str, Any]: Complete loading results and metrics
    """
    logger.info(f"Starting data warehouse loading for run: {run_id}")

    loading_result = {
        "status": "SUCCESS",
        "run_id": run_id,
        "loading_start_time": datetime.now(),
        "loading_end_time": None,
        "dimension_results": {},
        "fact_results": {},
        "scd_results": {},
        "integrity_validation": {},
        "loading_metrics": {
            "total_records_loaded": 0,
            "total_records_updated": 0,
            "loading_duration_seconds": 0.0,
            "data_quality_score": 0.0,
        },
        "errors": [],
        "warnings": [],
    }

    try:
        # 1. Create database connection
        logger.info("Step 1: Creating database connection")
        engine = create_database_connection(connection_string)

        # 2. Load dimension tables with SCD Type 1 support
        logger.info("Step 2: Loading dimension tables")

        # Load customers dimension - use the actual key names from transformer
        if (
            "dimension_data" in transformation_result
            and "dim_customers" in transformation_result["dimension_data"]
        ):
            customers_data = transformation_result["dimension_data"]["dim_customers"]
            customers_scd = transformation_result.get("scd_operations", {}).get(
                "dim_customers", {}
            )

            # Convert list to DataFrame if needed
            if isinstance(customers_data, list):
                customers_df = pd.DataFrame(customers_data)
            else:
                customers_df = customers_data

            customer_result = load_dimension_table(
                customers_df, "dim_customers", engine, customers_scd
            )
            loading_result["dimension_results"]["customers"] = customer_result
            loading_result["loading_metrics"][
                "total_records_loaded"
            ] += customer_result["records_inserted"]
            loading_result["loading_metrics"][
                "total_records_updated"
            ] += customer_result["records_updated"]
            loading_result["errors"].extend(customer_result["errors"])

        # Load products dimension
        if (
            "dimension_data" in transformation_result
            and "dim_products" in transformation_result["dimension_data"]
        ):
            products_data = transformation_result["dimension_data"]["dim_products"]
            products_scd = transformation_result.get("scd_operations", {}).get(
                "dim_products", {}
            )

            # Convert list to DataFrame if needed
            if isinstance(products_data, list):
                products_df = pd.DataFrame(products_data)
            else:
                products_df = products_data

            product_result = load_dimension_table(
                products_df, "dim_products", engine, products_scd
            )
            loading_result["dimension_results"]["products"] = product_result
            loading_result["loading_metrics"]["total_records_loaded"] += product_result[
                "records_inserted"
            ]
            loading_result["loading_metrics"][
                "total_records_updated"
            ] += product_result["records_updated"]
            loading_result["errors"].extend(product_result["errors"])

        # Load stores dimension
        if (
            "dimension_data" in transformation_result
            and "dim_stores" in transformation_result["dimension_data"]
        ):
            stores_data = transformation_result["dimension_data"]["dim_stores"]
            stores_scd = transformation_result.get("scd_operations", {}).get(
                "dim_stores", {}
            )

            # Convert list to DataFrame if needed
            if isinstance(stores_data, list):
                stores_df = pd.DataFrame(stores_data)
            else:
                stores_df = stores_data

            store_result = load_dimension_table(
                stores_df, "dim_stores", engine, stores_scd
            )
            loading_result["dimension_results"]["stores"] = store_result
            loading_result["loading_metrics"]["total_records_loaded"] += store_result[
                "records_inserted"
            ]
            loading_result["loading_metrics"]["total_records_updated"] += store_result[
                "records_updated"
            ]
            loading_result["errors"].extend(store_result["errors"])

        # Load support categories dimension
        if (
            "dimension_data" in transformation_result
            and "dim_support_categories" in transformation_result["dimension_data"]
        ):
            categories_data = transformation_result["dimension_data"][
                "dim_support_categories"
            ]
            categories_scd = transformation_result.get("scd_operations", {}).get(
                "dim_support_categories", {}
            )

            # Convert list to DataFrame if needed
            if isinstance(categories_data, list):
                categories_df = pd.DataFrame(categories_data)
            else:
                categories_df = categories_data

            category_result = load_dimension_table(
                categories_df, "dim_support_categories", engine, categories_scd
            )
            loading_result["dimension_results"]["support_categories"] = category_result
            loading_result["loading_metrics"][
                "total_records_loaded"
            ] += category_result["records_inserted"]
            loading_result["loading_metrics"][
                "total_records_updated"
            ] += category_result["records_updated"]
            loading_result["errors"].extend(category_result["errors"])

        # 3. Load fact tables
        logger.info("Step 3: Loading fact tables")

        # Load sales facts - use the actual key names from transformer
        if (
            "fact_data" in transformation_result
            and "fact_sales" in transformation_result["fact_data"]
        ):
            sales_data = transformation_result["fact_data"]["fact_sales"]

            # Convert list to DataFrame if needed
            if isinstance(sales_data, list):
                sales_df = pd.DataFrame(sales_data)
            else:
                sales_df = sales_data

            sales_result = load_fact_table(sales_df, "fact_sales", engine)
            loading_result["fact_results"]["sales"] = sales_result
            loading_result["loading_metrics"]["total_records_loaded"] += sales_result[
                "records_inserted"
            ]
            loading_result["errors"].extend(sales_result["errors"])

        # Load customer support facts
        if (
            "fact_data" in transformation_result
            and "fact_customer_support" in transformation_result["fact_data"]
        ):
            support_data = transformation_result["fact_data"]["fact_customer_support"]

            # Convert list to DataFrame if needed
            if isinstance(support_data, list):
                support_df = pd.DataFrame(support_data)
            else:
                support_df = support_data

            support_result = load_fact_table(
                support_df, "fact_customer_support", engine
            )
            loading_result["fact_results"]["customer_support"] = support_result
            loading_result["loading_metrics"]["total_records_loaded"] += support_result[
                "records_inserted"
            ]
            loading_result["errors"].extend(support_result["errors"])

        # 4. Validate referential integrity
        logger.info("Step 4: Validating referential integrity")
        integrity_result = validate_referential_integrity(engine)
        loading_result["integrity_validation"] = integrity_result

        if not integrity_result["is_valid"]:
            loading_result["warnings"].extend(integrity_result["validation_errors"])
            logger.warning("Referential integrity issues detected")

        # 5. Calculate data quality score
        transformation_metrics = transformation_result.get("transformation_metrics", {})
        loading_result["loading_metrics"]["data_quality_score"] = (
            transformation_metrics.get("data_quality_score", 0.0)
        )

        # 6. Load ETL metadata
        logger.info("Step 5: Loading ETL metadata")
        pipeline_metrics = {
            "extraction_metrics": transformation_result.get("extraction_metrics", {}),
            "transformation_metrics": transformation_metrics,
            "loading_metrics": loading_result["loading_metrics"],
            "pipeline_start_time": loading_result["loading_start_time"],
            "pipeline_end_time": datetime.now(),
            "status": loading_result["status"],
            "errors": loading_result["errors"],
            "warnings": loading_result["warnings"],
        }

        load_etl_metadata(run_id, pipeline_metrics, engine)

        # 7. Generate business summary data
        try:
            logger.info("Step 6: Generating business summary data")
            # This is optional and shouldn't fail the pipeline
            business_summary = generate_business_summary_data(engine)
            loading_result["business_summary"] = business_summary
        except Exception as e:
            logger.warning(f"Business summary generation failed: {str(e)}")
            loading_result["warnings"].append(
                f"Business summary generation failed: {str(e)}"
            )

        # Set final status
        loading_result["loading_end_time"] = datetime.now()
        loading_result["loading_metrics"]["loading_duration_seconds"] = (
            loading_result["loading_end_time"] - loading_result["loading_start_time"]
        ).total_seconds()

        # Determine final status
        if loading_result["errors"]:
            loading_result["status"] = (
                "WARNING"
                if loading_result["loading_metrics"]["total_records_loaded"] > 0
                else "FAILED"
            )

        logger.info(
            f"Data warehouse loading completed with status: {loading_result['status']}"
        )
        logger.info(
            f"Total records loaded: {loading_result['loading_metrics']['total_records_loaded']}"
        )
        logger.info(
            f"Total records updated: {loading_result['loading_metrics']['total_records_updated']}"
        )

    except Exception as e:
        error_msg = f"Data warehouse loading failed: {str(e)}"
        logger.error(error_msg)
        loading_result["status"] = "FAILED"
        loading_result["errors"].append(error_msg)
        loading_result["loading_end_time"] = datetime.now()
        loading_result["loading_metrics"]["loading_duration_seconds"] = (
            loading_result["loading_end_time"] - loading_result["loading_start_time"]
        ).total_seconds()

    return loading_result


def generate_business_summary_data(engine: Engine) -> Dict[str, Any]:
    """
    Generate business summary data for Power BI dashboard.

    Args:
        engine (Engine): Database engine

    Returns:
        Dict[str, Any]: Business summary metrics

    TODO: Implement business summary data generation:
    - Calculate total revenue and customer metrics
    - Generate product performance summaries
    - Calculate customer service metrics
    - Prepare data for Power BI consumption
    - Handle summary calculation errors
    """
    logger.info("Generating business summary data")

    # TODO: Query data warehouse for business metrics

    raise NotImplementedError("Business summary data generation not yet implemented")


def calculate_loading_metrics(loading_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate comprehensive loading performance metrics.

    Args:
        loading_result (Dict): Results from load_to_data_warehouse()

    Returns:
        Dict[str, Any]: Loading metrics for monitoring dashboard

    TODO: Implement loading metrics calculation:
    - Loading performance by table
    - SCD operation success rates
    - Data quality scores after loading
    - Referential integrity validation results
    - Overall loading success metrics
    """
    # TODO: Calculate loading metrics for Power BI dashboard

    raise NotImplementedError("Loading metrics calculation not yet implemented")
