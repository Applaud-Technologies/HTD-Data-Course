"""
Dimension Loading Module for Insurance ETL Pipeline

This module handles loading of dimension tables (customer, policy, agent, date) into
the insurance star schema data warehouse with proper surrogate key management,
SCD Type 1 updates, and referential integrity.

Key Components:
- InsuranceDimensionLoader: Main class for dimension loading operations
- Customer dimension loading with age calculation and risk classification
- Policy dimension loading with premium tier classification
- Agent dimension loading with region assignment
- Date dimension validation and gap filling
- Surrogate key management for fact table references

Dimension Loading Features:
- SCD Type 1 updates for changing dimension attributes
- Surrogate key generation and lookup functionality
- Data validation before loading
- Error handling and rollback capabilities
- Performance optimized batch loading

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM (~175 lines of code, 2 TODO methods for students)
"""

import pyodbc
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from config import InsuranceETLConfig


class InsuranceDimensionLoader:
    """
    Dimension loading class for Insurance ETL pipeline

    Handles loading of all dimension tables with proper surrogate key management
    and SCD Type 1 update logic for the insurance star schema.
    """

    def __init__(self, config: InsuranceETLConfig):
        """
        Initialize dimension loader with configuration

        Args:
            config: Insurance ETL configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.cursor = None

    def load_all_dimensions(
        self, transformed_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Dict[str, int]]:
        """
        Load all dimension tables and return surrogate key mappings

        Args:
            transformed_data: Dictionary containing transformed dimension data

        Returns:
            Dictionary containing business key to surrogate key mappings for each dimension
            {
                'dim_customer': {'CUST001': 1, 'CUST002': 2, ...},
                'dim_policy': {'POL0001': 1, 'POL0002': 2, ...},
                'dim_agent': {'AGT001': 1, 'AGT002': 2, ...}
            }

        Raises:
            Exception: If dimension loading fails
        """
        self.logger.info("Starting dimension loading for insurance star schema")

        try:
            # Connect to database
            self._connect_to_database()

            surrogate_key_mappings = {}

            # Load customer dimension
            customers = transformed_data.get("dim_customer", [])
            surrogate_key_mappings["dim_customer"] = self.load_customer_dimension(
                customers
            )

            # Load policy dimension
            policies = transformed_data.get("dim_policy", [])
            surrogate_key_mappings["dim_policy"] = self.load_policy_dimension(policies)

            # Load agent dimension
            agents = transformed_data.get("dim_agent", [])
            surrogate_key_mappings["dim_agent"] = self.load_agent_dimension(agents)

            # Validate date dimension
            date_keys = transformed_data.get("date_keys", [])
            self.validate_date_dimension(date_keys)

            # Log loading summary
            self.logger.info("Dimension Loading Summary:")
            for dim_name, mappings in surrogate_key_mappings.items():
                self.logger.info(f"  - {dim_name}: {len(mappings)} records loaded")

            return surrogate_key_mappings

        except Exception as e:
            self.logger.error(f"Failed to load dimensions: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            self._cleanup_connection()

    def load_customer_dimension(
        self, customers: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Load customer dimension with SCD Type 1 updates

        Args:
            customers: List of transformed customer records

        Returns:
            Dictionary mapping customer_id to customer_key (surrogate key)

        Business Rules:
        - Use customer_id as business key
        - Apply SCD Type 1 (overwrite) for changing attributes
        - Generate surrogate keys for new customers
        - Update existing customers with new attribute values
        """
        self.logger.info(f"Loading customer dimension with {len(customers)} records")

        surrogate_keys = {}

        try:
            # Process customers in batches for performance
            batch_size = self.config.batch_size
            for i in range(0, len(customers), batch_size):
                batch = customers[i : i + batch_size]
                batch_keys = self._process_customer_batch(batch)
                surrogate_keys.update(batch_keys)

            self.logger.info(f"Successfully loaded {len(surrogate_keys)} customers")
            return surrogate_keys

        except Exception as e:
            self.logger.error(f"Failed to load customer dimension: {str(e)}")
            raise

    def load_policy_dimension(self, policies: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Load policy dimension with SCD Type 1 updates

        Args:
            policies: List of transformed policy records

        Returns:
            Dictionary mapping policy_id to policy_key (surrogate key)
        """
        self.logger.info(f"Loading policy dimension with {len(policies)} records")

        surrogate_keys = {}

        try:
            # Process policies in batches
            batch_size = self.config.batch_size
            for i in range(0, len(policies), batch_size):
                batch = policies[i : i + batch_size]
                batch_keys = self._process_policy_batch(batch)
                surrogate_keys.update(batch_keys)

            self.logger.info(f"Successfully loaded {len(surrogate_keys)} policies")
            return surrogate_keys

        except Exception as e:
            self.logger.error(f"Failed to load policy dimension: {str(e)}")
            raise

    def load_agent_dimension(self, agents: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Load agent dimension with SCD Type 1 updates

        Args:
            agents: List of transformed agent records

        Returns:
            Dictionary mapping agent_id to agent_key (surrogate key)
        """
        self.logger.info(f"Loading agent dimension with {len(agents)} records")

        surrogate_keys = {}

        try:
            # Process agents in batches
            batch_size = self.config.batch_size
            for i in range(0, len(agents), batch_size):
                batch = agents[i : i + batch_size]
                batch_keys = self._process_agent_batch(batch)
                surrogate_keys.update(batch_keys)

            self.logger.info(f"Successfully loaded {len(surrogate_keys)} agents")
            return surrogate_keys

        except Exception as e:
            self.logger.error(f"Failed to load agent dimension: {str(e)}")
            raise

    # =====================================================================
    # STUDENT TODO METHODS - IMPLEMENT THESE!
    # =====================================================================

    def _process_customer_batch(
        self, customers: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        TODO: Process a batch of customer records for dimension loading

        BUSINESS LOGIC:
        1. Check if customer_id already exists in dim_customer table
        2. If exists: UPDATE the record with new values (SCD Type 1)
        3. If new: INSERT new record and get surrogate key
        4. Return mapping of customer_id -> customer_key

        IMPLEMENTATION STEPS:
        1. Initialize empty surrogate_keys dictionary
        2. For each customer in the batch:
           a. Execute SELECT query to check if customer_id exists
           b. If exists: Execute UPDATE query with new values
           c. If new: Execute INSERT query and get generated surrogate key
           d. Add customer_id -> customer_key mapping to dictionary
        3. Return the surrogate_keys dictionary

        SQL HINTS:
        - Check existing: "SELECT customer_key FROM dim_customer WHERE customer_id = ?"
        - Update: "UPDATE dim_customer SET first_name = ?, ... WHERE customer_id = ?"
        - Insert: "INSERT INTO dim_customer (...) VALUES (...)"
        - Get surrogate key: "SELECT @@IDENTITY" (after INSERT)

        EXCEPTION HANDLING:
        - Wrap individual customer processing in try/except
        - Log warnings for failed customers but continue processing
        - Use self.logger.warning() for individual failures

        Args:
            customers: List of customer dictionaries to process

        Returns:
            Dict[str, int]: Mapping of customer_id to customer_key (surrogate key)
        """
        # TODO: Implement customer batch processing logic here
        # Remove this pass statement and add your implementation
        pass

    def _process_policy_batch(self, policies: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        TODO: Process a batch of policy records for dimension loading

        BUSINESS LOGIC:
        - Similar to customer processing but for policy dimension
        - Check existing policy_id, update if exists, insert if new
        - Return mapping of policy_id -> policy_key

        IMPLEMENTATION STEPS:
        1. Initialize empty surrogate_keys dictionary
        2. For each policy in the batch:
           a. Execute SELECT query to check if policy_id exists
           b. If exists: Execute UPDATE query with new values
           c. If new: Execute INSERT query and get generated surrogate key
           d. Add policy_id -> policy_key mapping to dictionary
        3. Return the surrogate_keys dictionary

        SQL HINTS:
        - Check existing: "SELECT policy_key FROM dim_policy WHERE policy_id = ?"
        - Update: "UPDATE dim_policy SET policy_type = ?, ... WHERE policy_id = ?"
        - Insert: "INSERT INTO dim_policy (...) VALUES (...)"
        - Get surrogate key: "SELECT @@IDENTITY" (after INSERT)

        POLICY-SPECIFIC FIELDS:
        - policy_type, coverage_amount, annual_premium, premium_tier
        - deductible, effective_date, expiration_date, status

        Args:
            policies: List of policy dictionaries to process

        Returns:
            Dict[str, int]: Mapping of policy_id to policy_key (surrogate key)
        """
        # TODO: Implement policy batch processing logic here
        # Remove this pass statement and add your implementation
        pass

    # =====================================================================
    # COMPLETED METHODS - ALREADY IMPLEMENTED (DO NOT MODIFY)
    # =====================================================================

    def _process_agent_batch(self, agents: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process a batch of agent records for dimension loading"""
        surrogate_keys = {}

        for agent in agents:
            try:
                # Check if agent exists
                check_sql = "SELECT agent_key FROM dim_agent WHERE agent_id = ?"
                self.cursor.execute(check_sql, (agent["agent_id"],))
                existing = self.cursor.fetchone()

                if existing:
                    # Update existing agent (SCD Type 1)
                    update_sql = """
                    UPDATE dim_agent 
                    SET first_name = ?, last_name = ?, full_name = ?, region = ?,
                        experience_years = ?, hire_date = ?
                    WHERE agent_id = ?
                    """
                    self.cursor.execute(
                        update_sql,
                        (
                            agent["first_name"],
                            agent["last_name"],
                            agent["full_name"],
                            agent["region"],
                            agent["experience_years"],
                            agent["hire_date"],
                            agent["agent_id"],
                        ),
                    )
                    agent_key = existing[0]
                else:
                    # Insert new agent
                    insert_sql = """
                    INSERT INTO dim_agent 
                    (agent_id, first_name, last_name, full_name, region, 
                     experience_years, hire_date, created_date, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE(), 1)
                    """
                    self.cursor.execute(
                        insert_sql,
                        (
                            agent["agent_id"],
                            agent["first_name"],
                            agent["last_name"],
                            agent["full_name"],
                            agent["region"],
                            agent["experience_years"],
                            agent["hire_date"],
                        ),
                    )

                    # Get the generated surrogate key
                    self.cursor.execute("SELECT @@IDENTITY")
                    agent_key = self.cursor.fetchone()[0]

                surrogate_keys[agent["agent_id"]] = agent_key

            except Exception as e:
                self.logger.warning(
                    f"Error processing agent {agent.get('agent_id')}: {e}"
                )
                continue

        return surrogate_keys

    def validate_date_dimension(self, date_keys: List[int]) -> bool:
        """
        Validate that required date keys exist in date dimension

        Args:
            date_keys: List of date keys needed for fact loading

        Returns:
            bool: True if all date keys exist, False otherwise
        """
        if not date_keys:
            self.logger.info("No date keys to validate")
            return True

        self.logger.info(f"Validating {len(date_keys)} date keys in date dimension")

        try:
            # Check which date keys exist
            date_keys_str = ",".join(map(str, date_keys))
            check_sql = f"""
            SELECT date_key 
            FROM dim_date 
            WHERE date_key IN ({date_keys_str})
            """

            self.cursor.execute(check_sql)
            existing_keys = {row[0] for row in self.cursor.fetchall()}

            missing_keys = set(date_keys) - existing_keys

            if missing_keys:
                self.logger.warning(
                    f"Missing date keys in dimension: {sorted(list(missing_keys))}"
                )
                return False

            self.logger.info("All date keys validated successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to validate date dimension: {str(e)}")
            return False

    def _connect_to_database(self):
        """Establish database connection"""
        try:
            connection_string = self.config.get_connection_string()
            self.connection = pyodbc.connect(connection_string)
            self.connection.autocommit = False  # Use transactions
            self.cursor = self.connection.cursor()
            self.logger.info(f"Connected to database: {self.config.db_name}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def _cleanup_connection(self):
        """Clean up database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.commit()  # Commit successful operations
            self.connection.close()


def load_insurance_dimensions(
    config: InsuranceETLConfig, transformed_data: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, Dict[str, int]]:
    """
    Convenience function to load all insurance dimensions

    Args:
        config: Insurance ETL configuration
        transformed_data: Transformed dimension data

    Returns:
        Dictionary containing surrogate key mappings for all dimensions
    """
    loader = InsuranceDimensionLoader(config)
    return loader.load_all_dimensions(transformed_data)


if __name__ == "__main__":
    # Test dimension loading functionality
    from config import load_config

    try:
        print("Testing Insurance Dimension Loading")
        print("=" * 40)

        config = load_config()
        loader = InsuranceDimensionLoader(config)

        # Test with sample data
        sample_data = {
            "dim_customer": [
                {
                    "customer_id": "CUST001",
                    "first_name": "John",
                    "last_name": "Doe",
                    "full_name": "John Doe",
                    "email": "john.doe@example.com",
                    "phone": "(555) 123-4567",
                    "birth_date": "1985-03-15",
                    "age": 39,
                    "address": "123 Main St",
                    "city": "Anytown",
                    "state": "CA",
                    "risk_score": 2.1,
                    "risk_tier": "Medium",
                    "customer_since": "2015-01-01",
                }
            ],
            "dim_policy": [],
            "dim_agent": [],
            "date_keys": [],
        }

        print("Testing dimension loading with sample data...")
        surrogate_keys = loader.load_all_dimensions(sample_data)

        print("Dimension Loading Results:")
        for dim_name, mappings in surrogate_keys.items():
            print(f"  - {dim_name}: {len(mappings)} records")

    except Exception as e:
        print(f"Dimension loading test failed: {e}")
        print("Ensure database schema is created and accessible!")
