"""
Fact Loading Module for Insurance ETL Pipeline

This module handles loading of fact tables (claims) into the insurance star schema
data warehouse with proper surrogate key lookups, referential integrity validation,
and fact table optimization.

Key Components:
- InsuranceFactLoader: Main class for fact loading operations
- Claims fact loading with surrogate key resolution
- Foreign key validation and referential integrity
- Fact table optimization and indexing
- Error handling and data quality validation

Fact Loading Features:
- Surrogate key lookups for dimension references
- Business rule validation before loading
- Batch processing for performance optimization
- Duplicate detection and handling
- Referential integrity enforcement

Target Audience: Beginner/Intermediate students
Complexity Level: MEDIUM (~150 lines of code, 1 TODO method for students)
"""

import pyodbc
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, date
from config import InsuranceETLConfig


class InsuranceFactLoader:
    """
    Fact loading class for Insurance ETL pipeline

    Handles loading of fact tables with proper surrogate key resolution
    and referential integrity validation for the insurance star schema.
    """

    def __init__(self, config: InsuranceETLConfig):
        """
        Initialize fact loader with configuration

        Args:
            config: Insurance ETL configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.cursor = None

    def load_all_facts(
        self,
        facts_data: List[Dict[str, Any]],
        surrogate_key_mappings: Dict[str, Dict[str, int]],
    ) -> Dict[str, int]:
        """
        Load all fact tables with surrogate key resolution

        Args:
            facts_data: List of transformed fact records
            surrogate_key_mappings: Dictionary containing surrogate key mappings from dimensions

        Returns:
            Dictionary containing loading statistics:
            {
                'total_processed': 650,
                'successfully_loaded': 645,
                'failed_validation': 3,
                'duplicate_claims': 2
            }

        Raises:
            Exception: If fact loading fails
        """
        self.logger.info(f"Starting fact loading with {len(facts_data)} claims")

        try:
            # Connect to database
            self._connect_to_database()

            # Load claims facts
            loading_stats = self.load_claims_facts(facts_data, surrogate_key_mappings)

            # Log loading summary
            self.logger.info("Fact Loading Summary:")
            self.logger.info(f"  - Total processed: {loading_stats['total_processed']}")
            self.logger.info(
                f"  - Successfully loaded: {loading_stats['successfully_loaded']}"
            )
            self.logger.info(
                f"  - Failed validation: {loading_stats['failed_validation']}"
            )
            self.logger.info(
                f"  - Duplicates found: {loading_stats['duplicate_claims']}"
            )

            return loading_stats

        except Exception as e:
            self.logger.error(f"Failed to load facts: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            self._cleanup_connection()

    def load_claims_facts(
        self,
        claims: List[Dict[str, Any]],
        surrogate_key_mappings: Dict[str, Dict[str, int]],
    ) -> Dict[str, int]:
        """
        Load claims fact table with surrogate key resolution

        Args:
            claims: List of transformed claims fact records
            surrogate_key_mappings: Surrogate key mappings from dimension loading

        Returns:
            Dictionary containing loading statistics

        Business Rules:
        - Resolve all foreign keys to surrogate keys before loading
        - Validate referential integrity
        - Skip records with missing dimension references
        - Handle duplicate claim_id records
        - Maintain fact table performance
        """
        self.logger.info(f"Loading claims facts with {len(claims)} records")

        stats = {
            "total_processed": 0,
            "successfully_loaded": 0,
            "failed_validation": 0,
            "duplicate_claims": 0,
        }

        try:
            # Get existing claim IDs to detect duplicates
            existing_claims = self._get_existing_claim_ids()

            # Process claims in batches for performance
            batch_size = self.config.batch_size
            for i in range(0, len(claims), batch_size):
                batch = claims[i : i + batch_size]
                batch_stats = self._process_claims_batch(
                    batch, surrogate_key_mappings, existing_claims
                )

                # Update overall statistics
                for key, value in batch_stats.items():
                    stats[key] += value

            self.logger.info(
                f"Successfully processed {stats['total_processed']} claims"
            )
            return stats

        except Exception as e:
            self.logger.error(f"Failed to load claims facts: {str(e)}")
            raise

    def _process_claims_batch(
        self,
        claims: List[Dict[str, Any]],
        surrogate_key_mappings: Dict[str, Dict[str, int]],
        existing_claims: Set[str],
    ) -> Dict[str, int]:
        """
        Process a batch of claims records for fact loading

        Args:
            claims: Batch of claims records
            surrogate_key_mappings: Surrogate key mappings
            existing_claims: Set of existing claim IDs

        Returns:
            Dictionary containing batch statistics
        """
        batch_stats = {
            "total_processed": 0,
            "successfully_loaded": 0,
            "failed_validation": 0,
            "duplicate_claims": 0,
        }

        for claim in claims:
            batch_stats["total_processed"] += 1

            try:
                # Check for duplicate claim
                if claim["claim_id"] in existing_claims:
                    batch_stats["duplicate_claims"] += 1
                    self.logger.warning(f"Duplicate claim found: {claim['claim_id']}")
                    continue

                # Resolve surrogate keys
                resolved_claim = self._resolve_surrogate_keys(
                    claim, surrogate_key_mappings
                )

                if not resolved_claim:
                    batch_stats["failed_validation"] += 1
                    continue

                # Load the fact record
                success = self._load_single_claim_fact(resolved_claim)

                if success:
                    batch_stats["successfully_loaded"] += 1
                    existing_claims.add(claim["claim_id"])  # Track for duplicates
                else:
                    batch_stats["failed_validation"] += 1

            except Exception as e:
                batch_stats["failed_validation"] += 1
                self.logger.warning(
                    f"Error processing claim {claim.get('claim_id')}: {e}"
                )
                continue

        return batch_stats

    def _resolve_surrogate_keys(
        self, claim: Dict[str, Any], surrogate_key_mappings: Dict[str, Dict[str, int]]
    ) -> Optional[Dict[str, Any]]:
        """
        Resolve business keys to surrogate keys for fact loading

        Args:
            claim: Original claim record with business keys
            surrogate_key_mappings: Mappings from dimension loading

        Returns:
            Claim record with surrogate keys, or None if validation fails

        Business Rules:
        - All required foreign keys must resolve to valid surrogate keys
        - Missing dimension references should cause record rejection
        - Log warnings for missing references but continue processing
        """
        try:
            # Resolve customer surrogate key
            customer_key = surrogate_key_mappings.get("dim_customer", {}).get(
                claim["customer_id"]
            )
            if not customer_key:
                self.logger.warning(
                    f"Missing customer reference for claim {claim['claim_id']}: {claim['customer_id']}"
                )
                return None

            # Resolve policy surrogate key
            policy_key = surrogate_key_mappings.get("dim_policy", {}).get(
                claim["policy_id"]
            )
            if not policy_key:
                self.logger.warning(
                    f"Missing policy reference for claim {claim['claim_id']}: {claim['policy_id']}"
                )
                return None

            # Resolve agent surrogate key
            agent_key = surrogate_key_mappings.get("dim_agent", {}).get(
                claim["agent_id"]
            )
            if not agent_key:
                self.logger.warning(
                    f"Missing agent reference for claim {claim['claim_id']}: {claim['agent_id']}"
                )
                return None

            # Validate date keys exist (they should be in dim_date already)
            filed_date_key = claim["filed_date_key"]
            closed_date_key = claim["closed_date_key"]

            if not filed_date_key or filed_date_key == 0:
                self.logger.warning(
                    f"Invalid filed_date_key for claim {claim['claim_id']}"
                )
                return None

            # Create resolved claim record
            resolved_claim = {
                "claim_id": claim["claim_id"],
                "customer_key": customer_key,
                "policy_key": policy_key,
                "agent_key": agent_key,
                "filed_date_key": filed_date_key,
                "closed_date_key": (
                    closed_date_key if closed_date_key and closed_date_key > 0 else None
                ),
                "claim_amount": claim["claim_amount"],
                "coverage_amount": claim["coverage_amount"],
                "deductible_amount": claim["deductible_amount"],
                "payout_amount": claim["payout_amount"],
                "processing_days": claim["processing_days"],
                "claim_status": claim["claim_status"],
            }

            return resolved_claim

        except Exception as e:
            self.logger.warning(
                f"Error resolving surrogate keys for claim {claim.get('claim_id')}: {e}"
            )
            return None

    # =====================================================================
    # STUDENT TODO METHOD - IMPLEMENT THIS!
    # =====================================================================

    def _load_single_claim_fact(self, resolved_claim: Dict[str, Any]) -> bool:
        """
        TODO: Load a single resolved claim record into the fact table

        BUSINESS LOGIC:
        1. Insert the resolved claim into fact_claims table
        2. Use parameterized query to prevent SQL injection
        3. Handle any database constraints or errors
        4. Return True if successful, False if failed

        IMPLEMENTATION STEPS:
        1. Create SQL INSERT statement for fact_claims table
        2. Use parameterized query with ? placeholders
        3. Execute the query with resolved_claim values
        4. Return True if successful, False if exception occurs

        FACT TABLE STRUCTURE:
        The fact_claims table has these columns:
        - claim_id (business key)
        - customer_key (foreign key to dim_customer)
        - policy_key (foreign key to dim_policy)
        - agent_key (foreign key to dim_agent)
        - filed_date_key (foreign key to dim_date)
        - closed_date_key (foreign key to dim_date, can be NULL)
        - claim_amount (decimal)
        - coverage_amount (decimal)
        - deductible_amount (decimal)
        - payout_amount (decimal)
        - processing_days (integer)
        - claim_status (varchar)
        - created_date (datetime, auto-generated)

        SQL TEMPLATE:
        INSERT INTO fact_claims
        (claim_id, customer_key, policy_key, agent_key, filed_date_key,
         closed_date_key, claim_amount, coverage_amount, deductible_amount,
         payout_amount, processing_days, claim_status, created_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())

        EXCEPTION HANDLING:
        - Use try/except block to catch database errors
        - Log warnings for failed inserts using self.logger.warning()
        - Return False if any exception occurs

        Args:
            resolved_claim: Dictionary with all surrogate keys resolved

        Returns:
            bool: True if record loaded successfully, False if failed
        """
        # TODO: Implement fact record loading logic here
        # Remove this pass statement and add your implementation
        pass

    # =====================================================================
    # COMPLETED METHODS - ALREADY IMPLEMENTED (DO NOT MODIFY)
    # =====================================================================

    def _get_existing_claim_ids(self) -> Set[str]:
        """
        Get set of existing claim IDs to detect duplicates

        Returns:
            Set of existing claim_id values in fact table
        """
        try:
            self.cursor.execute("SELECT claim_id FROM fact_claims")
            existing_claims = {row[0] for row in self.cursor.fetchall()}
            self.logger.info(
                f"Found {len(existing_claims)} existing claims in fact table"
            )
            return existing_claims

        except Exception as e:
            self.logger.warning(f"Error getting existing claim IDs: {e}")
            return set()

    def validate_fact_referential_integrity(
        self, surrogate_key_mappings: Dict[str, Dict[str, int]]
    ) -> bool:
        """
        Validate referential integrity of loaded fact records

        Args:
            surrogate_key_mappings: Surrogate key mappings from dimensions

        Returns:
            bool: True if validation passes, False otherwise
        """
        self.logger.info("Validating fact table referential integrity")

        try:
            validation_queries = [
                # Check for orphaned customer references
                """
                SELECT COUNT(*) FROM fact_claims f
                LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
                WHERE c.customer_key IS NULL
                """,
                # Check for orphaned policy references
                """
                SELECT COUNT(*) FROM fact_claims f
                LEFT JOIN dim_policy p ON f.policy_key = p.policy_key
                WHERE p.policy_key IS NULL
                """,
                # Check for orphaned agent references
                """
                SELECT COUNT(*) FROM fact_claims f
                LEFT JOIN dim_agent a ON f.agent_key = a.agent_key
                WHERE a.agent_key IS NULL
                """,
                # Check for orphaned filed date references
                """
                SELECT COUNT(*) FROM fact_claims f
                LEFT JOIN dim_date d ON f.filed_date_key = d.date_key
                WHERE d.date_key IS NULL
                """,
            ]

            validation_names = ["customers", "policies", "agents", "filed_dates"]
            all_valid = True

            for i, query in enumerate(validation_queries):
                self.cursor.execute(query)
                orphaned_count = self.cursor.fetchone()[0]

                if orphaned_count > 0:
                    self.logger.error(
                        f"Found {orphaned_count} orphaned {validation_names[i]} references"
                    )
                    all_valid = False

            if all_valid:
                self.logger.info("All referential integrity checks passed")
            else:
                self.logger.error("Referential integrity validation failed")

            return all_valid

        except Exception as e:
            self.logger.error(f"Error validating referential integrity: {e}")
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


def load_insurance_facts(
    config: InsuranceETLConfig,
    facts_data: List[Dict[str, Any]],
    surrogate_key_mappings: Dict[str, Dict[str, int]],
) -> Dict[str, int]:
    """
    Convenience function to load all insurance facts

    Args:
        config: Insurance ETL configuration
        facts_data: Transformed facts data
        surrogate_key_mappings: Surrogate key mappings from dimension loading

    Returns:
        Dictionary containing loading statistics
    """
    loader = InsuranceFactLoader(config)
    return loader.load_all_facts(facts_data, surrogate_key_mappings)


if __name__ == "__main__":
    # Test fact loading functionality
    from config import load_config

    try:
        print("Testing Insurance Fact Loading")
        print("=" * 35)

        config = load_config()
        loader = InsuranceFactLoader(config)

        # Test with sample data
        sample_facts = [
            {
                "claim_id": "CLM00001",
                "policy_id": "POL0001",
                "customer_id": "CUST001",
                "agent_id": "AGT001",
                "filed_date_key": 20240315,
                "closed_date_key": 20240420,
                "claim_amount": 5000.00,
                "coverage_amount": 50000.00,
                "deductible_amount": 500.00,
                "payout_amount": 4500.00,
                "processing_days": 36,
                "claim_status": "Approved",
            }
        ]

        sample_mappings = {
            "dim_customer": {"CUST001": 1},
            "dim_policy": {"POL0001": 1},
            "dim_agent": {"AGT001": 1},
        }

        print("Testing fact loading with sample data...")
        stats = loader.load_all_facts(sample_facts, sample_mappings)

        print("Fact Loading Results:")
        for stat_name, value in stats.items():
            print(f"  - {stat_name}: {value}")

    except Exception as e:
        print(f"Fact loading test failed: {e}")
        print("Ensure database schema and dimensions are loaded first!")
