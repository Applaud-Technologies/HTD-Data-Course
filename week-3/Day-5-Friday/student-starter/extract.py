"""
Data Extraction Module for Insurance ETL Pipeline

This module handles data extraction from multiple sources (CSV, JSON) for the insurance
ETL pipeline. Provides standardized data extraction with error handling and validation.

STUDENT ASSIGNMENT: Complete the 2 TODO helper methods below

Key Components:
- InsuranceExtractor: Main extraction class for insurance data sources
- CSV extraction: Customer data from CSV files
- JSON extraction: Policy data from JSON files
- Claims data extraction: Claims data from CSV files
- Data validation and quality checks
- Standardized return format for transformation phase

Target Audience: Beginner/Intermediate students
Complexity Level: SIMPLE (~150 lines of code, 2 minor TODOs for students)
"""

import csv
import json
import logging
import os
from typing import List, Dict, Any, Optional
from pathlib import Path
from config import InsuranceETLConfig


class InsuranceExtractor:
    """
    Data extraction class for Insurance ETL pipeline

    Handles extraction from CSV and JSON sources with proper error handling
    and data validation for insurance business data.
    """

    def __init__(self, config: InsuranceETLConfig):
        """
        Initialize extractor with configuration

        Args:
            config: Insurance ETL configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

    def extract_all_sources(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from all configured insurance data sources
        This method is complete - it orchestrates all extractions.
        """
        self.logger.info("Starting extraction from all insurance data sources")

        extracted_data = {}

        try:
            # Extract customer data from CSV
            customers_path = self.config.get_data_source_path("customers_csv")
            extracted_data["customers"] = self.extract_customers_csv(customers_path)

            # Extract policy data from JSON
            policies_path = self.config.get_data_source_path("policies_json")
            extracted_data["policies"] = self.extract_policies_json(policies_path)

            # Extract claims data from CSV
            claims_path = self.config.get_data_source_path("claims_csv")
            extracted_data["claims"] = self.extract_claims_csv(claims_path)

            # Log extraction summary
            self.logger.info("Extraction Summary:")
            for source, data in extracted_data.items():
                self.logger.info(f"  - {source}: {len(data)} records")

            return extracted_data

        except Exception as e:
            self.logger.error(f"Failed to extract data from sources: {str(e)}")
            raise

    def extract_customers_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract customer data from CSV file
        This method is complete - it calls your TODO helper methods.
        """
        self.logger.info(f"Extracting customer data from: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Customer data file not found: {file_path}")

        customers = []

        try:
            with open(file_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)

                for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                    try:
                        # Basic data validation and type conversion (calls your TODO helper)
                        customer = {
                            "customer_id": row.get("customer_id", "").strip(),
                            "first_name": row.get("first_name", "").strip(),
                            "last_name": row.get("last_name", "").strip(),
                            "email": row.get("email", "").strip().lower(),
                            "phone": row.get("phone", "").strip(),
                            "birth_date": row.get("birth_date", "").strip(),
                            "address": row.get("address", "").strip(),
                            "city": row.get("city", "").strip(),
                            "state": row.get("state", "").strip(),
                            "risk_score": self._safe_float_conversion(
                                row.get("risk_score", "0")
                            ),
                            "customer_since": row.get("customer_since", "").strip(),
                        }

                        # Basic validation
                        if not customer["customer_id"]:
                            self.logger.warning(
                                f"Row {row_num}: Missing customer_id, skipping"
                            )
                            continue

                        # Validate email format (basic check)
                        if customer["email"] and "@" not in customer["email"]:
                            self.logger.warning(
                                f"Row {row_num}: Invalid email format: {customer['email']}"
                            )

                        customers.append(customer)

                    except Exception as e:
                        self.logger.warning(
                            f"Error processing customer row {row_num}: {e}"
                        )
                        continue

        except Exception as e:
            raise Exception(f"Failed to read customer CSV file: {str(e)}")

        self.logger.info(f"Successfully extracted {len(customers)} customer records")
        return customers

    def extract_policies_json(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract policy data from JSON file
        This method is complete - it calls your TODO helper methods.
        """
        self.logger.info(f"Extracting policy data from: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Policy data file not found: {file_path}")

        try:
            with open(file_path, "r", encoding="utf-8") as jsonfile:
                policies_data = json.load(jsonfile)

            if not isinstance(policies_data, list):
                raise ValueError(
                    "Policy JSON file must contain an array of policy objects"
                )

            policies = []
            valid_policy_types = {"Auto", "Home", "Life"}

            for index, policy_raw in enumerate(policies_data):
                try:
                    # Data validation and type conversion (calls your TODO helpers)
                    policy = {
                        "policy_id": policy_raw.get("policy_id", "").strip(),
                        "customer_id": policy_raw.get("customer_id", "").strip(),
                        "policy_type": policy_raw.get("policy_type", "").strip(),
                        "coverage_amount": self._safe_float_conversion(
                            policy_raw.get("coverage_amount", "0")
                        ),
                        "annual_premium": self._safe_float_conversion(
                            policy_raw.get("annual_premium", "0")
                        ),
                        "deductible": self._safe_float_conversion(
                            policy_raw.get("deductible", "0")
                        ),
                        "effective_date": policy_raw.get("effective_date", "").strip(),
                        "expiration_date": policy_raw.get(
                            "expiration_date", ""
                        ).strip(),
                        "status": policy_raw.get("status", "Active").strip(),
                    }

                    # Basic validation
                    if not policy["policy_id"]:
                        self.logger.warning(
                            f"Policy index {index}: Missing policy_id, skipping"
                        )
                        continue

                    if not policy["customer_id"]:
                        self.logger.warning(
                            f"Policy {policy['policy_id']}: Missing customer_id"
                        )

                    # Validate policy type
                    if policy["policy_type"] not in valid_policy_types:
                        self.logger.warning(
                            f"Policy {policy['policy_id']}: Invalid policy_type: {policy['policy_type']}"
                        )

                    # Validate numeric values
                    if policy["coverage_amount"] <= 0:
                        self.logger.warning(
                            f"Policy {policy['policy_id']}: Invalid coverage_amount: {policy['coverage_amount']}"
                        )

                    if policy["annual_premium"] <= 0:
                        self.logger.warning(
                            f"Policy {policy['policy_id']}: Invalid annual_premium: {policy['annual_premium']}"
                        )

                    policies.append(policy)

                except Exception as e:
                    self.logger.warning(f"Error processing policy index {index}: {e}")
                    continue

        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON format in policy file: {str(e)}")
        except Exception as e:
            raise Exception(f"Failed to read policy JSON file: {str(e)}")

        self.logger.info(f"Successfully extracted {len(policies)} policy records")
        return policies

    def extract_claims_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract claims data from CSV file
        This method is complete - it calls your TODO helper methods.
        """
        self.logger.info(f"Extracting claims data from: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Claims data file not found: {file_path}")

        claims = []

        try:
            with open(file_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)

                for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                    try:
                        # Data validation and type conversion (calls your TODO helpers)
                        claim = {
                            "claim_id": row.get("claim_id", "").strip(),
                            "policy_id": row.get("policy_id", "").strip(),
                            "customer_id": row.get("customer_id", "").strip(),
                            "agent_id": row.get("agent_id", "").strip(),
                            "claim_amount": self._safe_float_conversion(
                                row.get("claim_amount", "0")
                            ),
                            "coverage_amount": self._safe_float_conversion(
                                row.get("coverage_amount", "0")
                            ),
                            "deductible_amount": self._safe_float_conversion(
                                row.get("deductible_amount", "0")
                            ),
                            "payout_amount": self._safe_float_conversion(
                                row.get("payout_amount", "0")
                            ),
                            "filed_date": row.get("filed_date", "").strip(),
                            "closed_date": row.get("closed_date", "").strip(),
                            "processing_days": self._safe_int_conversion(
                                row.get("processing_days", "0")
                            ),
                            "claim_status": row.get("claim_status", "").strip(),
                        }

                        # Basic validation
                        if not claim["claim_id"]:
                            self.logger.warning(
                                f"Row {row_num}: Missing claim_id, skipping"
                            )
                            continue

                        # Validate required references
                        if not claim["policy_id"]:
                            self.logger.warning(
                                f"Claim {claim['claim_id']}: Missing policy_id"
                            )

                        if not claim["customer_id"]:
                            self.logger.warning(
                                f"Claim {claim['claim_id']}: Missing customer_id"
                            )

                        # Business rule validation
                        if (
                            claim["claim_amount"] > claim["coverage_amount"]
                            and claim["coverage_amount"] > 0
                        ):
                            self.logger.warning(
                                f"Claim {claim['claim_id']}: Claim amount exceeds coverage"
                            )

                        claims.append(claim)

                    except Exception as e:
                        self.logger.warning(
                            f"Error processing claims row {row_num}: {e}"
                        )
                        continue

        except Exception as e:
            raise Exception(f"Failed to read claims CSV file: {str(e)}")

        self.logger.info(f"Successfully extracted {len(claims)} claims records")
        return claims

    # =====================================================================
    # STUDENT TODO HELPER METHODS - COMPLETE THESE 2 METHODS
    # =====================================================================

    def _safe_float_conversion(self, value: Any) -> float:
        """
        TODO #1: Safely convert value to float with error handling

        EXAMPLES:
        - Input: "123.45" → Output: 123.45
        - Input: "" → Output: 0.0
        - Input: "invalid" → Output: 0.0
        - Input: 123 → Output: 123.0

        BUSINESS RULES:
        - Return 0.0 for invalid or empty values
        - Handle string values by stripping whitespace
        - Handle numeric values directly

        HINTS:
        - Use try/except to handle ValueError and TypeError
        - Check if value is already int or float
        - For strings, strip whitespace and check if empty
        - Return 0.0 as the default for any errors

        TESTING:
        Test with: "", "123.45", "invalid", 123, 45.67, None
        """
        # TODO: Your implementation here
        # Remove this pass statement and implement the method
        pass

    def _safe_int_conversion(self, value: Any) -> int:
        """
        TODO #2: Safely convert value to integer with error handling

        EXAMPLES:
        - Input: "123" → Output: 123
        - Input: "123.7" → Output: 123 (truncated)
        - Input: "" → Output: 0
        - Input: 45.9 → Output: 45

        BUSINESS RULES:
        - Return 0 for invalid or empty values
        - Truncate decimal values (don't round)
        - Handle string values by converting to float first

        HINTS:
        - Similar to _safe_float_conversion but return int
        - Use int(float(value)) to handle decimal strings like "123.7"
        - Handle different input types: int, float, str
        - Return 0 as the default for any errors

        TESTING:
        Test with: "", "123", "123.7", 45.9, "invalid", None
        """
        # TODO: Your implementation here
        # Remove this pass statement and implement the method
        pass

    # =====================================================================
    # COMPLETED METHODS - DO NOT MODIFY BELOW THIS LINE
    # =====================================================================

    def validate_extracted_data(
        self, extracted_data: Dict[str, List[Dict[str, Any]]]
    ) -> bool:
        """
        Validate extracted data for completeness and referential integrity
        This method is complete.
        """
        self.logger.info("Validating extracted data integrity")

        try:
            customers = extracted_data.get("customers", [])
            policies = extracted_data.get("policies", [])
            claims = extracted_data.get("claims", [])

            # Check minimum record counts
            if len(customers) < 50:
                self.logger.warning(
                    f"Low customer count: {len(customers)} (expected ~75)"
                )

            if len(policies) < 75:
                self.logger.warning(
                    f"Low policy count: {len(policies)} (expected ~125)"
                )

            if len(claims) < 400:
                self.logger.warning(f"Low claims count: {len(claims)} (expected ~650)")

            # Check referential integrity
            customer_ids = {c["customer_id"] for c in customers}
            policy_customer_ids = {p["customer_id"] for p in policies}
            claims_customer_ids = {c["customer_id"] for c in claims}

            # Find orphaned references
            orphaned_policy_customers = policy_customer_ids - customer_ids
            orphaned_claims_customers = claims_customer_ids - customer_ids

            if orphaned_policy_customers:
                self.logger.warning(
                    f"Policies with invalid customer references: {len(orphaned_policy_customers)}"
                )

            if orphaned_claims_customers:
                self.logger.warning(
                    f"Claims with invalid customer references: {len(orphaned_claims_customers)}"
                )

            self.logger.info("Data validation completed")
            return True

        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            return False


def extract_insurance_data(
    config: InsuranceETLConfig,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convenience function to extract all insurance data sources

    Args:
        config: Insurance ETL configuration

    Returns:
        Dictionary containing extracted data from all sources
    """
    extractor = InsuranceExtractor(config)
    return extractor.extract_all_sources()


if __name__ == "__main__":
    # Test your TODO implementations
    from config import load_config

    try:
        print("Testing Insurance Extract TODO Methods")
        print("=" * 45)

        config = load_config()
        extractor = InsuranceExtractor(config)

        # Test TODO #1: Float conversion
        print("Testing TODO #1: _safe_float_conversion")
        test_floats = [
            ("123.45", "Should be 123.45"),
            ("", "Should be 0.0"),
            ("invalid", "Should be 0.0"),
            (123, "Should be 123.0"),
            (45.67, "Should be 45.67"),
        ]

        for value, expected in test_floats:
            result = extractor._safe_float_conversion(value)
            print(f"  {repr(value)} → {result} ({expected})")

        # Test TODO #2: Int conversion
        print("\nTesting TODO #2: _safe_int_conversion")
        test_ints = [
            ("123", "Should be 123"),
            ("123.7", "Should be 123"),
            ("", "Should be 0"),
            (45.9, "Should be 45"),
            ("invalid", "Should be 0"),
        ]

        for value, expected in test_ints:
            result = extractor._safe_int_conversion(value)
            print(f"  {repr(value)} → {result} ({expected})")

        print("\n✅ TODO method testing completed!")
        print("Complete both TODO methods to make extraction work properly!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        print("Complete the TODO methods to fix this error!")
