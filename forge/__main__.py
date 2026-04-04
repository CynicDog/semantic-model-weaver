"""
Entry point: python -m forge --database <DB> --schema <SCHEMA>
"""

import argparse


def main():
    parser = argparse.ArgumentParser(
        prog="forge",
        description="Generate and evaluate a Cortex Analyst semantic model from a Snowflake schema.",  # noqa: E501
    )
    parser.add_argument("--database", required=True, help="Snowflake database name")
    parser.add_argument("--schema", required=True, help="Snowflake schema name")
    parser.add_argument(
        "--iterations", type=int, default=3, help="Max refinement iterations (default: 3)"
    )
    args = parser.parse_args()

    print(f"Forging semantic model for {args.database}.{args.schema} ...")
    # TODO: wire up pipeline stages


if __name__ == "__main__":
    main()
