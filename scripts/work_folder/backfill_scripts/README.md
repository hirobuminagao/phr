This directory contains one-time migration / backfill scripts.

These scripts are used during schema upgrades and must NOT be executed regularly.

Each script corresponds to a schema change recorded in ADR documents.

Typical upgrade flow:

1. ADR defines the schema change
2. DDL migration is applied to the database
3. A backfill script in this directory is executed once to update existing data
4. Application code begins using the new schema

Important:
- These scripts are **not part of normal system operation**
- They are intended to be executed manually during controlled upgrades
- Each script should clearly indicate the version or change it belongs to (e.g. v1_0_1_*)