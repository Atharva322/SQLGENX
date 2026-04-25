# Helper scripts for setup and maintenance.

## Large Dataset Seed

Use this script to repopulate the demo DB with a larger synthetic dataset:

```powershell
.\scripts\seed_large_dataset.ps1
```

Defaults:
- container: `text2sql-db`
- database: `sample_company`
- user: `text2sql_user`
