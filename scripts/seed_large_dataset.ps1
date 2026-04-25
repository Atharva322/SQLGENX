param(
    [string]$Container = "text2sql-db",
    [string]$Database = "sample_company",
    [string]$User = "text2sql_user"
)

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$seedFile = Join-Path $root "db\\init\\002_seed_large_dataset.sql"

if (-not (Test-Path $seedFile)) {
    throw "Seed file not found: $seedFile"
}

Write-Host "Applying large seed file to container '$Container'..."
Get-Content -Raw $seedFile | docker exec -i $Container psql -U $User -d $Database -v ON_ERROR_STOP=1

if ($LASTEXITCODE -ne 0) {
    throw "Failed to apply seed file."
}

Write-Host "Seed complete. Current table counts:"
docker exec -i $Container psql -U $User -d $Database -c "SELECT COUNT(*) AS departments_count FROM departments;"
docker exec -i $Container psql -U $User -d $Database -c "SELECT COUNT(*) AS employees_count FROM employees;"
docker exec -i $Container psql -U $User -d $Database -c "SELECT COUNT(*) AS sales_count FROM sales;"
