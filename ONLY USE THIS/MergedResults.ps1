# Load JSON directly and build a simple string-based lookup
$jsonData = Get-Content "AzureMonitorTables_Index_Structure.json" -Raw | ConvertFrom-Json

# Build lookup with ALL tables per ResourceType as a single string
$lookupTable = @{}

foreach ($categoryName in $jsonData.Categories.PSObject.Properties.Name) {
    $category = $jsonData.Categories.$categoryName
    
    foreach ($providerName in $category.PSObject.Properties.Name) {
        $provider = $category.$providerName
        
        if ($provider.ResourceType -and $provider.Tables) {
            $resourceType = [string]$provider.ResourceType
            
            # Get all table names for this resource type
            $tableNames = @()
            foreach ($table in $provider.Tables) {
                if ($table.TableName) {
                    $tableNames += [string]$table.TableName
                }
            }
            
            # Add to lookup table (combine if ResourceType already exists)
            if ($lookupTable.ContainsKey($resourceType)) {
                $existingTables = $lookupTable[$resourceType] -split "; "
                $allTableNames = ($existingTables + $tableNames | Sort-Object | Get-Unique) -join "; "
                $lookupTable[$resourceType] = $allTableNames
            } else {
                $lookupTable[$resourceType] = ($tableNames | Sort-Object | Get-Unique) -join "; "
            }
        }
    }
}

# Show what we built for debugging
Write-Host "Lookup table built with $($lookupTable.Keys.Count) resource types from JSON"
$lookupTable.Keys | Sort-Object | Select-Object -First 5 | ForEach-Object {
    Write-Host "$_ -> $($lookupTable[$_])"
}

# Now do the audit with simple string lookup
$results = @()
Get-AzResource | ForEach-Object {
    try {
        $categories = Get-AzDiagnosticSettingCategory -ResourceId $_.ResourceId -ErrorAction SilentlyContinue
        foreach ($cat in $categories | Where-Object {$_.CategoryType -eq "Logs"}) {
            
            # Simple direct string lookup - no array operations
            $allTablesForType = "Unknown"
            if ($lookupTable.ContainsKey($_.ResourceType)) {
                $allTablesForType = [string]$lookupTable[$_.ResourceType]
            }
            
            $results += [PSCustomObject]@{
                ResourceType = [string]$_.ResourceType
                ResourceName = [string]$_.Name
                CategoryType = [string]$cat.CategoryType
                CategoryName = [string]$cat.Name
                LogAnalyticsTable = $allTablesForType
            }
        }
    } catch { }
}

$results | Format-Table -AutoSize