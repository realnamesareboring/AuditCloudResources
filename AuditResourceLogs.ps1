# Azure Diagnostic Settings Audit - Enhanced Version with Dynamic Table Mapping
# Features: Subscription selection, filtered results, and accurate table name mapping

param(
    [switch]$Verbose,  # Show all resources, including those without diagnostic settings
    [string]$SubscriptionId,  # Optional: specify subscription ID directly
    [switch]$DisableTableLookup  # Disable web lookup for table names (faster but less accurate)
)

Write-Host "Azure Diagnostic Settings Audit Tool" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green
Write-Host ""

# Global cache for table name lookups and official table list
$script:TableNameCache = @{}
$script:OfficialTableList = @()

# Function to fetch and parse Microsoft's official table reference
function Initialize-TableMapping {
    Write-Host "Initializing table name mapping from Microsoft documentation..." -ForegroundColor Yellow
    
    try {
        # Fetch the official table reference page
        $response = Invoke-WebRequest -Uri "https://learn.microsoft.com/en-us/azure/azure-monitor/reference/tables-category" -UseBasicParsing -TimeoutSec 30
        
        # Parse table names from the content using regex
        $tablePattern = 'tables/([a-zA-Z0-9_]+)\)'
        $matches = [regex]::Matches($response.Content, $tablePattern)
        
        $script:OfficialTableList = $matches | ForEach-Object { $_.Groups[1].Value } | Sort-Object | Get-Unique
        
        Write-Host "  -> Found $($script:OfficialTableList.Count) official Log Analytics tables" -ForegroundColor Green
        return $true
        
    } catch {
        Write-Warning "Could not fetch official table reference: $($_.Exception.Message)"
        Write-Host "  -> Using fallback table mapping only" -ForegroundColor Yellow
        return $false
    }
}

# Enhanced function to map diagnostic category names to actual Log Analytics table names
function Get-LogAnalyticsTableName {
    param(
        [string]$CategoryName,
        [string]$ResourceType
    )
    
    # Check cache first
    $cacheKey = "$CategoryName|$ResourceType"
    if ($script:TableNameCache.ContainsKey($cacheKey)) {
        return $script:TableNameCache[$cacheKey]
    }
    
    $tableName = $null
    
    # Try to find exact or pattern-based matches in the official table list
    if ($script:OfficialTableList.Count -gt 0) {
        
        # Direct matches (case-insensitive)
        $exactMatch = $script:OfficialTableList | Where-Object { $_ -ieq $CategoryName }
        if ($exactMatch) {
            $tableName = $exactMatch
        }
        
        # Pattern-based matching for common transformations
        if (-not $tableName) {
            
            # Common prefixes and patterns
            $patterns = @(
                "DCR$CategoryName",           # Data Collection Rules: LogErrors -> DCRLogErrors
                "LA$CategoryName",            # Log Analytics: SummaryLogs -> LASummaryLogs, QueryLogs -> LAQueryLogs
                "$CategoryName" + "Logs",     # Category + Logs: Storage -> StorageLogs
                "Container$CategoryName",     # Container logs
                "AppService$CategoryName",    # App Service logs
                "Storage$CategoryName",       # Storage logs
                "CDB$CategoryName"           # Cosmos DB: DataPlaneRequests -> CDBDataPlaneRequests
            )
            
            foreach ($pattern in $patterns) {
                $match = $script:OfficialTableList | Where-Object { $_ -ieq $pattern }
                if ($match) {
                    $tableName = $match
                    break
                }
            }
        }
        
        # Resource-specific pattern matching
        if (-not $tableName) {
            switch -Regex ($ResourceType) {
                'Microsoft\.Storage' {
                    # Storage patterns
                    if ($CategoryName -match 'Storage(Read|Write|Delete)') {
                        $tableName = $script:OfficialTableList | Where-Object { $_ -ieq 'StorageBlobLogs' }
                    }
                }
                'Microsoft\.DocumentDB' {
                    # Cosmos DB patterns
                    $cosmosPattern = "CDB$CategoryName"
                    $tableName = $script:OfficialTableList | Where-Object { $_ -ieq $cosmosPattern }
                }
                'Microsoft\.Web' {
                    # App Service patterns
                    $appServicePattern = "AppService$CategoryName"
                    $tableName = $script:OfficialTableList | Where-Object { $_ -ieq $appServicePattern }
                }
                'Microsoft\.ContainerRegistry' {
                    # Container Registry patterns
                    $containerPattern = "ContainerRegistry$CategoryName"
                    $tableName = $script:OfficialTableList | Where-Object { $_ -ieq $containerPattern }
                }
            }
        }
    }
    
    # Fallback to intelligent defaults if no official match found
    if (-not $tableName) {
        $tableName = Get-FallbackTableName -CategoryName $CategoryName -ResourceType $ResourceType
    }
    
    # Cache the result
    $script:TableNameCache[$cacheKey] = $tableName
    return $tableName
}

# Fallback function for when official lookup fails
function Get-FallbackTableName {
    param(
        [string]$CategoryName,
        [string]$ResourceType
    )
    
    # Apply intelligent defaults based on known patterns
    switch -Regex ($ResourceType) {
        'Microsoft\.Storage' {
            if ($CategoryName -match 'Storage(Read|Write|Delete)') {
                return 'StorageBlobLogs'
            }
            return "Storage$($CategoryName)Logs"
        }
        'Microsoft\.Logic' {
            return 'AzureDiagnostics'  # Logic Apps typically use AzureDiagnostics
        }
        'Microsoft\.KeyVault' {
            if ($CategoryName -eq 'AuditEvent') {
                return 'KeyVaultLogs'
            }
            return 'AzureDiagnostics'
        }
        'Microsoft\.Web' {
            if ($CategoryName -like 'AppService*') {
                return $CategoryName  # App Service table names usually match category names
            }
            return "AppService$CategoryName"
        }
        'Microsoft\.Sql' {
            return 'AzureDiagnostics'  # SQL typically uses AzureDiagnostics
        }
        'Microsoft\.Network' {
            return 'AzureDiagnostics'  # Network resources typically use AzureDiagnostics
        }
        'Microsoft\.DocumentDB' {
            return "CDB$CategoryName"  # Cosmos DB uses CDB prefix
        }
        'Microsoft\.EventHub' {
            return 'AzureDiagnostics'
        }
        'Microsoft\.ServiceBus' {
            return 'AzureDiagnostics'
        }
        'Microsoft\.Insights' {
            # Data Collection Rules
            if ($CategoryName -like '*Errors' -or $CategoryName -like '*Error*') {
                return "DCR$CategoryName"
            }
            if ($CategoryName -like '*Logs' -or $CategoryName -like '*Log*') {
                return "LA$CategoryName"
            }
        }
    }
    
    # Generic fallbacks
    if ($CategoryName -match '^(Summary|Query|Audit|Job)') {
        return "LA$CategoryName"  # Many system logs have LA prefix
    }
    
    if ($CategoryName -like '*Logs' -or $CategoryName -like '*Log') {
        return $CategoryName  # If it already has "Log" in the name, it might be the table name
    }
    
    # Last resort
    return "$CategoryName (estimated)"
}

# Function to select subscription
function Select-AzureSubscription {
    Write-Host "Getting available subscriptions..." -ForegroundColor Yellow
    $subscriptions = Get-AzSubscription | Sort-Object Name
    
    if ($subscriptions.Count -eq 0) {
        Write-Error "No subscriptions found. Please check your Azure authentication."
        exit 1
    }
    
    if ($subscriptions.Count -eq 1) {
        Write-Host "Only one subscription found: $($subscriptions[0].Name)" -ForegroundColor Green
        return $subscriptions[0]
    }
    
    Write-Host ""
    Write-Host "Available Subscriptions:" -ForegroundColor Cyan
    Write-Host "========================" -ForegroundColor Cyan
    
    for ($i = 0; $i -lt $subscriptions.Count; $i++) {
        $sub = $subscriptions[$i]
        Write-Host "$($i + 1). $($sub.Name) ($($sub.Id))" -ForegroundColor White
    }
    
    Write-Host ""
    do {
        $selection = Read-Host "Please select a subscription (1-$($subscriptions.Count))"
        $selectedIndex = [int]$selection - 1
    } while ($selectedIndex -lt 0 -or $selectedIndex -ge $subscriptions.Count)
    
    return $subscriptions[$selectedIndex]
}

# Authentication check
try {
    $currentContext = Get-AzContext
    if (-not $currentContext) {
        Write-Host "Please authenticate first..." -ForegroundColor Yellow
        Connect-AzAccount
    }
} catch {
    Write-Host "Please authenticate first..." -ForegroundColor Yellow
    Connect-AzAccount
}

# Initialize table mapping unless disabled
if (-not $DisableTableLookup) {
    $mappingSuccess = Initialize-TableMapping
} else {
    $mappingSuccess = $false
    Write-Host "Table lookup disabled - using fallback names only" -ForegroundColor Yellow
}

# Subscription selection
if ($SubscriptionId) {
    Write-Host "Using specified subscription ID: $SubscriptionId" -ForegroundColor Green
    $selectedSubscription = Get-AzSubscription -SubscriptionId $SubscriptionId
    if (-not $selectedSubscription) {
        Write-Error "Subscription $SubscriptionId not found or not accessible."
        exit 1
    }
} else {
    $selectedSubscription = Select-AzureSubscription
}

# Set the subscription context
Write-Host ""
Write-Host "Setting subscription context to: $($selectedSubscription.Name)" -ForegroundColor Green
Set-AzContext -SubscriptionId $selectedSubscription.Id | Out-Null

Write-Host ""
Write-Host "Audit Settings:" -ForegroundColor Cyan
Write-Host "===============" -ForegroundColor Cyan
Write-Host "Subscription: $($selectedSubscription.Name)"
Write-Host "Subscription ID: $($selectedSubscription.Id)"
Write-Host "Verbose Mode: $(if($Verbose) {'Enabled - Will show all resources'} else {'Disabled - Will only show resources with diagnostic settings'})"
Write-Host "Table Mapping: $(if($DisableTableLookup) {'Disabled - Using fallback names only'} elseif($mappingSuccess) {'Enabled - Using official Microsoft table reference'} else {'Fallback mode - Official reference unavailable'})"
Write-Host ""

# Get all resources
Write-Host "Getting all resources in subscription..." -ForegroundColor Yellow
$startTime = Get-Date
$allResources = Get-AzResource
Write-Host "Found $($allResources.Count) total resources" -ForegroundColor Green

if (-not $Verbose) {
    Write-Host "Filtering to only resources with diagnostic capabilities..." -ForegroundColor Yellow
}
Write-Host ""

# Initialize results array
$allResults = @()
$processedCount = 0
$resourcesWithDiagnostics = 0

# Function to process diagnostic settings for any resource ID
function Get-DiagnosticInfo {
    param(
        [string]$ResourceId,
        [string]$ResourceName,
        [string]$ResourceType,
        [string]$ResourceGroup,
        [string]$ServiceType = "",
        [string]$SubscriptionName,
        [string]$SubscriptionId,
        [bool]$ShowProgress = $true
    )
    
    $diagnosticResults = @()
    $displayName = if ($ServiceType) { "$ResourceName ($ServiceType)" } else { $ResourceName }
    $displayType = if ($ServiceType) { "$ResourceType/$ServiceType" } else { $ResourceType }
    
    try {
        if ($ShowProgress) {
            Write-Progress -Activity "Processing Resource" -Status $displayName -PercentComplete -1
        }
        
        # Get available categories (suppress deprecation warnings)
        $categories = Get-AzDiagnosticSettingCategory -ResourceId $ResourceId -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
        
        if ($categories -and $categories.Count -gt 0) {
            # Get current settings (suppress deprecation warnings)
            $currentSettings = Get-AzDiagnosticSetting -ResourceId $ResourceId -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
            
            # Process log categories
            foreach ($category in $categories | Where-Object { $_.CategoryType -eq "Logs" }) {
                $enabled = $false
                if ($currentSettings) {
                    $logSetting = $currentSettings.Log | Where-Object { 
                        $_.Category -eq $category.Name -and $_.Enabled -eq $true 
                    } | Select-Object -First 1
                    $enabled = [bool]$logSetting
                }
                
                # Get the actual table name (either lookup or fallback)
                $logAnalyticsTable = if ($DisableTableLookup) {
                    Get-FallbackTableName -CategoryName $category.Name -ResourceType $ResourceType
                } else {
                    Get-LogAnalyticsTableName -CategoryName $category.Name -ResourceType $ResourceType
                }
                
                $diagnosticResults += [PSCustomObject]@{
                    SubscriptionName = $SubscriptionName
                    SubscriptionId = $SubscriptionId
                    ResourceName = $displayName
                    ResourceType = $displayType
                    ResourceGroup = $ResourceGroup
                    LogCategory = $category.Name
                    LogAnalyticsTable = $logAnalyticsTable
                    MetricCategory = ""
                    Enabled = $enabled
                    ResourceId = $ResourceId
                }
            }
            
            # Process metric categories
            foreach ($category in $categories | Where-Object { $_.CategoryType -eq "Metrics" }) {
                $enabled = $false
                if ($currentSettings) {
                    $metricSetting = $currentSettings.Metric | Where-Object { 
                        $_.Category -eq $category.Name -and $_.Enabled -eq $true 
                    } | Select-Object -First 1
                    $enabled = [bool]$metricSetting
                }
                
                # For metrics, table name is usually "Metrics" unless it's a specific metric table
                $metricTableName = if ($category.Name -eq "AllMetrics") { "AzureMetrics" } else { "$($category.Name) (Metrics)" }
                
                $diagnosticResults += [PSCustomObject]@{
                    SubscriptionName = $SubscriptionName
                    SubscriptionId = $SubscriptionId
                    ResourceName = $displayName
                    ResourceType = $displayType
                    ResourceGroup = $ResourceGroup
                    LogCategory = ""
                    LogAnalyticsTable = $metricTableName
                    MetricCategory = $category.Name
                    Enabled = $enabled
                    ResourceId = $ResourceId
                }
            }
            
            return @{
                Results = $diagnosticResults
                HasDiagnostics = $true
            }
        } else {
            # No diagnostic settings available
            if ($Verbose) {
                $noDataResult = [PSCustomObject]@{
                    SubscriptionName = $SubscriptionName
                    SubscriptionId = $SubscriptionId
                    ResourceName = $displayName
                    ResourceType = $displayType
                    ResourceGroup = $ResourceGroup
                    LogCategory = "No diagnostic settings available"
                    LogAnalyticsTable = "N/A"
                    MetricCategory = ""
                    Enabled = "N/A"
                    ResourceId = $ResourceId
                }
                return @{
                    Results = @($noDataResult)
                    HasDiagnostics = $false
                }
            } else {
                return @{
                    Results = @()
                    HasDiagnostics = $false
                }
            }
        }
    } catch {
        # Error processing this resource
        if ($Verbose) {
            $errorResult = [PSCustomObject]@{
                SubscriptionName = $SubscriptionName
                SubscriptionId = $SubscriptionId
                ResourceName = $displayName
                ResourceType = $displayType
                ResourceGroup = $ResourceGroup
                LogCategory = "Error: $($_.Exception.Message)"
                LogAnalyticsTable = "Error"
                MetricCategory = ""
                Enabled = "Error"
                ResourceId = $ResourceId
            }
            return @{
                Results = @($errorResult)
                HasDiagnostics = $false
            }
        } else {
            return @{
                Results = @()
                HasDiagnostics = $false
            }
        }
    }
}

# Process resources
$totalResources = $allResources.Count

foreach ($resource in $allResources) {
    $processedCount++
    $percentComplete = [int](($processedCount / $totalResources) * 100)
    
    Write-Progress -Activity "Processing Resources" -Status "Resource $processedCount of $totalResources - $($resource.Name)" -PercentComplete $percentComplete
    
    # Main resource processing
    $result = Get-DiagnosticInfo -ResourceId $resource.ResourceId -ResourceName $resource.Name -ResourceType $resource.ResourceType -ResourceGroup $resource.ResourceGroupName -SubscriptionName $selectedSubscription.Name -SubscriptionId $selectedSubscription.Id -ShowProgress $false
    
    if ($result.HasDiagnostics -or $Verbose) {
        $allResults += $result.Results
        if ($result.HasDiagnostics) {
            $resourcesWithDiagnostics++
        }
    }
    
    # Handle special cases with sub-services
    if ($result.HasDiagnostics) {
        switch ($resource.ResourceType) {
            "Microsoft.Storage/storageAccounts" {
                $subServices = @(
                    @{Name="blob"; Path="blobServices/default"},
                    @{Name="file"; Path="fileServices/default"},
                    @{Name="queue"; Path="queueServices/default"},
                    @{Name="table"; Path="tableServices/default"}
                )
                
                foreach ($subService in $subServices) {
                    $subResourceId = "$($resource.ResourceId)/$($subService.Path)"
                    $subResult = Get-DiagnosticInfo -ResourceId $subResourceId -ResourceName $resource.Name -ResourceType $resource.ResourceType -ResourceGroup $resource.ResourceGroupName -ServiceType $subService.Name -SubscriptionName $selectedSubscription.Name -SubscriptionId $selectedSubscription.Id -ShowProgress $false
                    
                    if ($subResult.HasDiagnostics -or $Verbose) {
                        $allResults += $subResult.Results
                    }
                }
            }
            
            "Microsoft.Sql/servers" {
                try {
                    $databases = Get-AzSqlDatabase -ServerName $resource.Name -ResourceGroupName $resource.ResourceGroupName -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
                    foreach ($db in $databases | Where-Object { $_.DatabaseName -ne "master" }) {
                        $dbResourceId = "$($resource.ResourceId)/databases/$($db.DatabaseName)"
                        $dbResult = Get-DiagnosticInfo -ResourceId $dbResourceId -ResourceName $resource.Name -ResourceType $resource.ResourceType -ResourceGroup $resource.ResourceGroupName -ServiceType "database-$($db.DatabaseName)" -SubscriptionName $selectedSubscription.Name -SubscriptionId $selectedSubscription.Id -ShowProgress $false
                        
                        if ($dbResult.HasDiagnostics -or $Verbose) {
                            $allResults += $dbResult.Results
                        }
                    }
                } catch {
                    # Skip SQL databases if we can't access them
                }
            }
        }
    }
    
    # Show progress every 20 resources
    if ($processedCount % 20 -eq 0) {
        $elapsed = (Get-Date) - $startTime
        $avgPerResource = $elapsed.TotalSeconds / $processedCount
        $estimatedRemaining = ($totalResources - $processedCount) * $avgPerResource
        Write-Host "  Progress: $processedCount/$totalResources processed. Resources with diagnostics: $resourcesWithDiagnostics. ETA: $([int]$estimatedRemaining)s" -ForegroundColor Gray
    }
}

Write-Progress -Activity "Processing Resources" -Completed

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "Processing completed!" -ForegroundColor Green
Write-Host "Processing time: $($duration.TotalMinutes.ToString('F2')) minutes ($($duration.TotalSeconds.ToString('F2')) seconds)" -ForegroundColor Green
Write-Host ""

# Export results
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$subscriptionSafe = $selectedSubscription.Name -replace '[^\w\-_]', '_'
$modeIndicator = if ($Verbose) { "_verbose" } else { "_filtered" }
$outputFile = "diagnostic_audit_$($subscriptionSafe)$($modeIndicator)_$timestamp.csv"

$allResults | Export-Csv -Path $outputFile -NoTypeInformation

# Generate summary
$totalCategories = ($allResults | Where-Object { $_.LogCategory -ne "No diagnostic settings available" -and $_.LogCategory -notlike "Error:*" }).Count
$enabledCategories = ($allResults | Where-Object { $_.Enabled -eq $true }).Count
$disabledCategories = ($allResults | Where-Object { $_.Enabled -eq $false }).Count
$errorCount = ($allResults | Where-Object { $_.LogCategory -like "Error:*" }).Count
$noSettingsCount = ($allResults | Where-Object { $_.LogCategory -eq "No diagnostic settings available" }).Count

Write-Host "Audit Results Summary:" -ForegroundColor Green
Write-Host "=====================" -ForegroundColor Green
Write-Host "Subscription: $($selectedSubscription.Name)"
Write-Host "Output file: $outputFile"
Write-Host "Total resources scanned: $totalResources"
Write-Host "Resources with diagnostic capabilities: $resourcesWithDiagnostics" -ForegroundColor Cyan
Write-Host "Total diagnostic categories found: $totalCategories"
Write-Host "Currently enabled: $enabledCategories" -ForegroundColor Green
Write-Host "Currently disabled: $disabledCategories" -ForegroundColor Red
if ($Verbose) {
    Write-Host "Resources without diagnostics: $noSettingsCount" -ForegroundColor Gray
    Write-Host "Errors encountered: $errorCount" -ForegroundColor Yellow
}
Write-Host "Processing time: $($duration.TotalMinutes.ToString('F2')) minutes"
Write-Host ""

# Display sample results
Write-Host "Sample Results:" -ForegroundColor Yellow
$sampleResults = $allResults | Where-Object { $_.LogCategory -ne "No diagnostic settings available" -and $_.LogCategory -notlike "Error:*" } | Select-Object -First 10
if ($sampleResults.Count -gt 0) {
    $sampleResults | Select-Object ResourceName, ResourceType, LogCategory, LogAnalyticsTable, MetricCategory, Enabled | Format-Table -AutoSize
    
    Write-Host ""
    Write-Host "Table Name Mapping Information:" -ForegroundColor Cyan
    Write-Host "===============================" -ForegroundColor Cyan
    Write-Host "• LogCategory: Name shown in Azure portal diagnostic settings"
    Write-Host "• LogAnalyticsTable: Actual table name in Log Analytics workspace"
    Write-Host "• $(if($mappingSuccess) {'✓ Using official Microsoft table reference for accurate mappings'} else {'⚠ Using intelligent fallback mappings (official reference unavailable)'})"
    if ($script:OfficialTableList.Count -gt 0) {
        Write-Host "• Found $($script:OfficialTableList.Count) official table names from Microsoft docs"
    }
    Write-Host "• Names with '(estimated)' indicate intelligent guesses - verify in your Log Analytics workspace"
    Write-Host ""
} else {
    Write-Host "No diagnostic settings found in this subscription." -ForegroundColor Red
}

# Show resources by type
$resourceTypes = $allResults | Where-Object { $_.LogCategory -ne "No diagnostic settings available" -and $_.LogCategory -notlike "Error:*" } | Group-Object ResourceType | Sort-Object Count -Descending
if ($resourceTypes.Count -gt 0) {
    Write-Host ""
    Write-Host "Resources with Diagnostic Settings by Type:" -ForegroundColor Cyan
    $resourceTypes | Select-Object @{Name="ResourceType";Expression={$_.Name}}, @{Name="Count";Expression={$_.Count}} | Format-Table -AutoSize
}

Write-Host ""
Write-Host "Full results saved to: $outputFile" -ForegroundColor Green
Write-Host ""
Write-Host "Usage Tips:" -ForegroundColor Yellow
Write-Host "- Run without -Verbose for clean auditor reports (default)"
Write-Host "- Run with -Verbose to see all resources including those without diagnostic settings"
Write-Host "- Use -SubscriptionId parameter to run non-interactively"
Write-Host "- Use -DisableTableLookup for faster execution with fallback table names"