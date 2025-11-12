# Azure Diagnostic Settings Audit - Proper HTML Structure Parsing
# Features: Follows exact Microsoft HTML structure using <ul>/<li> tags

param(
    [switch]$Verbose,
    [string]$SubscriptionId,
    [switch]$DisableTableLookup,
    [switch]$RefreshCache,
    [switch]$Debug,
    [int]$DelayMs = 500,
    [switch]$Validate
)

Write-Host "Azure Diagnostic Settings Audit Tool - HTML Structure Parser Edition" -ForegroundColor Green
Write-Host "====================================================================" -ForegroundColor Green
Write-Host ""

# Global variables
$script:TableNameCache = @{}
$script:ResourceTypeToTables = @{}
$script:CacheFile = "azure_resourcetype_tables_cache.json"
$script:ValidationErrors = @()

function Write-ValidationError {
    param([string]$Message)
    $script:ValidationErrors += $Message
    Write-Warning "VALIDATION: $Message"
}

function Test-TableNameValidity {
    param([string]$TableName)
    
    if ([string]::IsNullOrWhiteSpace($TableName)) {
        return $false
    }
    
    if ($TableName.Length -lt 3) {
        Write-ValidationError "Table name too short: '$TableName'"
        return $false
    }
    
    if ($TableName -notmatch '^[A-Za-z][A-Za-z0-9_]*$') {
        Write-ValidationError "Invalid table name format: '$TableName'"
        return $false
    }
    
    # Check for corruption patterns (single characters, etc.)
    $corruptionPatterns = @('^[A-Za-z]$', '^[A-Za-z][A-Za-z]$', '^\d+$')
    foreach ($pattern in $corruptionPatterns) {
        if ($TableName -match $pattern) {
            Write-ValidationError "Suspicious table name pattern: '$TableName'"
            return $false
        }
    }
    
    return $true
}

function Get-WebContent {
    param([string]$Url)
    
    $maxRetries = 3
    $retryDelay = 2000
    
    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            if ($Debug) {
                Write-Host "Debug: Fetching URL (attempt $attempt): $Url" -ForegroundColor Gray
            }
            
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 45 -ErrorAction Stop
            
            if ($response.Content.Length -lt 1000) {
                Write-Warning "Response too small, might be truncated"
                if ($attempt -lt $maxRetries) {
                    Start-Sleep -Milliseconds $retryDelay
                    continue
                }
            }
            
            Start-Sleep -Milliseconds $DelayMs
            return $response.Content
            
        } catch {
            Write-Warning "Attempt $attempt failed: $($_.Exception.Message)"
            if ($attempt -lt $maxRetries) {
                Start-Sleep -Milliseconds $retryDelay
            }
        }
    }
    
    return $null
}

# NEW: Proper HTML structure parser following Microsoft's <ul>/<li> format
function Get-ResourceTypeTableMapping {
    param([string]$HtmlContent)
    
    if ($Debug) {
        Write-Host "Debug: Starting proper HTML structure parsing using <ul>/<li> tags..." -ForegroundColor Gray
        Write-Host "Debug: HTML content length: $($HtmlContent.Length) characters" -ForegroundColor Gray
    }
    
    if ([string]::IsNullOrWhiteSpace($HtmlContent) -or $HtmlContent.Length -lt 1000) {
        Write-ValidationError "HTML content is empty or too small"
        return @{}
    }
    
    $resourceTypeMapping = @{}
    
    # Clean HTML
    $HtmlContent = $HtmlContent -replace '<script[^>]*>.*?</script>', ''
    $HtmlContent = $HtmlContent -replace '<style[^>]*>.*?</style>', ''
    
    Write-Host "  -> Using proper HTML structure: <h2/h3> -> <ul> -> <li><a href='tables/...'>" -ForegroundColor Yellow
    
    # Step 1: Find all service sections (headers followed by content)
    $sectionPattern = '<h[23][^>]*[^>]*>([^<]+)</h[23]>(.*?)(?=<h[23]|$)'
    $sectionMatches = [regex]::Matches($HtmlContent, $sectionPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline -bor [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    
    if ($Debug) {
        Write-Host "Debug: Found $($sectionMatches.Count) service sections to process" -ForegroundColor DarkGray
    }
    
    foreach ($sectionMatch in $sectionMatches) {
        $sectionTitle = $sectionMatch.Groups[1].Value.Trim() -replace '<[^>]+>', '' -replace '&amp;', '&'
        $sectionContent = $sectionMatch.Groups[2].Value
        
        # Skip navigation sections
        if ($sectionTitle -match "^(In this article|Feedback|Table of contents|Skip|Additional resources|Contents|Exit)") {
            continue
        }
        
        if ($Debug) {
            Write-Host "Debug: Processing service section: '$sectionTitle'" -ForegroundColor DarkGray
        }
        
        # Step 2: Look for ResourceType identifiers in this section
        $resourceTypes = @()
        
        # Direct ResourceType pattern matching
        $resourceTypePattern = '(Microsoft\.[A-Za-z][A-Za-z0-9]*(?:/[a-zA-Z][a-zA-Z0-9]*)?)'
        $resourceTypeMatches = [regex]::Matches($sectionContent, $resourceTypePattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        
        foreach ($rtMatch in $resourceTypeMatches) {
            $resourceType = $rtMatch.Groups[1].Value
            if ($resourceTypes -notcontains $resourceType) {
                $resourceTypes += $resourceType
                if ($Debug) {
                    Write-Host "Debug:   Found ResourceType: '$resourceType'" -ForegroundColor DarkGray
                }
            }
        }
        
        # Step 3: Infer ResourceType from service name if not found directly
        if ($resourceTypes.Count -eq 0) {
            $inferredResourceType = ""
            switch -Regex ($sectionTitle) {
                "PlayFab|Azure PlayFab" { $inferredResourceType = "Microsoft.PlayFab/titles" }
                "API Management|APIM" { $inferredResourceType = "Microsoft.ApiManagement/service" }
                "Logic Apps" { $inferredResourceType = "Microsoft.Logic/workflows" }
                "Storage|Azure Storage" { $inferredResourceType = "Microsoft.Storage/storageAccounts" }
                "App Service|Web Apps" { $inferredResourceType = "Microsoft.Web/sites" }
                "Operational Insights|Log Analytics" { $inferredResourceType = "Microsoft.OperationalInsights/workspaces" }
                "Key Vault" { $inferredResourceType = "Microsoft.KeyVault/vaults" }
                "Sentinel|Azure Sentinel" { $inferredResourceType = "microsoft.securityinsights" }
                "Data Collection|Insights" { $inferredResourceType = "Microsoft.Insights/dataCollectionRules" }
                "Network|Virtual Network" { $inferredResourceType = "Microsoft.Network/virtualNetworks" }
                "Load Balancer" { $inferredResourceType = "Microsoft.Network/LoadBalancers" }
                "Attestation" { $inferredResourceType = "Microsoft.Attestation/attestationProviders" }
            }
            
            if ($inferredResourceType) {
                $resourceTypes += $inferredResourceType
                if ($Debug) {
                    Write-Host "Debug:   Inferred ResourceType: '$inferredResourceType' from section '$sectionTitle'" -ForegroundColor DarkGray
                }
            }
        }
        
        # Step 4: Extract tables using proper <ul>/<li> structure
        $tablesInSection = @()
        
        # Find all <ul> blocks in this section
        $ulPattern = '<ul[^>]*>(.*?)</ul>'
        $ulMatches = [regex]::Matches($sectionContent, $ulPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline -bor [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        
        if ($Debug -and $ulMatches.Count -gt 0) {
            Write-Host "Debug:   Found $($ulMatches.Count) <ul> blocks in this section" -ForegroundColor DarkGray
        }
        
        foreach ($ulMatch in $ulMatches) {
            $ulContent = $ulMatch.Groups[1].Value
            
            # Extract <li><a href="tables/...">TableName</a></li> entries
            $liPattern = '<li[^>]*>.*?<a[^>]+href="tables/([^"]+)"[^>]*>([^<]+)</a>.*?</li>'
            $liMatches = [regex]::Matches($ulContent, $liPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline -bor [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
            
            if ($Debug -and $liMatches.Count -gt 0) {
                Write-Host "Debug:     <ul> contains $($liMatches.Count) table entries" -ForegroundColor DarkGray
            }
            
            foreach ($liMatch in $liMatches) {
                $tableUrl = $liMatch.Groups[1].Value.Trim()
                $tableName = $liMatch.Groups[2].Value.Trim()
                
                # Validate and add table
                if (Test-TableNameValidity -TableName $tableName) {
                    if ($tablesInSection -notcontains $tableName) {
                        $tablesInSection += $tableName
                        if ($Debug) {
                            Write-Host "Debug:       Valid table: '$tableName'" -ForegroundColor Green
                        }
                    }
                } else {
                    if ($Debug) {
                        Write-Host "Debug:       Invalid table rejected: '$tableName'" -ForegroundColor Red
                    }
                }
            }
        }
        
        # Step 5: Fallback - if no <ul> structure, try direct links
        if ($tablesInSection.Count -eq 0) {
            $directLinkPattern = '<a[^>]+href="tables/([^"]+)"[^>]*>([^<]+)</a>'
            $directMatches = [regex]::Matches($sectionContent, $directLinkPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
            
            if ($Debug -and $directMatches.Count -gt 0) {
                Write-Host "Debug:   Fallback: Found $($directMatches.Count) direct table links" -ForegroundColor DarkGray
            }
            
            foreach ($directMatch in $directMatches) {
                $tableName = $directMatch.Groups[2].Value.Trim()
                if (Test-TableNameValidity -TableName $tableName) {
                    if ($tablesInSection -notcontains $tableName) {
                        $tablesInSection += $tableName
                        if ($Debug) {
                            Write-Host "Debug:     Fallback table: '$tableName'" -ForegroundColor DarkGray
                        }
                    }
                }
            }
        }
        
        # Step 6: Associate ResourceTypes with tables from this section
        if ($tablesInSection.Count -gt 0 -and $resourceTypes.Count -gt 0) {
            foreach ($resourceType in $resourceTypes) {
                if (-not $resourceTypeMapping.ContainsKey($resourceType)) {
                    $resourceTypeMapping[$resourceType] = @()
                }
                
                foreach ($table in $tablesInSection) {
                    if ($resourceTypeMapping[$resourceType] -notcontains $table) {
                        $resourceTypeMapping[$resourceType] += $table
                    }
                }
            }
            
            if ($Debug) {
                Write-Host "Debug:   Mapped $($resourceTypes.Count) ResourceTypes to $($tablesInSection.Count) tables" -ForegroundColor Green
                foreach ($rt in $resourceTypes) {
                    Write-Host "Debug:     $rt -> $($tablesInSection -join ', ')" -ForegroundColor DarkGray
                }
            }
        } elseif ($tablesInSection.Count -gt 0) {
            if ($Debug) {
                Write-Host "Debug:   Found $($tablesInSection.Count) tables but no ResourceType for section '$sectionTitle'" -ForegroundColor Yellow
            }
        }
        
        # Add delay between sections
        Start-Sleep -Milliseconds ($DelayMs / 4)
    }
    
    # Step 7: Add critical known mappings to ensure key ResourceTypes are covered
    $knownMappings = @{
        'Microsoft.Logic/workflows' = @('LogicAppWorkflowRuntime', 'LogicAppWorkflowEvent')
        'Microsoft.PlayFab/titles' = @('PFTitleAuditLogs')
        'Microsoft.OperationalInsights/workspaces' = @('LAQueryLogs', 'Usage')
        'Microsoft.Storage/storageAccounts' = @('StorageBlobLogs', 'StorageQueueLogs', 'StorageTableLogs')
        'Microsoft.Web/sites' = @('AppServiceHTTPLogs', 'AppServiceConsoleLogs')
        'Microsoft.Insights/dataCollectionRules' = @('DCRLogErrors', 'Usage')
    }
    
    foreach ($knownType in $knownMappings.Keys) {
        if (-not $resourceTypeMapping.ContainsKey($knownType)) {
            $resourceTypeMapping[$knownType] = $knownMappings[$knownType]
            if ($Debug) {
                Write-Host "Debug: Added known mapping: $knownType -> $($knownMappings[$knownType] -join ', ')" -ForegroundColor Cyan
            }
        }
    }
    
    # Step 8: Final cleanup and validation
    $finalMapping = @{}
    foreach ($resourceType in $resourceTypeMapping.Keys) {
        $validTables = $resourceTypeMapping[$resourceType] | Where-Object { Test-TableNameValidity -TableName $_ } | Sort-Object | Get-Unique
        if ($validTables.Count -gt 0) {
            $finalMapping[$resourceType] = $validTables
        }
    }
    
    Write-Host "  -> Successfully parsed $($finalMapping.Keys.Count) resource types using structured HTML parsing" -ForegroundColor Green
    
    if ($Debug) {
        Write-Host "Debug: Final mapping summary (first 10):" -ForegroundColor Gray
        $finalMapping.Keys | Sort-Object | Select-Object -First 10 | ForEach-Object {
            Write-Host "Debug:   $_ -> $($finalMapping[$_] -join ', ')" -ForegroundColor DarkGray
        }
    }
    
    return $finalMapping
}

function Initialize-ResourceTypeMapping {
    Write-Host "Initializing ResourceType-based table mapping with structured HTML parsing..." -ForegroundColor Yellow
    
    # Check cache first
    if (-not $RefreshCache -and (Test-Path $script:CacheFile)) {
        try {
            $cacheAge = (Get-Date) - (Get-Item $script:CacheFile).LastWriteTime
            if ($cacheAge.TotalHours -lt 24) {
                Write-Host "  -> Loading cached ResourceType mappings..." -ForegroundColor Green
                $cachedData = Get-Content $script:CacheFile | ConvertFrom-Json
                
                $validCacheData = $true
                if (-not $cachedData -or -not $cachedData.PSObject.Properties) {
                    $validCacheData = $false
                }
                
                if ($validCacheData) {
                    $script:ResourceTypeToTables = @{}
                    $cachedData.PSObject.Properties | ForEach-Object {
                        $validTables = $_.Value | Where-Object { Test-TableNameValidity -TableName $_ }
                        if ($validTables.Count -gt 0) {
                            $script:ResourceTypeToTables[$_.Name] = $validTables
                        }
                    }
                    
                    if ($script:ResourceTypeToTables.Keys.Count -gt 0) {
                        Write-Host "  -> Loaded and validated mappings for $($script:ResourceTypeToTables.Keys.Count) resource types" -ForegroundColor Green
                        return $true
                    }
                }
            } else {
                Write-Host "  -> Cache is stale, fetching fresh data..." -ForegroundColor Yellow
            }
        } catch {
            Write-Warning "Cache error: $($_.Exception.Message)"
        }
    }
    
    # Fetch fresh data
    try {
        Write-Host "  -> Fetching Microsoft documentation with structured parsing..." -ForegroundColor Yellow
        $tablesIndexUrl = "https://learn.microsoft.com/en-us/azure/azure-monitor/reference/tables-index"
        
        $response = Get-WebContent -Url $tablesIndexUrl
        
        if (-not $response) {
            Write-ValidationError "Could not fetch Microsoft documentation"
            return $false
        }
        
        # Parse using the new structured approach
        $script:ResourceTypeToTables = Get-ResourceTypeTableMapping -HtmlContent $response
        
        if ($script:ResourceTypeToTables.Keys.Count -eq 0) {
            Write-ValidationError "No ResourceType mappings extracted"
            return $false
        }
        
        # Cache results
        $script:ResourceTypeToTables | ConvertTo-Json -Depth 10 | Out-File $script:CacheFile -Encoding UTF8
        
        Write-Host "  -> Successfully mapped $($script:ResourceTypeToTables.Keys.Count) resource types" -ForegroundColor Green
        return $true
        
    } catch {
        Write-ValidationError "Failed to initialize ResourceType mapping: $($_.Exception.Message)"
        return $false
    }
}

function Get-LogAnalyticsTableName {
    param(
        [string]$CategoryName,
        [string]$ResourceType,
        [string]$CategoryType
    )
    
    # Handle metrics
    if ($CategoryType -eq "Metrics") {
        return "AzureMetrics"
    }
    
    # Validate inputs
    if ([string]::IsNullOrWhiteSpace($CategoryName) -or [string]::IsNullOrWhiteSpace($ResourceType)) {
        return "Unknown (invalid input)"
    }
    
    # Check cache
    $cacheKey = "$ResourceType|$CategoryName"
    if ($script:TableNameCache.ContainsKey($cacheKey)) {
        $cachedResult = $script:TableNameCache[$cacheKey]
        if (Test-TableNameValidity -TableName $cachedResult -or $cachedResult.StartsWith("Unknown")) {
            return $cachedResult
        } else {
            $script:TableNameCache.Remove($cacheKey)
            Write-ValidationError "Removed invalid cached result for '$cacheKey': '$cachedResult'"
        }
    }
    
    $result = "Unknown"
    
    if ($script:ResourceTypeToTables.ContainsKey($ResourceType)) {
        $availableTables = $script:ResourceTypeToTables[$ResourceType]
        
        if ($Debug) {
            Write-Host "Debug: Mapping $ResourceType category '$CategoryName'" -ForegroundColor DarkGray
            Write-Host "Debug:   Available tables: $($availableTables -join ', ')" -ForegroundColor DarkGray
        }
        
        # Exact match
        $exactMatch = $availableTables | Where-Object { $_ -eq $CategoryName }
        if ($exactMatch) {
            $result = $exactMatch
            if ($Debug) {
                Write-Host "Debug:   Exact match: $result" -ForegroundColor DarkGray
            }
        }
        # Contains match
        else {
            $containsMatches = $availableTables | Where-Object { $_ -like "*$CategoryName*" }
            if ($containsMatches.Count -eq 1) {
                $result = $containsMatches[0]
                if ($Debug) {
                    Write-Host "Debug:   Unique contains match: $result" -ForegroundColor DarkGray
                }
            } elseif ($containsMatches.Count -gt 1) {
                $result = $containsMatches | Sort-Object Length | Select-Object -First 1
                if ($Debug) {
                    Write-Host "Debug:   Best contains match: $result" -ForegroundColor DarkGray
                }
            } else {
                $result = "Unknown (category '$CategoryName' not found in tables for $ResourceType)"
                if ($Debug) {
                    Write-Host "Debug:   No match found" -ForegroundColor DarkGray
                }
            }
        }
    } else {
        $result = "Unknown (ResourceType '$ResourceType' not in documentation)"
        if ($Debug) {
            Write-Host "Debug: ResourceType '$ResourceType' not found" -ForegroundColor DarkGray
        }
    }
    
    # Validate and cache result
    if (Test-TableNameValidity -TableName $result -or $result.StartsWith("Unknown")) {
        $script:TableNameCache[$cacheKey] = $result
    } else {
        Write-ValidationError "Invalid mapping result for '$cacheKey': '$result'"
        $result = "Unknown (validation failed)"
        $script:TableNameCache[$cacheKey] = $result
    }
    
    Start-Sleep -Milliseconds ($DelayMs / 4)
    return $result
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

# Authentication
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

# Initialize mapping
if (-not $DisableTableLookup) {
    $mappingSuccess = Initialize-ResourceTypeMapping
} else {
    $mappingSuccess = $false
    Write-Host "Table lookup disabled" -ForegroundColor Yellow
}

# Validation summary
if ($script:ValidationErrors.Count -gt 0) {
    Write-Host ""
    Write-Host "Validation Issues Found:" -ForegroundColor Red
    $script:ValidationErrors | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
}

# Test problematic cases if Debug enabled
if ($Debug -and $mappingSuccess) {
    Write-Host ""
    Write-Host "Testing HTML structure parsing with problematic cases:" -ForegroundColor Yellow
    
    $testCases = @(
        @{ ResourceType = "Microsoft.PlayFab/titles"; Category = "AuditLogs"; Expected = "PFTitleAuditLogs" },
        @{ ResourceType = "Microsoft.OperationalInsights/workspaces"; Category = "Audit"; Expected = "LAQueryLogs or similar" },
        @{ ResourceType = "Microsoft.Logic/workflows"; Category = "WorkflowRuntime"; Expected = "LogicAppWorkflowRuntime" }
    )
    
    foreach ($test in $testCases) {
        Write-Host "Testing: $($test.ResourceType) + '$($test.Category)'" -ForegroundColor Cyan
        
        if ($script:ResourceTypeToTables.ContainsKey($test.ResourceType)) {
            Write-Host "  Available tables: $($script:ResourceTypeToTables[$test.ResourceType] -join ', ')" -ForegroundColor Gray
        } else {
            Write-Host "  ResourceType not found in mapping!" -ForegroundColor Red
        }
        
        $result = Get-LogAnalyticsTableName -CategoryName $test.Category -ResourceType $test.ResourceType -CategoryType "Logs"
        Write-Host "  Result: '$result'" -ForegroundColor White
        Write-Host "  Expected: $($test.Expected)" -ForegroundColor Gray
        Write-Host ""
    }
}

# Subscription selection
if ($SubscriptionId) {
    $selectedSubscription = Get-AzSubscription -SubscriptionId $SubscriptionId
} else {
    $selectedSubscription = Select-AzureSubscription
}

Set-AzContext -SubscriptionId $selectedSubscription.Id | Out-Null

Write-Host ""
Write-Host "Audit Settings:" -ForegroundColor Cyan
Write-Host "===============" -ForegroundColor Cyan
Write-Host "Subscription: $($selectedSubscription.Name)"
Write-Host "ResourceType mappings loaded: $($script:ResourceTypeToTables.Keys.Count)"
Write-Host "HTML parsing method: Structured <ul>/<li> parsing"
Write-Host "Delay between operations: $DelayMs ms"
Write-Host "Validation enabled: $Validate"
Write-Host ""

# Get and process resources
Write-Host "Getting all resources..." -ForegroundColor Yellow
$allResources = Get-AzResource
Write-Host "Found $($allResources.Count) total resources" -ForegroundColor Green

$allResults = @()
$processedCount = 0

function Get-DiagnosticInfo {
    param(
        [string]$ResourceId,
        [string]$ResourceName,
        [string]$ResourceType,
        [string]$ResourceGroup,
        [string]$SubscriptionName,
        [string]$SubscriptionId
    )
    
    $results = @()
    
    try {
        Start-Sleep -Milliseconds $DelayMs
        $categories = Get-AzDiagnosticSettingCategory -ResourceId $ResourceId -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
        
        if ($categories) {
            Start-Sleep -Milliseconds ($DelayMs / 2)
            $currentSettings = Get-AzDiagnosticSetting -ResourceId $ResourceId -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
            
            foreach ($category in $categories) {
                $enabled = $false
                if ($currentSettings) {
                    if ($category.CategoryType -eq "Logs") {
                        $setting = $currentSettings.Log | Where-Object { $_.Category -eq $category.Name -and $_.Enabled -eq $true } | Select-Object -First 1
                    } else {
                        $setting = $currentSettings.Metric | Where-Object { $_.Category -eq $category.Name -and $_.Enabled -eq $true } | Select-Object -First 1
                    }
                    $enabled = [bool]$setting
                }
                
                $tableName = if ($DisableTableLookup) {
                    "Unknown (lookup disabled)"
                } else {
                    Get-LogAnalyticsTableName -CategoryName $category.Name -ResourceType $ResourceType -CategoryType $category.CategoryType
                }
                
                # Final validation
                if ($Validate -and -not $tableName.StartsWith("Unknown") -and -not (Test-TableNameValidity -TableName $tableName)) {
                    Write-ValidationError "Invalid table name: '$tableName' for $ResourceType|$($category.Name)"
                    $tableName = "Unknown (validation failed)"
                }
                
                $results += [PSCustomObject]@{
                    SubscriptionName = $SubscriptionName
                    SubscriptionId = $SubscriptionId
                    ResourceName = $ResourceName
                    ResourceType = $ResourceType
                    ResourceGroup = $ResourceGroup
                    CategoryName = $category.Name
                    CategoryType = $category.CategoryType
                    LogAnalyticsTable = $tableName
                    Enabled = $enabled
                    ResourceId = $ResourceId
                }
                
                Start-Sleep -Milliseconds ($DelayMs / 8)
            }
        }
    } catch {
        if ($Debug) {
            Write-Host "Debug: Error processing $ResourceName : $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    
    return $results
}

foreach ($resource in $allResources) {
    $processedCount++
    Write-Progress -Activity "Processing Resources (HTML Structure Parser)" -Status "$processedCount of $($allResources.Count) - $($resource.Name)" -PercentComplete (($processedCount / $allResources.Count) * 100)
    
    $results = Get-DiagnosticInfo -ResourceId $resource.ResourceId -ResourceName $resource.Name -ResourceType $resource.ResourceType -ResourceGroup $resource.ResourceGroupName -SubscriptionName $selectedSubscription.Name -SubscriptionId $selectedSubscription.Id
    
    if ($results.Count -gt 0) {
        $allResults += $results
    }
    
    if ($processedCount % 10 -eq 0) {
        Write-Host "  Processed $processedCount resources, validation errors: $($script:ValidationErrors.Count)" -ForegroundColor Gray
    }
}

Write-Progress -Completed -Activity "Processing Resources"

# Export and analyze results
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$outputFile = "diagnostic_audit_html_structure_$timestamp.csv"
$allResults | Export-Csv -Path $outputFile -NoTypeInformation

# Check for corruption
$suspiciousResults = $allResults | Where-Object { 
    $_.LogAnalyticsTable.Length -le 2 -or
    ($_.LogAnalyticsTable -notmatch '^[A-Za-z][A-Za-z0-9_]*$' -and -not $_.LogAnalyticsTable.StartsWith("Unknown"))
}

Write-Host ""
Write-Host "Processing Complete - HTML Structure Parser Results:" -ForegroundColor Green
Write-Host "====================================================" -ForegroundColor Green
Write-Host "Resources processed: $processedCount"
Write-Host "Diagnostic categories found: $($allResults.Count)"
Write-Host "ResourceType mappings: $($script:ResourceTypeToTables.Keys.Count)"
Write-Host "Validation errors: $($script:ValidationErrors.Count)"
Write-Host "Suspicious results: $($suspiciousResults.Count)"

if ($suspiciousResults.Count -gt 0) {
    Write-Host ""
    Write-Host "⚠ SUSPICIOUS RESULTS (possible corruption):" -ForegroundColor Red
    $suspiciousResults | Select-Object ResourceType, CategoryName, LogAnalyticsTable | Format-Table -AutoSize
} else {
    Write-Host "✓ No suspicious single-character results detected" -ForegroundColor Green
}

# Show some key mappings
$logicResults = $allResults | Where-Object { $_.ResourceType -eq "Microsoft.Logic/workflows" }
if ($logicResults.Count -gt 0) {
    Write-Host ""
    Write-Host "Logic Apps Results (HTML Structure Parser):" -ForegroundColor Cyan
    $logicResults | Select-Object ResourceName, CategoryName, CategoryType, LogAnalyticsTable, Enabled | Format-Table -AutoSize
    
    $workflowRuntimeLogs = $logicResults | Where-Object { $_.CategoryName -eq "WorkflowRuntime" -and $_.CategoryType -eq "Logs" }
    if ($workflowRuntimeLogs.Count -gt 0) {
        $uniqueLogTables = $workflowRuntimeLogs | Select-Object -ExpandProperty LogAnalyticsTable | Sort-Object | Get-Unique
        if ($uniqueLogTables.Count -eq 1) {
            Write-Host "✓ WorkflowRuntime consistency: ALL map to $($uniqueLogTables[0])" -ForegroundColor Green
        } else {
            Write-Host "⚠ WorkflowRuntime inconsistency: $($uniqueLogTables -join ', ')" -ForegroundColor Red
        }
    }
}

Write-Host ""
Write-Host "Output saved to: $outputFile" -ForegroundColor Green
Write-Host "HTML parsing method: Structured <ul>/<li> tag parsing" -ForegroundColor Gray

if ($script:ValidationErrors.Count -gt 0) {
    Write-Host ""
    Write-Host "Validation Errors:" -ForegroundColor Red
    $script:ValidationErrors | Select-Object -Unique | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
}