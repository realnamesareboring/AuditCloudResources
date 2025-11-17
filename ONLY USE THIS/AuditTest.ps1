# Azure Monitor Tables Index Scraper - v53 + Audit Mode
# Extracts Categories -> Resource Providers -> Table Names + ResourceType (in documentation order)
# Based on working v53 pattern with added audit functionality

param(
    # Original JSON Generation Parameters (unchanged)
    [string]$BaseUrl = "https://learn.microsoft.com/en-us/azure/azure-monitor/reference",
    [string]$OutputFile = "AzureMonitorTables_Index_Structure.json",
    [string]$CategoryFilter = "",  # Optional: filter to specific category
    [switch]$Debug,  # Enable debug output
    
    # New Audit Parameters
    [switch]$RunAudit,           # Enable audit mode after JSON generation
    [switch]$AuditOnly,          # Skip JSON generation, only run audit
    [string]$SubscriptionId,     # Specific Azure subscription
    [int]$DelayMs = 500         # Delay between Azure API calls
)

# Global variables for audit mode
$script:ResourceTypeToTables = @{}
$script:TableNameCache = @{}

# =====================================
# ORIGINAL JSON GENERATION FUNCTIONS (UNCHANGED)
# =====================================

# Function to fetch and parse a web page
function Get-WebContent {
    param([string]$Url)
    
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -ErrorAction Stop
        return $response.Content
    }
    catch {
        Write-Warning "Failed to fetch $Url : $_"
        return $null
    }
}

# Function to extract ResourceType from section content
function Get-ResourceTypeFromContent {
    param([string]$Content)
    
    # Look for <p>Microsoft.Something/something</p> pattern (case-insensitive)
    $resourceTypePattern = '<p>[Mm]icrosoft\.[^<]+</p>'
    $resourceTypeMatch = [regex]::Match($Content, $resourceTypePattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    
    if ($resourceTypeMatch.Success) {
        # Extract just the Microsoft.* part without the <p> tags
        $extracted = $resourceTypeMatch.Value -replace '</?p>', ''
        return $extracted.Trim()
    }
    
    return ""
}

# Function to extract categories and tables from tables-index page
function Get-CategoryTables {
    param(
        [string]$HtmlContent,
        [switch]$DebugMode,
        [string]$FilterCategory = ""
    )
    
    $categories = [ordered]@{}
    
    # Remove script and style tags
    $HtmlContent = $HtmlContent -replace '<script[^>]*>.*?</script>', ''
    $HtmlContent = $HtmlContent -replace '<style[^>]*>.*?</style>', ''
    
    if ($DebugMode) {
        Write-Host "Debug: Starting to parse tables-index page" -ForegroundColor Gray
        $HtmlContent | Out-File -FilePath "debug_tables_index_raw.html" -Encoding UTF8
        Write-Host "Debug: Saved raw HTML to debug_tables_index_raw.html" -ForegroundColor Gray
        
        # Count all table links first
        $allTableLinks = [regex]::Matches($HtmlContent, '<a\s+[^>]*href="tables/([^"]+)"[^>]*>([^<]+)</a>')
        Write-Host "Debug: Found $($allTableLinks.Count) total table links in entire page" -ForegroundColor Gray
    }
    
    # Look for main content section
    $mainContentPattern = '<main[^>]*>(.*?)</main>'
    $mainContentMatch = [regex]::Match($HtmlContent, $mainContentPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    
    if ($mainContentMatch.Success) {
        $mainContent = $mainContentMatch.Groups[1].Value
    } else {
        $mainContent = $HtmlContent
        if ($DebugMode) {
            Write-Host "Debug: Could not find <main> section, using entire HTML" -ForegroundColor Yellow
        }
    }
    
    # Split content into sections based on headings
    $sectionPattern = '(<h[2-6][^>]*>.*?(?=<h[2-6][^>]*>|$))'
    $sectionMatches = [regex]::Matches($mainContent, $sectionPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    
    $currentCategory = ""
    $currentResourceProvider = ""
    
    if ($DebugMode) {
        Write-Host "Debug: Found $($sectionMatches.Count) sections to process" -ForegroundColor Gray
    }
    
    foreach ($sectionMatch in $sectionMatches) {
        $sectionContent = $sectionMatch.Groups[1].Value
        
        # Extract heading info
        $headingPattern = '<h([2-6])[^>]*>([^<]+)</h[2-6]>'
        $headingMatch = [regex]::Match($sectionContent, $headingPattern)
        
        if ($headingMatch.Success) {
            $headingLevel = "h$($headingMatch.Groups[1].Value)"
            $headingText = $headingMatch.Groups[2].Value.Trim()
            
            # Category level (h2)
            if ($headingLevel -eq "h2") {
                $currentCategory = $headingText
                $currentResourceProvider = ""
                
                # Apply category filter if specified
                if ($FilterCategory -and $currentCategory -ne $FilterCategory) {
                    $currentCategory = ""
                    continue
                }
                
                if (-not $categories.Contains($currentCategory)) {
                    $categories[$currentCategory] = [ordered]@{}
                }
                
                if ($DebugMode) {
                    Write-Host "Debug: >>> CATEGORY - $currentCategory" -ForegroundColor Yellow
                }
            }
            # Resource Provider level (h3/h4/h5/h6)
            elseif ($headingLevel -match "h[3-6]" -and $currentCategory) {
                $resourceProviderName = $headingText -replace '\s*tables?\s*$', '' -replace '\s*logs?\s*$', ''
                $resourceProviderName = $resourceProviderName.Trim()
                
                $currentResourceProvider = $resourceProviderName
                
                if (-not $categories[$currentCategory].Contains($currentResourceProvider)) {
                    $categories[$currentCategory][$currentResourceProvider] = @{
                        ResourceType = ""
                        Tables = @()
                    }
                }
                
                if ($DebugMode) {
                    Write-Host "Debug:   >> RESOURCE PROVIDER - $currentResourceProvider" -ForegroundColor Cyan
                }
            }
            
            # Look for table links and ResourceType in this section content
            if ($currentCategory) {
                $tableLinks = @()
                
                # Extract ResourceType from section content
                $resourceTypeFound = Get-ResourceTypeFromContent -Content $sectionContent
                if ($resourceTypeFound -and $DebugMode) {
                    Write-Host "Debug:       > RESOURCE TYPE - $resourceTypeFound" -ForegroundColor Magenta
                }
                
                # Pattern for table links
                $tableLinkPattern = '<a\s+[^>]*href="tables/([^"]+)"[^>]*>([^<]+)</a>'
                $linkMatches = [regex]::Matches($sectionContent, $tableLinkPattern)
                
                foreach ($linkMatch in $linkMatches) {
                    $tableUrl = $linkMatch.Groups[1].Value.Trim()
                    $tableName = $linkMatch.Groups[2].Value.Trim()
                    
                    if ($tableName -and $tableUrl) {
                        $tableLinks += [PSCustomObject]@{
                            Name = $tableName
                            Url = "$BaseUrl/tables/$tableUrl"
                        }
                        
                        if ($DebugMode) {
                            Write-Host "Debug:       > TABLE - $tableName" -ForegroundColor Green
                        }
                    }
                }
                
                # Also check for markdown-style links
                $mdTablePattern = '\[([^\]]+)\]\(tables/([^\)]+)\)'
                $mdMatches = [regex]::Matches($sectionContent, $mdTablePattern)
                
                foreach ($mdMatch in $mdMatches) {
                    $tableName = $mdMatch.Groups[1].Value.Trim()
                    $tableUrl = $mdMatch.Groups[2].Value.Trim()
                    
                    if ($tableName -and $tableUrl) {
                        $tableLinks += [PSCustomObject]@{
                            Name = $tableName
                            Url = "$BaseUrl/tables/$tableUrl"
                        }
                        
                        if ($DebugMode) {
                            Write-Host "Debug:       > TABLE (MD) - $tableName" -ForegroundColor Green
                        }
                    }
                }
                
                # Add tables to appropriate location
                if ($tableLinks.Count -gt 0) {
                    if ($currentResourceProvider) {
                        # Add to current resource provider
                        $categories[$currentCategory][$currentResourceProvider].Tables += $tableLinks
                        
                        # Update ResourceType if found
                        if ($resourceTypeFound) {
                            $categories[$currentCategory][$currentResourceProvider].ResourceType = $resourceTypeFound
                        }
                        
                        if ($DebugMode) {
                            Write-Host "Debug:     Added $($tableLinks.Count) tables to $currentCategory -> $currentResourceProvider" -ForegroundColor Green
                        }
                    } else {
                        # Add directly to category
                        if (-not $categories[$currentCategory].Contains("Uncategorized")) {
                            $categories[$currentCategory]["Uncategorized"] = @{
                                ResourceType = ""
                                Tables = @()
                            }
                        }
                        $categories[$currentCategory]["Uncategorized"].Tables += $tableLinks
                        
                        # Update ResourceType if found
                        if ($resourceTypeFound) {
                            $categories[$currentCategory]["Uncategorized"].ResourceType = $resourceTypeFound
                        }
                        
                        if ($DebugMode) {
                            Write-Host "Debug:     Added $($tableLinks.Count) tables to $currentCategory -> Uncategorized" -ForegroundColor Yellow
                        }
                    }
                }
            }
        }
    }
    
    return $categories
}

# =====================================
# NEW AUDIT FUNCTIONS
# =====================================

function Initialize-AuditMapping {
    param([string]$JsonFile)
    
    if (-not (Test-Path $JsonFile)) {
        Write-Error "JSON file not found: $JsonFile"
        return $false
    }
    
    try {
        Write-Host "  -> Loading JSON mapping from: $JsonFile" -ForegroundColor Cyan
        $jsonData = Get-Content $JsonFile -Raw | ConvertFrom-Json
        
        $script:ResourceTypeToTables = @{}
        
        # Build ResourceType -> Tables lookup from JSON
        foreach ($categoryName in $jsonData.Categories.PSObject.Properties.Name) {
            $category = $jsonData.Categories.$categoryName
            
            foreach ($providerName in $category.PSObject.Properties.Name) {
                $provider = $category.$providerName
                
                if ($provider.ResourceType -and $provider.ResourceType.ToString().Length -gt 5) {
                    $resourceType = $provider.ResourceType.ToString()
                    
                    if (-not $script:ResourceTypeToTables.ContainsKey($resourceType)) {
                        $script:ResourceTypeToTables[$resourceType] = @()
                    }
                    
                    if ($provider.Tables -and $provider.Tables.Count -gt 0) {
                        foreach ($table in $provider.Tables) {
                            if ($table.TableName -and $table.TableName.ToString().Length -gt 2) {
                                $tableName = $table.TableName.ToString()
                                # Ensure we don't add duplicates and the table name is valid
                                if ($script:ResourceTypeToTables[$resourceType] -notcontains $tableName) {
                                    $script:ResourceTypeToTables[$resourceType] += $tableName
                                    
                                    if ($Debug) {
                                        Write-Host "Debug: Added table '$tableName' to ResourceType '$resourceType'" -ForegroundColor DarkGray
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Write-Host "  -> Successfully loaded mappings for $($script:ResourceTypeToTables.Keys.Count) resource types" -ForegroundColor Green
        
        if ($Debug) {
            Write-Host "Debug: Sample mappings:" -ForegroundColor Gray
            $script:ResourceTypeToTables.Keys | Sort-Object | Select-Object -First 3 | ForEach-Object {
                $tables = $script:ResourceTypeToTables[$_] -join ', '
                Write-Host "Debug:   $_ -> $tables" -ForegroundColor DarkGray
            }
        }
        
        return $true
        
    } catch {
        Write-Error "Failed to load JSON: $($_.Exception.Message)"
        return $false
    }
}

function Get-TableNameFromMapping {
    param(
        [string]$ResourceType,
        [string]$CategoryName,
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
        # Ensure we return a string, not a character
        return [string]$cachedResult
    }
    
    $result = "Unknown"
    
    if ($script:ResourceTypeToTables.ContainsKey($ResourceType)) {
        $availableTables = $script:ResourceTypeToTables[$ResourceType]
        
        # Ensure availableTables is an array of strings
        if ($availableTables -and $availableTables.Count -gt 0) {
            # Convert to string array to avoid character issues
            $tableArray = @()
            foreach ($table in $availableTables) {
                if ($table -and $table.ToString().Length -gt 2) {
                    $tableArray += $table.ToString()
                }
            }
            
            if ($Debug) {
                Write-Host "Debug: Mapping $ResourceType -> '$CategoryName'" -ForegroundColor DarkGray
                Write-Host "Debug:   Available: $($tableArray -join ', ')" -ForegroundColor DarkGray
            }
            
            # Exact match
            $exactMatch = $tableArray | Where-Object { $_.ToString() -eq $CategoryName }
            if ($exactMatch) {
                $result = $exactMatch.ToString()
                if ($Debug) { Write-Host "Debug:   Exact match: $result" -ForegroundColor DarkGreen }
            }
            # Case-insensitive exact match
            elseif (-not $exactMatch) {
                $exactMatchCI = $tableArray | Where-Object { $_.ToString().ToLower() -eq $CategoryName.ToLower() }
                if ($exactMatchCI) {
                    $result = $exactMatchCI.ToString()
                    if ($Debug) { Write-Host "Debug:   Case-insensitive match: $result" -ForegroundColor DarkGreen }
                }
            }
            # Contains match
            if ($result -eq "Unknown") {
                $containsMatches = $tableArray | Where-Object { $_.ToString() -like "*$CategoryName*" }
                if ($containsMatches.Count -eq 1) {
                    $result = $containsMatches[0].ToString()
                    if ($Debug) { Write-Host "Debug:   Contains match: $result" -ForegroundColor DarkGreen }
                } elseif ($containsMatches.Count -gt 1) {
                    $result = ($containsMatches | Sort-Object Length | Select-Object -First 1).ToString()
                    if ($Debug) { Write-Host "Debug:   Best contains match: $result" -ForegroundColor DarkGreen }
                }
            }
            # Reverse contains
            if ($result -eq "Unknown") {
                $reverseMatches = $tableArray | Where-Object { $CategoryName -like "*$($_.ToString())*" }
                if ($reverseMatches.Count -eq 1) {
                    $result = $reverseMatches[0].ToString()
                    if ($Debug) { Write-Host "Debug:   Reverse match: $result" -ForegroundColor DarkGreen }
                }
            }
        }
        
        if ($result -eq "Unknown") {
            $result = "Unknown (category '$CategoryName' not found for $ResourceType)"
            if ($Debug) { Write-Host "Debug:   No match found" -ForegroundColor DarkRed }
        }
    } else {
        $result = "Unknown (ResourceType '$ResourceType' not in documentation)"
        if ($Debug) { Write-Host "Debug:   ResourceType not found: $ResourceType" -ForegroundColor DarkRed }
    }
    
    # Ensure result is a proper string
    $result = [string]$result
    
    # Cache result
    $script:TableNameCache[$cacheKey] = $result
    return $result
}

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
                
                # Get table name using JSON mapping
                $tableName = Get-TableNameFromMapping -ResourceType $ResourceType -CategoryName $category.Name -CategoryType $category.CategoryType
                
                # Ensure tableName is a proper string
                $tableName = [string]$tableName
                if ([string]::IsNullOrWhiteSpace($tableName) -or $tableName.Length -le 2) {
                    $tableName = "Unknown (mapping failed)"
                    if ($Debug) {
                        Write-Host "Debug: Mapping failed for $ResourceType -> $($category.Name)" -ForegroundColor Red
                    }
                }
                
                $results += [PSCustomObject]@{
                    SubscriptionName = [string]$SubscriptionName
                    SubscriptionId = [string]$SubscriptionId
                    ResourceName = [string]$ResourceName
                    ResourceType = [string]$ResourceType
                    ResourceGroup = [string]$ResourceGroup
                    CategoryName = [string]$category.Name
                    CategoryType = [string]$category.CategoryType
                    LogAnalyticsTable = [string]$tableName
                    Enabled = [bool]$enabled
                    ResourceId = [string]$ResourceId
                }
            }
        }
    } catch {
        if ($Debug) {
            Write-Host "Debug: Error processing $ResourceName : $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    
    return $results
}

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
    for ($i = 0; $i -lt $subscriptions.Count; $i++) {
        Write-Host "$($i + 1). $($subscriptions[$i].Name) ($($subscriptions[$i].Id))" -ForegroundColor White
    }
    
    Write-Host ""
    do {
        $selection = Read-Host "Please select a subscription (1-$($subscriptions.Count))"
        $selectedIndex = [int]$selection - 1
    } while ($selectedIndex -lt 0 -or $selectedIndex -ge $subscriptions.Count)
    
    return $subscriptions[$selectedIndex]
}

function Invoke-DiagnosticAudit {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "AUDIT MODE: Diagnostic Settings Analysis" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    
    # Check Azure authentication
    try {
        $currentContext = Get-AzContext
        if (-not $currentContext) {
            Write-Host "Please authenticate to Azure..." -ForegroundColor Yellow
            Connect-AzAccount
        }
    } catch {
        Write-Host "Please authenticate to Azure..." -ForegroundColor Yellow
        Connect-AzAccount
    }
    
    # Initialize mapping from JSON
    if (-not (Initialize-AuditMapping -JsonFile $OutputFile)) {
        Write-Error "Failed to initialize audit mapping from JSON file."
        return
    }
    
    # Select subscription
    if ($SubscriptionId) {
        $selectedSubscription = Get-AzSubscription -SubscriptionId $SubscriptionId
    } else {
        $selectedSubscription = Select-AzureSubscription
    }
    
    Set-AzContext -SubscriptionId $selectedSubscription.Id | Out-Null
    
    Write-Host ""
    Write-Host "Audit Configuration:" -ForegroundColor Cyan
    Write-Host "Subscription: $($selectedSubscription.Name)" -ForegroundColor White
    Write-Host "ResourceType mappings: $($script:ResourceTypeToTables.Keys.Count)" -ForegroundColor White
    Write-Host "Delay between calls: $DelayMs ms" -ForegroundColor White
    Write-Host ""
    
    # Get all resources
    Write-Host "Getting all resources..." -ForegroundColor Yellow
    $allResources = Get-AzResource
    Write-Host "Found $($allResources.Count) total resources" -ForegroundColor Green
    
    # Process resources
    $allResults = @()
    $processedCount = 0
    
    foreach ($resource in $allResources) {
        $processedCount++
        Write-Progress -Activity "Processing Resources for Diagnostic Audit" -Status "$processedCount of $($allResources.Count) - $($resource.Name)" -PercentComplete (($processedCount / $allResources.Count) * 100)
        
        $results = Get-DiagnosticInfo -ResourceId $resource.ResourceId -ResourceName $resource.Name -ResourceType $resource.ResourceType -ResourceGroup $resource.ResourceGroupName -SubscriptionName $selectedSubscription.Name -SubscriptionId $selectedSubscription.Id
        
        if ($results.Count -gt 0) {
            $allResults += $results
        }
        
        if ($processedCount % 50 -eq 0) {
            Write-Host "  Processed $processedCount resources..." -ForegroundColor Gray
        }
    }
    
    Write-Progress -Completed -Activity "Processing Resources"
    
    # Export results
    $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
    $outputCsv = "diagnostic_audit_$timestamp.csv"
    $allResults | Export-Csv -Path $outputCsv -NoTypeInformation
    
    # Results summary - fix string handling issues
    $authoritativeMappings = $allResults | Where-Object { 
        $table = [string]$_.LogAnalyticsTable
        $table -and $table.Length -gt 2 -and -not $table.StartsWith("Unknown")
    }
    $unknownMappings = $allResults | Where-Object { 
        $table = [string]$_.LogAnalyticsTable
        -not $table -or $table.Length -le 2 -or $table.StartsWith("Unknown")
    }
    
    Write-Host ""
    Write-Host "Audit Complete!" -ForegroundColor Green
    Write-Host "===============" -ForegroundColor Green
    Write-Host "Resources processed: $processedCount"
    Write-Host "Diagnostic categories found: $($allResults.Count)"
    Write-Host "Successfully mapped: $($authoritativeMappings.Count) ($(($authoritativeMappings.Count / $allResults.Count * 100).ToString('N1'))%)"
    Write-Host "Unknown mappings: $($unknownMappings.Count) ($(($unknownMappings.Count / $allResults.Count * 100).ToString('N1'))%)"
    Write-Host ""
    Write-Host "CSV output saved to: $outputCsv" -ForegroundColor Green
}

# =====================================
# MAIN SCRIPT EXECUTION
# =====================================

Write-Host "============================================" -ForegroundColor Green
Write-Host "Azure Monitor Tables Scraper + Audit Tool" -ForegroundColor Green  
Write-Host "============================================" -ForegroundColor Green
Write-Host ""

# Determine what to run
$shouldGenerateJson = -not $AuditOnly
$shouldRunAudit = $RunAudit -or $AuditOnly

if ($shouldGenerateJson) {
    Write-Host "Mode: JSON Generation (Table Names + ResourceType)" -ForegroundColor Yellow

    if ($CategoryFilter) {
        Write-Host "Category Filter: $CategoryFilter" -ForegroundColor Cyan
    } else {
        Write-Host "Category Filter: None (all categories)" -ForegroundColor Cyan
    }

    Write-Host "Output file: $OutputFile" -ForegroundColor Cyan
    Write-Host "Documentation order: PRESERVED" -ForegroundColor Green
    Write-Host ""

    # Fetch the main tables index page
    Write-Host "Fetching tables index page..." -ForegroundColor Yellow
    $indexUrl = "$BaseUrl/tables-index"
    $htmlContent = Get-WebContent -Url $indexUrl

    if (-not $htmlContent) {
        Write-Error "Failed to fetch the tables index page. Exiting."
        exit 1
    }

    Write-Host "Successfully fetched tables index page ($([math]::Round($htmlContent.Length / 1KB, 0)) KB)." -ForegroundColor Green
    Write-Host ""

    # Extract categories and tables
    Write-Host "Extracting table information (preserving documentation order)..." -ForegroundColor Yellow

    $categoryTables = Get-CategoryTables -HtmlContent $htmlContent -DebugMode:$Debug -FilterCategory $CategoryFilter

    if ($Debug) {
        Write-Host "Debug: Found $($categoryTables.Keys.Count) categories" -ForegroundColor Gray
        foreach ($cat in $categoryTables.Keys) {
            $totalTables = 0
            foreach ($rp in $categoryTables[$cat].Keys) {
                if ($categoryTables[$cat][$rp].Tables) {
                    $tableCount = $categoryTables[$cat][$rp].Tables.Count
                } else {
                    $tableCount = 0
                }
                $totalTables += $tableCount
            }
            Write-Host "  - $cat has $totalTables tables across $($categoryTables[$cat].Keys.Count) resource providers" -ForegroundColor Gray
        }
    }

    # If no categories found, exit
    if ($categoryTables.Keys.Count -eq 0) {
        Write-Error "No categories found. Check the HTML structure or try with -Debug to see what was parsed."
        exit 1
    }

    # Build the result object
    $result = @{
        GeneratedDate = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        SourceUrl = $indexUrl
        DocumentationOrderPreserved = $true
        StructureOnly = $true
        Categories = [ordered]@{}
    }

    $totalTablesFound = 0

    # Process each category in documentation order
    foreach ($categoryName in $categoryTables.Keys) {
        
        Write-Host "Processing category: $categoryName" -ForegroundColor Magenta
        $result.Categories[$categoryName] = [ordered]@{}
        
        $categoryData = $categoryTables[$categoryName]
        
        foreach ($resourceProviderName in $categoryData.Keys) {
            $resourceProviderData = $categoryData[$resourceProviderName]
            $tables = $resourceProviderData.Tables
            $resourceType = $resourceProviderData.ResourceType
            $tableCount = $tables.Count
            $totalTablesFound += $tableCount
            
            if ($resourceType) {
                Write-Host "  $resourceProviderName [$resourceType] - $tableCount tables" -ForegroundColor Cyan
            } else {
                Write-Host "  $resourceProviderName - $tableCount tables" -ForegroundColor Cyan
            }
            
            $result.Categories[$categoryName][$resourceProviderName] = [ordered]@{
                ResourceType = $resourceType
                Tables = @()
            }
            
            foreach ($table in $tables) {
                $result.Categories[$categoryName][$resourceProviderName].Tables += [PSCustomObject]@{
                    TableName = $table.Name
                    Url = $table.Url
                }
            }
            
            # Show first few table names for verification
            if ($Debug -and $tableCount -gt 0) {
                $sampleCount = [Math]::Min(3, $tableCount)
                $sampleNames = ($tables[0..($sampleCount-1)] | ForEach-Object { $_.Name }) -join ', '
                if ($tableCount -gt 3) {
                    Write-Host "    Sample tables: $sampleNames ..." -ForegroundColor DarkGray
                } else {
                    Write-Host "    Sample tables: $sampleNames" -ForegroundColor DarkGray
                }
                if ($resourceType) {
                    Write-Host "    ResourceType: $resourceType" -ForegroundColor DarkGray
                }
            }
        }
        Write-Host ""
    }

    # Export to JSON
    Write-Host "Exporting to JSON..." -ForegroundColor Yellow

    # Ensure we completely overwrite any existing file
    if (Test-Path $OutputFile) {
        Remove-Item $OutputFile -Force
    }

    $jsonOutput = $result | ConvertTo-Json -Depth 10
    $jsonOutput | Out-File -FilePath $OutputFile -Encoding UTF8 -Force

    # Verify the export
    $fileSize = (Get-Item $OutputFile).Length
    Write-Host "JSON file created successfully ($([math]::Round($fileSize / 1KB, 1)) KB)" -ForegroundColor Green

    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "JSON Generation completed successfully!" -ForegroundColor Green
    Write-Host "Output file: $OutputFile" -ForegroundColor Green
    Write-Host "Source: tables-index page" -ForegroundColor Green
    Write-Host "Documentation order preserved: YES" -ForegroundColor Green
    Write-Host "Total categories: $($result.Categories.Count)" -ForegroundColor Green
    Write-Host "Total tables found: $totalTablesFound" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green

    if ($Debug) {
        Write-Host ""
        Write-Host "DEBUG SUMMARY:" -ForegroundColor Red
        Write-Host "Structure: Categories -> Resource Providers -> Table Names + ResourceType" -ForegroundColor Yellow
        Write-Host "No column details included for faster processing" -ForegroundColor Yellow
        Write-Host "Check debug_tables_index_raw.html for raw HTML" -ForegroundColor Yellow
    }
}

# Run audit if requested
if ($shouldRunAudit) {
    Invoke-DiagnosticAudit
}

# Final usage summary
if (-not $shouldRunAudit -and -not $AuditOnly) {
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Cyan
    Write-Host "  Run audit: .\AzureResourceProviderLogs.ps1 -RunAudit" -ForegroundColor Yellow
    Write-Host "  Audit only: .\AzureResourceProviderLogs.ps1 -AuditOnly" -ForegroundColor Yellow
}