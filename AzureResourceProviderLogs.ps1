# Azure Monitor Tables Index Scraper - v47 + ResourceType Extraction
# Extracts Categories -> Resource Providers -> Table Names + ResourceType (in documentation order)
# Based on working v47 pattern with simple ResourceType addition

param(
    [string]$BaseUrl = "https://learn.microsoft.com/en-us/azure/azure-monitor/reference",
    [string]$OutputFile = "AzureMonitorTables_Index_Structure.json",
    [string]$CategoryFilter = "",  # Optional: filter to specific category
    [switch]$Debug  # Enable debug output
)

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
    
    $contentToProcess = if ($mainContentMatch.Success) { 
        $mainContentMatch.Groups[1].Value 
    } else { 
        $HtmlContent 
    }
    
    if ($DebugMode) {
        Write-Host "Debug: Processing content section (length: $($contentToProcess.Length))" -ForegroundColor Gray
    }
    
    # Find all headings and their content in order
    $headingPattern = '<(h[2-6])[^>]*[^>]*id="([^"]*)"[^>]*>([^<]+)</\1>(.*?)(?=<h[2-6]|$)'
    $headingMatches = [regex]::Matches($contentToProcess, $headingPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    
    if ($DebugMode) {
        Write-Host "Debug: Found $($headingMatches.Count) heading sections" -ForegroundColor Gray
    }
    
    $currentCategory = ""
    $currentResourceProvider = ""
    
    foreach ($headingMatch in $headingMatches) {
        $headingLevel = $headingMatch.Groups[1].Value
        $headingId = $headingMatch.Groups[2].Value
        $headingText = $headingMatch.Groups[3].Value -replace '<[^>]+>', '' -replace '&amp;', '&' -replace '&nbsp;', ' ' -replace '\s+', ' '
        $headingText = $headingText.Trim()
        $sectionContent = $headingMatch.Groups[4].Value
        
        # Skip navigation headers
        if ($headingText -match "^(In this article|Feedback|Table of contents|Exit|Additional resources|Overview|Contents|See also)") {
            continue
        }
        
        if ($headingText.Length -lt 3 -or $headingText.Length -gt 100) {
            continue
        }
        
        if ($DebugMode) {
            Write-Host "Debug: Processing $headingLevel '$headingText'" -ForegroundColor Gray
        }
        
        # Category level (h2)
        if ($headingLevel -eq "h2") {
            $categoryName = $headingText -replace '\s*tables?\s*$', '' -replace '\s*logs?\s*$', ''
            $categoryName = $categoryName.Trim()
            
            # Apply category filter
            if ($FilterCategory -and $categoryName -ne $FilterCategory) {
                if ($DebugMode) {
                    Write-Host "Debug: Skipping category '$categoryName' (not matching filter '$FilterCategory')" -ForegroundColor DarkGray
                }
                continue
            }
            
            $currentCategory = $categoryName
            $currentResourceProvider = ""
            
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
    
    return $categories
}

# Main script execution
Write-Host "============================================" -ForegroundColor Green
Write-Host "Azure Monitor Tables Index Scraper" -ForegroundColor Green  
Write-Host "============================================" -ForegroundColor Green
Write-Host ""

Write-Host "Mode: Structure Only (Table Names + ResourceType)" -ForegroundColor Yellow

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
Write-Host "Export completed successfully!" -ForegroundColor Green
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