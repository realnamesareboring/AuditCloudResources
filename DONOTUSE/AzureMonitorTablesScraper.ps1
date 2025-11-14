# Azure Monitor Tables Reference Scraper
# This script fetches Azure Monitor table information and exports to JSON

param(
    [string]$BaseUrl = "https://learn.microsoft.com/en-us/azure/azure-monitor/reference",
    [string]$OutputFile = "AzureMonitorTables_Security.json",
    [string]$CategoryFilter = "Security",  # Default to Security category only
    [switch]$Debug,  # Enable debug output
    [switch]$AllCategories  # Set this flag to process all categories instead of just Security
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

# Function to extract columns from a table detail page
function Get-TableColumns {
    param(
        [string]$TableUrl,
        [string]$TableName
    )
    
    Write-Host "Processing table: $TableName" -ForegroundColor Cyan
    
    $htmlContent = Get-WebContent -Url $TableUrl
    if (-not $htmlContent) {
        return @()
    }
    
    $columns = @()
    
    # Look for the table with aria-label="Columns" or any table with Column/Type/Description headers
    # The structure is: <table><thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead><tbody>...
    
    # Strategy 1: Find table with aria-label="Columns"
    $tablePattern = '<table[^>]*aria-label="Columns"[^>]*>(.*?)</table>'
    $tableMatch = [regex]::Match($htmlContent, $tablePattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    
    if (-not $tableMatch.Success) {
        # Strategy 2: Find any table that has Column/Type/Description headers
        $allTablesPattern = '<table[^>]*>(.*?)</table>'
        $allTableMatches = [regex]::Matches($htmlContent, $allTablesPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
        
        foreach ($tblMatch in $allTableMatches) {
            $tableContent = $tblMatch.Groups[1].Value
            # Check if this table has the right headers
            if ($tableContent -match '<th[^>]*>Column</th>' -and $tableContent -match '<th[^>]*>Type</th>' -and $tableContent -match '<th[^>]*>Description</th>') {
                $tableMatch = $tblMatch
                break
            }
        }
    }
    
    if ($tableMatch.Success) {
        $tableContent = $tableMatch.Groups[1].Value
        
        # Extract tbody content
        $tbodyPattern = '<tbody[^>]*>(.*?)</tbody>'
        $tbodyMatch = [regex]::Match($tableContent, $tbodyPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
        
        if ($tbodyMatch.Success) {
            $tbody = $tbodyMatch.Groups[1].Value
            
            # Extract each row
            $rowPattern = '<tr[^>]*>(.*?)</tr>'
            $rowMatches = [regex]::Matches($tbody, $rowPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
            
            foreach ($rowMatch in $rowMatches) {
                $rowContent = $rowMatch.Groups[1].Value
                
                # Extract all <td> cells from this row
                $cellPattern = '<td[^>]*>(.*?)</td>'
                $cellMatches = [regex]::Matches($rowContent, $cellPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
                
                if ($cellMatches.Count -ge 3) {
                    # First cell is column name, second is type, third is description
                    $columnName = $cellMatches[0].Groups[1].Value -replace '<[^>]+>', '' -replace '&nbsp;', ' ' -replace '&amp;', '&' -replace '\s+', ' '
                    $columnType = $cellMatches[1].Groups[1].Value -replace '<[^>]+>', '' -replace '&nbsp;', ' ' -replace '&amp;', '&' -replace '\s+', ' '
                    $description = $cellMatches[2].Groups[1].Value -replace '<[^>]+>', '' -replace '&nbsp;', ' ' -replace '&amp;', '&' -replace '\s+', ' '
                    
                    $columnName = $columnName.Trim()
                    $columnType = $columnType.Trim()
                    $description = $description.Trim()
                    
                    if ($columnName -and $columnName -ne "Column") {
                        $columns += [PSCustomObject]@{
                            Name = $columnName
                            Type = $columnType
                            Description = $description
                        }
                    }
                }
            }
        }
    }
    
    return $columns
}

# Function to extract categories and tables
function Get-CategoryTables {
    param(
        [string]$HtmlContent,
        [switch]$DebugMode,
        [string]$FilterCategory = ""
    )
    
    $categories = @{}
    
    # Remove script and style tags
    $HtmlContent = $HtmlContent -replace '<script[^>]*>.*?</script>', ''
    $HtmlContent = $HtmlContent -replace '<style[^>]*>.*?</style>', ''
    
    # NEW Strategy 0: Direct extraction of <a href="tables/..."> links grouped by category
    # This is the actual structure shown in the HTML
    if ($DebugMode) {
        Write-Host "Debug: Trying direct <a href='tables/'> link extraction" -ForegroundColor Gray
    }
    
    # First, let's find all <a href="tables/..."> links and their surrounding context
    $linkPattern = '<a\s+[^>]*href="tables/([^"]+)"[^>]*>([^<]+)</a>'
    $allLinkMatches = [regex]::Matches($HtmlContent, $linkPattern)
    
    if ($DebugMode) {
        Write-Host "Debug: Found $($allLinkMatches.Count) total table links in HTML" -ForegroundColor Gray
    }
    
    # Now we need to associate these links with their categories
    # Look for category headers (h2/h3) and collect links until the next header
    $sectionPattern = '<h[23][^>]*[^>]*id="([^"]*)"[^>]*>([^<]+)</h[23]>(.*?)(?=<h[23]|$)'
    $sectionMatches = [regex]::Matches($HtmlContent, $sectionPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    
    if ($DebugMode) {
        Write-Host "Debug: Found $($sectionMatches.Count) header sections" -ForegroundColor Gray
    }
    
    foreach ($sectionMatch in $sectionMatches) {
        $categoryName = $sectionMatch.Groups[2].Value -replace '<[^>]+>', '' -replace '&amp;', '&' -replace '&nbsp;', ' '
        $categoryName = $categoryName.Trim()
        $sectionContent = $sectionMatch.Groups[3].Value
        
        # Skip navigation/utility headers
        if ($categoryName -match "^(In this article|Feedback|Table of contents|Exit|Additional resources)") {
            continue
        }
        
        if ($categoryName.Length -lt 3 -or $categoryName.Length -gt 50) {
            continue
        }
        
        # Early filtering - if CategoryFilter is set, only process that category
        if ($CategoryFilter -and $categoryName -ne $CategoryFilter) {
            if ($DebugMode) {
                Write-Host "Debug: Skipping category '$categoryName' (not matching filter '$CategoryFilter')" -ForegroundColor DarkGray
            }
            continue
        }
        
        if (-not $categories.ContainsKey($categoryName)) {
            $categories[$categoryName] = @()
        }
        
        # Find all table links in this section
        $sectionLinks = [regex]::Matches($sectionContent, $linkPattern)
        
        if ($DebugMode -and $sectionLinks.Count -gt 0) {
            Write-Host "Debug: Category '$categoryName' has $($sectionLinks.Count) tables" -ForegroundColor Gray
        }
        
        foreach ($linkMatch in $sectionLinks) {
            $tableUrl = $linkMatch.Groups[1].Value
            $tableName = $linkMatch.Groups[2].Value.Trim()
            
            if ($DebugMode -and $categories[$categoryName].Count -lt 3) {
                Write-Host "Debug:   - $tableName ($tableUrl)" -ForegroundColor DarkGray
            }
            
            $categories[$categoryName] += [PSCustomObject]@{
                Name = $tableName
                Url = "$BaseUrl/tables/$tableUrl"
            }
        }
    }
    
    # If we found tables with this method, return immediately
    if (($categories.Values | Measure-Object -Property Count -Sum).Sum -gt 0) {
        if ($DebugMode) {
            Write-Host "Debug: Successfully extracted using direct link extraction!" -ForegroundColor Green
        }
        return $categories
    }
    
    # Strategy 0b: Alternative approach - look for category text followed by links
    if ($DebugMode) {
        Write-Host "Debug: Trying alternative pattern matching" -ForegroundColor Gray
    }
    
    # Pattern: Category name (text not in brackets) followed by markdown table links
    # We'll look for: plain text, then one or more [name](tables/name) patterns
    $directPattern = "(?:^|\n|\r|>)([A-Z][A-Za-z\s&/]+?)(?:\s*<[^>]*>)*\s*((?:\[(?:[^\]]+)\]\(tables/[^\)]+\)\s*)+)"
    $directMatches = [regex]::Matches($HtmlContent, $directPattern)
    
    if ($DebugMode) {
        Write-Host "Debug: Direct pattern found $($directMatches.Count) category+tables blocks" -ForegroundColor Gray
    }
    
    foreach ($directMatch in $directMatches) {
        $categoryName = $directMatch.Groups[1].Value.Trim()
        $tablesBlock = $directMatch.Groups[2].Value
        
        # Clean category name
        $categoryName = $categoryName -replace '<[^>]+>', '' -replace '&amp;', '&' -replace '&nbsp;', ' '
        $categoryName = $categoryName.Trim()
        
        # Skip if category name is too short or looks wrong
        if ($categoryName.Length -lt 3 -or $categoryName.Length -gt 50) { continue }
        if ($categoryName -match "^\d+$") { continue }  # Skip if it's just numbers
        if ($categoryName -match "^[A-Z]{4,}$") { continue }  # Skip if it's all caps abbreviation
        
        # Filter by category if specified
        if ($FilterCategory -and $categoryName -ne $FilterCategory) {
            continue
        }
        
        if (-not $categories.ContainsKey($categoryName)) {
            $categories[$categoryName] = @()
        }
        
        if ($DebugMode) {
            Write-Host "Debug: Processing category: $categoryName" -ForegroundColor Gray
        }
        
        # Extract all table links from this block
        $mdPattern = "\[([^\]]+)\]\(tables/([^\)]+)\)"
        $tableMatches = [regex]::Matches($tablesBlock, $mdPattern)
        
        if ($DebugMode) {
            Write-Host "Debug:   Found $($tableMatches.Count) tables" -ForegroundColor Gray
        }
        
        foreach ($tableMatch in $tableMatches) {
            $tableName = $tableMatch.Groups[1].Value
            $tableUrl = $tableMatch.Groups[2].Value
            
            if ($DebugMode -and $categories[$categoryName].Count -lt 3) {
                Write-Host "Debug:     - $tableName" -ForegroundColor DarkGray
            }
            
            $categories[$categoryName] += [PSCustomObject]@{
                Name = $tableName
                Url = "$BaseUrl/tables/$tableUrl"
            }
        }
    }
    
    # If we found tables with this method, return immediately
    if (($categories.Values | Measure-Object -Property Count -Sum).Sum -gt 0) {
        if ($DebugMode) {
            Write-Host "Debug: Successfully extracted using direct pattern matching!" -ForegroundColor Green
        }
        return $categories
    }
    
    # Strategy 1: Look for h2/h3 headers followed by links
    $headerPattern = '<h[23][^>]*>([^<]+)</h[23]>(.*?)(?=<h[23]|$)'
    $headerMatches = [regex]::Matches($HtmlContent, $headerPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
    
    if ($DebugMode) {
        Write-Host "Debug: Found $($headerMatches.Count) header sections" -ForegroundColor Gray
    }
    
    if ($headerMatches.Count -gt 0) {
        foreach ($headerMatch in $headerMatches) {
            $categoryName = $headerMatch.Groups[1].Value -replace '<[^>]+>', '' -replace '\s+', ' '
            $categoryName = $categoryName.Trim()
            $sectionContent = $headerMatch.Groups[2].Value
            
            if ($categoryName.Length -lt 2) { continue }
            
            $categoryName = $categoryName -replace '&amp;', '&' -replace '&nbsp;', ' '
            
            if (-not $categories.ContainsKey($categoryName)) {
                $categories[$categoryName] = @()
            }
            
            # Find table links - try multiple patterns
            $tablePattern1 = '<a[^>]+href="[^"]*?/tables/([^"/#]+)"[^>]*>([^<]+)</a>'
            $tableMatches = [regex]::Matches($sectionContent, $tablePattern1)
            
            if ($DebugMode -and $tableMatches.Count -gt 0) {
                Write-Host "Debug: Category '$categoryName' - Pattern 1 found $($tableMatches.Count) tables" -ForegroundColor Gray
            }
            
            if ($tableMatches.Count -eq 0) {
                $tablePattern2 = '<a[^>]+href="([^"]+)"[^>]*>([A-Z][a-zA-Z0-9]+)</a>'
                $tempMatches = [regex]::Matches($sectionContent, $tablePattern2)
                
                foreach ($match in $tempMatches) {
                    $href = $match.Groups[1].Value
                    if ($href -match '/tables/([^/#"]+)') {
                        $tableMatches = $tempMatches
                        break
                    }
                }
                
                if ($DebugMode -and $tableMatches.Count -gt 0) {
                    Write-Host "Debug: Category '$categoryName' - Pattern 2 found $($tableMatches.Count) tables" -ForegroundColor Gray
                }
            }
            
            foreach ($tableMatch in $tableMatches) {
                $href = $tableMatch.Groups[1].Value
                $tableName = $tableMatch.Groups[2].Value -replace '\s+', ' '
                $tableName = $tableName.Trim()
                
                $tableUrl = ""
                if ($href -match '/tables/([^/#"]+)') {
                    $tableUrl = $matches[1]
                } else {
                    $tableUrl = $href
                }
                
                if ($DebugMode) {
                    Write-Host "Debug:   - Table: $tableName -> $tableUrl" -ForegroundColor DarkGray
                }
                
                $categories[$categoryName] += [PSCustomObject]@{
                    Name = $tableName
                    Url = "$BaseUrl/tables/$tableUrl"
                }
            }
        }
    }
    
    # Strategy 2: Look for markdown-style content
    if ($categories.Keys.Count -eq 0 -or ($categories.Values | Measure-Object -Property Count -Sum).Sum -eq 0) {
        if ($DebugMode) {
            Write-Host "Debug: Trying markdown-style parsing" -ForegroundColor Gray
        }
        
        $mainContentPattern = '<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>'
        $contentMatch = [regex]::Match($HtmlContent, $mainContentPattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
        
        $searchContent = if ($contentMatch.Success) { $contentMatch.Groups[1].Value } else { $HtmlContent }
        
        $lines = $searchContent -split '<br\s*/?>|</p>|</div>|[\r\n]+'
        
        $currentCategory = ""
        
        # Regex pattern for markdown links - using double quotes with escaped backslashes
        $mdPattern = "\[([^\]]+)\]\(tables/([^\)]+)\)"
        
        foreach ($line in $lines) {
            $cleanLine = $line -replace '<[^>]+>', '' -replace '&nbsp;', ' ' -replace '&amp;', '&'
            $cleanLine = $cleanLine.Trim()
            
            if (-not $cleanLine) { continue }
            
            # Category header
            if ($cleanLine -match '^([A-Z][A-Za-z\s&/]+)$' -and $cleanLine.Length -lt 50 -and $cleanLine -notmatch "\[.*?\]") {
                $currentCategory = $cleanLine
                if (-not $categories.ContainsKey($currentCategory)) {
                    $categories[$currentCategory] = @()
                }
                if ($DebugMode) {
                    Write-Host "Debug: Found category header: $currentCategory" -ForegroundColor Gray
                }
            }
            # Table links in markdown format
            elseif ($currentCategory -and $cleanLine -match "\[.*?\]\(tables/") {
                $mdMatches = [regex]::Matches($cleanLine, $mdPattern)
                
                if ($DebugMode -and $mdMatches.Count -gt 0) {
                    Write-Host "Debug: Found $($mdMatches.Count) markdown links in category $currentCategory" -ForegroundColor Gray
                }
                
                foreach ($mdMatch in $mdMatches) {
                    $tableName = $mdMatch.Groups[1].Value
                    $tableUrl = $mdMatch.Groups[2].Value
                    
                    if ($DebugMode) {
                        Write-Host "Debug:   - MD Link: $tableName -> $tableUrl" -ForegroundColor DarkGray
                    }
                    
                    $categories[$currentCategory] += [PSCustomObject]@{
                        Name = $tableName
                        Url = "$BaseUrl/tables/$tableUrl"
                    }
                }
            }
            # HTML links
            elseif ($currentCategory -and $line -match '<a[^>]+href="[^"]*?/tables/') {
                $linkPattern = '<a[^>]+href="[^"]*?/tables/([^"#]+)"[^>]*>([^<]+)</a>'
                $linkMatches = [regex]::Matches($line, $linkPattern)
                
                if ($DebugMode -and $linkMatches.Count -gt 0) {
                    Write-Host "Debug: Found $($linkMatches.Count) HTML links in category $currentCategory" -ForegroundColor Gray
                }
                
                foreach ($linkMatch in $linkMatches) {
                    $tableUrl = $linkMatch.Groups[1].Value
                    $tableName = $linkMatch.Groups[2].Value.Trim()
                    
                    if ($DebugMode) {
                        Write-Host "Debug:   - HTML Link: $tableName -> $tableUrl" -ForegroundColor DarkGray
                    }
                    
                    $categories[$currentCategory] += [PSCustomObject]@{
                        Name = $tableName
                        Url = "$BaseUrl/tables/$tableUrl"
                    }
                }
            }
        }
    }
    
    # Strategy 3: Extract from span elements
    if ($categories.Keys.Count -eq 0 -or ($categories.Values | Measure-Object -Property Count -Sum).Sum -eq 0) {
        if ($DebugMode) {
            Write-Host "Debug: Trying span-based parsing strategy" -ForegroundColor Gray
        }
        
        $spanPatterns = @(
            '<span[^>]*class="[^"]*index[^"]*"[^>]*>(.*?)</span>',
            '<span[^>]*index[^>]*>(.*?)</span>',
            '<span[^>]*>(.*?)</span>'
        )
        
        $mdPattern = "\[([^\]]+)\]\(tables/([^\)]+)\)"
        
        foreach ($pattern in $spanPatterns) {
            $spanMatches = [regex]::Matches($HtmlContent, $pattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
            
            if ($DebugMode) {
                Write-Host "Debug: Pattern found $($spanMatches.Count) spans" -ForegroundColor Gray
            }
            
            foreach ($spanMatch in $spanMatches) {
                $spanContent = $spanMatch.Groups[1].Value
                
                if ($spanContent -match '/tables/') {
                    # Try markdown format
                    $mdMatches = [regex]::Matches($spanContent, $mdPattern)
                    
                    if ($mdMatches.Count -gt 0) {
                        if (-not $categories.ContainsKey("Extracted Tables")) {
                            $categories["Extracted Tables"] = @()
                        }
                        
                        if ($DebugMode) {
                            Write-Host "Debug: Found span with $($mdMatches.Count) markdown links" -ForegroundColor Gray
                        }
                        
                        $spanLines = $spanContent -split '[\r\n]+'
                        $currentCat = "Extracted Tables"
                        
                        foreach ($spanLine in $spanLines) {
                            $cleanLine = $spanLine -replace '<[^>]+>', '' -replace '&nbsp;', ' ' -replace '&amp;', '&'
                            $cleanLine = $cleanLine.Trim()
                            
                            # Category header
                            if ($cleanLine -match '^([A-Z][A-Za-z\s&/]+)$' -and $cleanLine.Length -lt 50 -and $cleanLine -notmatch "\[") {
                                $currentCat = $cleanLine
                                if (-not $categories.ContainsKey($currentCat)) {
                                    $categories[$currentCat] = @()
                                }
                                if ($DebugMode) {
                                    Write-Host "Debug: Span category: $currentCat" -ForegroundColor Gray
                                }
                            }
                            # Table links
                            elseif ($cleanLine -match "\[.*?\]\(tables/") {
                                $lineMdMatches = [regex]::Matches($cleanLine, $mdPattern)
                                foreach ($lineMdMatch in $lineMdMatches) {
                                    $tableName = $lineMdMatch.Groups[1].Value
                                    $tableUrl = $lineMdMatch.Groups[2].Value
                                    
                                    $categories[$currentCat] += [PSCustomObject]@{
                                        Name = $tableName
                                        Url = "$BaseUrl/tables/$tableUrl"
                                    }
                                }
                            }
                        }
                        
                        if (($categories.Values | Measure-Object -Property Count -Sum).Sum -gt 0) {
                            break
                        }
                    }
                    
                    # Try HTML link format
                    $linkPattern = '<a[^>]+href="[^"]*?/tables/([^"#]+)"[^>]*>([^<]+)</a>'
                    $linkMatches = [regex]::Matches($spanContent, $linkPattern)
                    
                    if ($linkMatches.Count -gt 0) {
                        if (-not $categories.ContainsKey("Extracted Tables")) {
                            $categories["Extracted Tables"] = @()
                        }
                        
                        if ($DebugMode) {
                            Write-Host "Debug: Found span with $($linkMatches.Count) HTML links" -ForegroundColor Gray
                        }
                        
                        foreach ($linkMatch in $linkMatches) {
                            $tableUrl = $linkMatch.Groups[1].Value
                            $tableName = $linkMatch.Groups[2].Value.Trim()
                            
                            $categories["Extracted Tables"] += [PSCustomObject]@{
                                Name = $tableName
                                Url = "$BaseUrl/tables/$tableUrl"
                            }
                        }
                        
                        if ($linkMatches.Count -gt 0) {
                            break
                        }
                    }
                }
            }
            
            if (($categories.Values | Measure-Object -Property Count -Sum).Sum -gt 0) {
                break
            }
        }
    }
    
    return $categories
}

# Main script execution
Write-Host "========================================" -ForegroundColor Green
Write-Host "Azure Monitor Tables Reference Scraper" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

if ($AllCategories) {
    $CategoryFilter = ""
    Write-Host "Mode: Processing ALL categories" -ForegroundColor Yellow
    if ($OutputFile -eq "AzureMonitorTables_Security.json") {
        $OutputFile = "AzureMonitorTables_All.json"
    }
} else {
    if (-not $CategoryFilter) {
        $CategoryFilter = "Security"
    }
    Write-Host "Mode: Processing category - $CategoryFilter" -ForegroundColor Yellow
}
Write-Host "Output file: $OutputFile" -ForegroundColor Cyan
Write-Host ""

# Fetch the main category page
Write-Host "Fetching category page..." -ForegroundColor Yellow
$categoryUrl = "$BaseUrl/tables-category"
$htmlContent = Get-WebContent -Url $categoryUrl

if (-not $htmlContent) {
    Write-Error "Failed to fetch the category page. Exiting."
    exit 1
}

Write-Host "Successfully fetched category page." -ForegroundColor Green
Write-Host ""

# Extract categories and tables
Write-Host "Extracting table information..." -ForegroundColor Yellow

if ($Debug) {
    $htmlContent | Out-File -FilePath "debug_raw_html.txt" -Encoding UTF8
    Write-Host "Debug: Saved raw HTML to debug_raw_html.txt" -ForegroundColor Gray
    
    Write-Host "Debug: First 1000 chars of HTML:" -ForegroundColor Gray
    Write-Host ($htmlContent.Substring(0, [Math]::Min(1000, $htmlContent.Length))) -ForegroundColor DarkGray
    
    # Check for markdown-style links in the raw HTML
    $testMdPattern = "\[([^\]]+)\]\(tables/([^\)]+)\)"
    $testMatches = [regex]::Matches($htmlContent, $testMdPattern)
    Write-Host "Debug: Found $($testMatches.Count) total markdown-style table links in raw HTML" -ForegroundColor Gray
    
    if ($testMatches.Count -gt 0) {
        Write-Host "Debug: First 5 examples:" -ForegroundColor Gray
        for ($i = 0; $i -lt [Math]::Min(5, $testMatches.Count); $i++) {
            Write-Host "  [$($testMatches[$i].Groups[1].Value)](tables/$($testMatches[$i].Groups[2].Value))" -ForegroundColor DarkGray
        }
    }
}

$categoryTables = Get-CategoryTables -HtmlContent $htmlContent -DebugMode:$Debug -FilterCategory $CategoryFilter

if ($Debug) {
    Write-Host "Debug: Found $($categoryTables.Keys.Count) categories" -ForegroundColor Gray
    foreach ($cat in $categoryTables.Keys) {
        Write-Host "  - $cat : $($categoryTables[$cat].Count) tables" -ForegroundColor Gray
    }
}

# If no categories found, try simplified extraction
if ($categoryTables.Keys.Count -eq 0) {
    Write-Host "Warning: No categories found. Trying simplified extraction..." -ForegroundColor Yellow
    
    $allTables = @()
    $mdPattern = "\[([^\]]+)\]\(tables/([^\)]+)\)"
    $tableMatches = [regex]::Matches($htmlContent, $mdPattern)
    
    Write-Host "Found $($tableMatches.Count) markdown table links" -ForegroundColor Yellow
    
    foreach ($match in $tableMatches) {
        $tableName = $match.Groups[1].Value
        $tableUrl = $match.Groups[2].Value
        
        $allTables += [PSCustomObject]@{
            Name = $tableName
            Url = "$BaseUrl/tables/$tableUrl"
        }
    }
    
    # Also try HTML links
    if ($allTables.Count -eq 0) {
        $linkPattern = '<a[^>]+href="[^"]*?/tables/([^"#]+)"[^>]*>([^<]+)</a>'
        $linkMatches = [regex]::Matches($htmlContent, $linkPattern)
        
        Write-Host "Found $($linkMatches.Count) HTML table links" -ForegroundColor Yellow
        
        foreach ($match in $linkMatches) {
            $tableUrl = $match.Groups[1].Value
            $tableName = $match.Groups[2].Value.Trim()
            
            $allTables += [PSCustomObject]@{
                Name = $tableName
                Url = "$BaseUrl/tables/$tableUrl"
            }
        }
    }
    
    if ($allTables.Count -gt 0) {
        $categoryTables = @{
            "All Tables" = $allTables
        }
        Write-Host "Successfully extracted $($allTables.Count) tables" -ForegroundColor Green
    } else {
        Write-Error "Could not extract any tables from the page. Please check debug_raw_html.txt"
        exit 1
    }
}

# Build the result object
$result = @{
    GeneratedDate = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
    SourceUrl = $categoryUrl
    Categories = @{}
}

# Process each category
foreach ($category in $categoryTables.Keys | Sort-Object) {
    
    if ($CategoryFilter -and $category -ne $CategoryFilter) {
        continue
    }
    
    Write-Host "Processing category: $category" -ForegroundColor Magenta
    $result.Categories[$category] = @()
    
    $tables = $categoryTables[$category]
    $tableCount = $tables.Count
    $currentTable = 0
    
    foreach ($table in $tables) {
        $currentTable++
        Write-Progress -Activity "Processing $category" -Status "$currentTable of $tableCount : $($table.Name)" -PercentComplete (($currentTable / $tableCount) * 100)
        
        # Get columns for this table
        $columns = Get-TableColumns -TableUrl $table.Url -TableName $table.Name
        
        $result.Categories[$category] += [PSCustomObject]@{
            TableName = $table.Name
            Url = $table.Url
            ColumnCount = $columns.Count
            Columns = $columns
        }
        
        # Small delay to be respectful to the server (reduced for smaller queries)
        Start-Sleep -Milliseconds 300
    }
    
    Write-Progress -Activity "Processing $category" -Completed
    Write-Host "Completed $category - $tableCount tables processed" -ForegroundColor Green
    Write-Host ""
}

# Export to JSON
Write-Host "Exporting to JSON..." -ForegroundColor Yellow
$jsonOutput = $result | ConvertTo-Json -Depth 10
$jsonOutput | Out-File -FilePath $OutputFile -Encoding UTF8

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "Export completed successfully!" -ForegroundColor Green
Write-Host "Output file: $OutputFile" -ForegroundColor Green
Write-Host "Total categories: $($result.Categories.Count)" -ForegroundColor Green
$totalTables = ($result.Categories.Values | ForEach-Object { $_.Count } | Measure-Object -Sum).Sum
Write-Host "Total tables: $totalTables" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green