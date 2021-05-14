Function FindUnusedReferenceTables
{
    Param
    (
        [parameter(Mandatory=$true)][string]   $GitRoot
       ,[parameter(Mandatory=$true)][string]   $SourceServer
       ,[parameter(Mandatory=$true)][string]   $SourceDatabase
       ,[parameter(Mandatory=$true)][string]   $DestServer
       ,[parameter(Mandatory=$true)][string]   $DestSchema
       ,[parameter(Mandatory=$true)][string]   $DestDatabase
       ,[parameter(Mandatory=$true)][string]   $DestTable
       ,[parameter(Mandatory=$false)][string]  $Truncate = $false
    )
    Begin
    {
        # Initialize $DataObject
        $DataObject = @()
        # Initialize Runtime
        $RunTime = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneID([DateTime]::Now, "Eastern Standard Time").ToString("yyyy-MM-dd hh:mm:ss tt")
        # Set $SearchRoot
        $SearchRoot = Get-ChildItem -Path ${GitRoot} -Directory -Recurse | Where-Object {($_ -like "*ETL" -and $_.Name -ne "_Template_ETL") -or $_ -like "*Template_DB*" -or $_ -like "*_Template_Qualit8*"}
        
        # Get Git branch while preserving current directory
        $StartingDir = $(Get-Item -path .).FullName
        Set-Location $GitRoot
        $GitBranch = (git rev-parse --abbrev-ref HEAD) | Split-Path -Leaf
        Set-Location $StartingDir

        # Due to the complexity of regular expressions, a description as well as references are listed below:
        # https://stackoverflow.com/questions/51312345/regex-to-find-a-string-excluding-comments (description of the regular expressions used, copied and modified below)
        # https://www.regular-expressions.info/lookaround.html (explanation of lookbehinds and lookaheads)
        # http://www.rexegg.com/regex-quantifiers.html#tempered_greed (explanation of a tempered greedy token)

        # Negative lookbehind that checks for -- followed by 0+ non-line break characters
        $ExcludeSingleLineComments = "(?<!--.*)"
        # Negative lookbehind that checks for:
        # /\* - a /* substring
        # (?:(?!\*/)[\s\S\r])*? - (tempered greedy token to allow for nested block comments)
        #                         A non-capturing group (?:) of any char including line breaks ([\s\S\r]), with 0 or more repetitions but as few as possible (*?) 
        #                         and a negative lookahead checking for a */ substring (?!\*/)
        $ExcludeBlockComments = "(?<!/\*(?:(?!\*/)[\s\S\r])*?)"
        # Concatenate and append an ignore-case expression
        $ExcludeComments = "(?i)$ExcludeSingleLineComments$ExcludeBlockComments"

    }
    Process
    {
        # Set SQL Query to pull list of Tables from Server
        $sqlcmd = @"
        use prodsupport

        ;with TableStats AS
        (
        select SchemaName, tablename, 
               max(isnull(Last_User_Seek,'1/1/1900')) as Last_User_Seek,
               max(isnull(Last_User_Scan,'1/1/1900')) as Last_User_Scan,
               max(isnull(Last_User_Lookup,'1/1/1900')) as Last_User_Lookup,
               max(isnull(Last_User_Update,'1/1/1900')) as Last_User_Update
        from  TABLE
        where DatabaseName  = 'dbname'
        group by SchemaName, tablename
        )
        ,TableStats2 AS
        (
        select SchemaName, tablename, 
               case 
                      when Last_User_Seek >= Last_User_Scan and Last_User_Seek >= Last_User_Lookup then Last_User_Seek
                      when Last_User_Scan >= Last_User_Seek and Last_User_Scan >= Last_User_Lookup then Last_User_Scan
                      when Last_User_Lookup >= Last_User_Seek and Last_User_Lookup >= Last_User_Seek then Last_User_Lookup
               else Null end as LastUsed,
               Last_User_Update
        from TableStats
        where SchemaName <> 'Deleted'
        )
        SELECT * FROM TableStats2
        order by LastUsed
"@
        # Force $Tables to always be an array
        $Tables = @(Invoke-Sqlcmd -Database $SourceDatabase -ServerInstance $SourceServer -Query $sqlcmd)

        # Build array of custom objects
        $DataObject += 0..($Tables.Count-1) |
        ForEach-Object {
            [PSCustomObject]@{
                RunDateTime               = $RunTime
                RunBy                     = $env:USERNAME
                GITBranch                 = $GitBranch
                ReferenceSchema           = $Tables[$_].SchemaName
                ReferenceTable            = $Tables[$_].tablename
                LastUsed                  = $Tables[$_].LastUsed
                LastUpdate                = $Tables[$_].Last_User_Update
                # File properties
                FoundInCode               = $null
            }
        }
        ForEach($Dir IN $SearchRoot)
        {
            # Get list of all .sql files within a given $Dir, ignoring anything in a ExclusionFolder folder
            $Files = (Get-ChildItem -Path $Dir.FullName -Exclude @("bin","obj","Scripts","Security","SqlSchemaCompare","tests") -Directory |
                        Get-ChildItem -Filter "*.sql" -Recurse).FullName |
                        Where-Object {$_ -notlike "*\ExclusionFolder\*"}
            
            $i = 0
            ForEach ($File IN $Files)
            {
                $FileContent = Get-Content -Raw $File

                ForEach ($Table IN $Tables)
                {
                    $FoundObject = $DataObject | Where-Object {$_.ReferenceTable -eq $Table.tablename -and $_.ReferenceSchema -eq $Table.SchemaName}

                    # Note: Select-String with a -Path or piped from Get-Content matches on each line in a file (also thought of as each element in an array).
                    #       This gives incorrect results when dealing with multiline comment blocks. The workaround to this is to pipe Get-Content -Raw to Select-String.
                    #       The -Raw flag forces Get-Content to return the file contents as one string instead of an array of string, thus allowing the comments to be ignored properly.
    
                    # Does the Object text contain a reference to one of the Reference tables, ignoring case, whitespace, and []?
                    $SearchString = "${ExcludeComments}\[\$\(dbVariable\)\].\[?$($Table.SchemaName)\]?.\[?$($Table.tablename)\]?\b"
                    If ($FileContent | Select-String -Pattern $SearchString) { $FoundObject.FoundInCode = 'Y' }
                }
            $i++
            Write-Progress -Activity ReferenceSearch -Status "Searching $($Dir.Name)" -PercentComplete (($i / $Files.Count) * 100)
            }
        }
    }
    End
    {
        If ($Truncate -eq $true)
        {
            # Drops destination table if exists
            # Note: This command requires the SqlServer PowerShell module be installed
            Invoke-Sqlcmd -Database $DestDatabase -ServerInstance $DestServer -Query "DROP TABLE IF EXISTS $DestSchema.$DestTable"
            Write-Output "Dropped table $DestSchema.$DestTable if it existed"
        }
        # Write $DataObject to Sql Server environment
        # Note: This command requires the SqlServer PowerShell module be installed
        Write-SqlTableData -DatabaseName $DestDatabase -ServerInstance $DestServer -SchemaName $DestSchema -TableName $DestTable -InputData $DataObject -Force
        Write-Output "$($DataObject.Count) records loaded into $DestServer.$DestDatabase.$DestSchema.$DestTable"
    }
}

$Params = @{
    GitRoot  = "Path"
    SourceServer   = "Source Server"
    SourceDatabase = "Source Database"
    DestServer   = "Destination Server"
    DestDatabase = "Destination Database"
    DestSchema   = "Destination Schema"
    DestTable    = "Tablename"
    # Truncate = $true
}
Measure-Command{FindUnusedReferenceTables @Params | Out-Host}