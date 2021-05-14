Function GetAllObjectsInDBsNotTemplates
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
        # Initialize Data Objects
        $ObjectDetails = @()
        $ObjectsToSearch = @()
        # Initialize Runtime
        $RunTime = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneID([DateTime]::Now, "Eastern Standard Time").ToString("yyyy-MM-dd hh:mm:ss tt")

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
        # Set SQL Query to pull list of Databases from Server
        $sqlcmd = @"
           This will be a SELECT statement that pulls a list of databases from the server
            and maps it to a list of Projects to iterate through
"@
        $Databases = @(Invoke-Sqlcmd -Database "master" -ServerInstance $SourceServer -Query $sqlcmd)
        $ObjectsToSearch += 0..($Databases.Count-1) | ForEach-Object {

            # Set SQL Query to pull objects from Database
            $sqlcmd = @"
                        SELECT '$($Databases[$_].Project)' AS ProjectName, DB_NAME() AS DatabaseName, s.name AS SchemaName, o.name AS ObjectName, o.type_desc, o.create_date, o.modify_date
                        FROM sys.objects o
                        JOIN sys.schemas s ON o.schema_id = s.schema_id
                        WHERE s.name <> 'sys'
                        AND o.type <> 'SQ'
                        ORDER BY s.name, o.type_desc, o.name
"@
            @(Invoke-Sqlcmd -Database $Databases[$_].name -ServerInstance $SourceServer -Query $sqlcmd)
        }

        # Build array of custom objects
        $ObjectDetails += 0..($ObjectsToSearch.Count-1) |
        ForEach-Object {
            [PSCustomObject]@{
                RunDateTime               = $RunTime
                RunBy                     = $env:USERNAME
                GITBranch                 = $GitBranch
                ProjectName               = $ObjectsToSearch[$_].ProjectName
                DatabaseName              = $ObjectsToSearch[$_].DatabaseName
                SchemaName                = $ObjectsToSearch[$_].SchemaName
                ObjectName                = $ObjectsToSearch[$_].ObjectName
                ObjectType                = $ObjectsToSearch[$_].type_desc
                CreateDate                = $ObjectsToSearch[$_].create_date
                ModifyDate                = $ObjectsToSearch[$_].modify_date
                # File properties
                FoundInSourceControl      = $null
            }
        }

        $i = 0
        ForEach ($Object in $ObjectDetails)
        {
            $SchemaFolder = switch -wildcard ($Object.ObjectType)
            {
                "*FUNCTION"         {"Functions"}
                "*PROCEDURE"        {"Stored Procedures"}
                "SEQUENCE_OBJECT"   {"Sequences"}
                "USER_TABLE"        {"Tables"}
                "VIEW"              {"Views"}
                "*CONSTRAINT"       {"Tables (Scan text)"}
                "*TRIGGER"          {"Tables (Scan text)"}
            }

            $FolderPath = switch -wildcard ($Object.ProjectName)
            {
                "NonStandardDB1" {@("${GitRoot}$($Object.ProjectName)\")}
                "NonStandardDB2" {@("${GitRoot}$($Object.ProjectName)_DB\$($Object.ProjectName)\")}
                "NonStandardDB3" {@("${GitRoot}$($Object.ProjectName)_DB\$($Object.ProjectName)\")}
                "*Wildcard*"     {@("${GitRoot}Wildcard\$($Object.ProjectName)\")}
                default          {@("${GitRoot}$($Object.ProjectName.Substring(0,$Object.ProjectName.Length-3))_DB\$($Object.ProjectName)\", "${GitRoot}SecondPath\SecondPath\")}
            }
            ForEach ($FP in $FolderPath)
            {
                $FP = "${FP}$($Object.SchemaName)\"
                $FP = switch ($SchemaFolder)
                {
                    "Tables (Scan text)"    {"${FP}Tables\"}
                    default                 {"${FP}${SchemaFolder}\"}
                }
                $FilePath = "${FP}$($Object.ObjectName).sql"

                If ($SchemaFolder -eq "Tables (Scan text)")
                {
                    If (Test-Path $FP)
                    {
                        $Files = (Get-ChildItem -Path $FP -Filter "*.sql" -Recurse).FullName
        
                        ForEach ($File in $Files)
                        {
                            If ($Object.FoundInSourceControl -ne "Y")
                            {
                                $ObjectContent = Get-Content -Raw $File
                                $SearchString = "${ExcludeComments}$($Object.ObjectName)"
                                If ($ObjectContent | Select-String -Pattern $SearchString)
                                {
                                    $Object.FoundInSourceControl = 'Y'
                                    # Breaks out of all loops, which we want
                                    break;
                                } 
                                Else {$Object.FoundInSourceControl = 'N'}
                            }
                        }
                    }
                    Else {$Object.FoundInSourceControl = 'N'}
                }
                Else
                {
                    If(Test-Path -Path $FilePath)
                    {
                        $Object.FoundInSourceControl = "Y"
                        # Breaks out of all loops, which we want
                        break;
                    }
                    Else {$Object.FoundInSourceControl = "N"}
                }
            }
        $i++
        Write-Progress -Activity ReferenceSearch -Status "Searching for $($Object.ObjectName)" -PercentComplete (($i / $ObjectDetails.Count) * 100)
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
        # Write $$DataObject to Sql Server environment
        # Note: This command requires the SqlServer PowerShell module be installed
        Write-SqlTableData -DatabaseName $DestDatabase -ServerInstance $DestServer -SchemaName $DestSchema -TableName $DestTable -InputData $ObjectDetails -Force
        Write-Output "$($ObjectDetails.Count) records loaded into $DestServer.$DestDatabase.$DestSchema.$DestTable"
    }
}

$Params = @{
    GitRoot  = "Path"
    SourceServer   = "Source Server"
    SourceDatabase = "master"
    DestServer   = "Destination Server"
    DestDatabase = "Destination Database"
    DestSchema   = "Destination Schema"
    DestTable    = "Tablename"
    # Truncate = $true
}
Measure-Command{GetAllObjectsInDBsNotTemplates @Params | Out-Host}