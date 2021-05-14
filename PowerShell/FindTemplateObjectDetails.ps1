<#
This function requires Powershell version 5.0 or greater due to the use of the use of the SqlServer Module for Write-SqlTableData and Invoke-Sqlcmd
#>
Function FindTemplateObjectDetails
{
    Param
    (
        [parameter(Mandatory=$true)][string]  $GitRoot
       ,[parameter(Mandatory=$true)][string]  $Server
       ,[parameter(Mandatory=$true)][string]  $Schema
       ,[parameter(Mandatory=$true)][string]  $Database
       ,[parameter(Mandatory=$true)][string]  $Table
       ,[parameter(Mandatory=$false)][string] $Truncate = $false
    )
    Begin
    {
        # Initialize $DataObject
        $DataObject = @()
        # Initialize Runtime
        $RunTime = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneID([DateTime]::Now, "Eastern Standard Time").ToString("yyyy-MM-dd hh:mm:ss tt")
        # Set $TemplateDB
        $TemplateDB = Get-ChildItem -Path "${GitRoot}Path\" -Directory | Where-Object {$_ -notlike "*Exclusion*"}
        
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
        ForEach ($TemplateRoot in $TemplateDB)
        {
            # Get location of Template's sqlproj file
            $SqlProj = (Get-ChildItem -Path $TemplateRoot.FullName -Filter "*.sqlproj").FullName
            # Get list of all .sql files within a given $TemplateRoot, ignoring anything in a ExclusionFolder folder
            $Files = (Get-ChildItem -Path $TemplateRoot.FullName -Exclude @("bin","obj","Scripts","Security","SqlSchemaCompare","tests") -Directory |
                        Get-ChildItem -Filter "*.sql" -Recurse).FullName |
                        Where-Object {$_ -notlike "*\ExclusionFolder\*"}
            # Use Split-Path to get different sections of the path
            $FileNames   = @($Files | Split-Path -Leaf)
            $FolderNames = @($Files | Split-Path | Split-Path -Leaf)
            $Schemas     = @($Files | Split-Path | Split-Path | Split-Path -Leaf)

            # Build array of custom objects
            $DataObject += 0..($Files.Count-1) |
            ForEach-Object {
                [PSCustomObject]@{
                    RunDateTime               = $RunTime
                    RunBy                     = $env:USERNAME
                    GITBranch                 = $GitBranch
                    FilePath                  = $Files[$_]
                    FileName                  = $FileNames[$_]
                    FolderName                = $FolderNames[$_]
                    Schema                    = $Schemas[$_]
                    ObjectType                = $null
                    # File properties
                    MeetsSyntax               = $null
                    InSqlProj                 = $null
                    ContainsSelectStar        = $null
                    ContainsDeletedDotStar    = $null
                    ContainsAmbiguousInsert   = $null
                    CallsLogTableDirectly     = $null
                    CallsLoggingProcedure     = $null
                }
            }
            # Set $DataObject.ObjectType based on FolderName
            $DataObject | Where-Object {$_.FolderName -eq "Functions"}          | ForEach-Object {$_.ObjectType = "FUNCTION"}
            $DataObject | Where-Object {$_.FolderName -eq "Sequences"}          | ForEach-Object {$_.ObjectType = "SEQUENCE"}
            $DataObject | Where-Object {$_.FolderName -eq "Stored Procedures"}  | ForEach-Object {$_.ObjectType = "PROCEDURE"}
            $DataObject | Where-Object {$_.FolderName -eq "Tables"}             | ForEach-Object {$_.ObjectType = "TABLE"}
            $DataObject | Where-Object {$_.FolderName -eq "User Defined Types"} | ForEach-Object {$_.ObjectType = "TYPE"}
            $DataObject | Where-Object {$_.FolderName -eq "Views"}              | ForEach-Object {$_.ObjectType = "VIEW"}

            ForEach ($File in $Files)
            {
                $Object = $DataObject | Where-Object {$_.FilePath -eq $File}
                $SearchObject = $Object.FileName -replace ".sql",""
                $ObjectContent = Get-Content -Raw $Object.FilePath

                # Note: Select-String with a -Path or piped from Get-Content matches on each line in a file (also thought of as each element in an array).
                #       This gives incorrect results when dealing with multiline comment blocks. The workaround to this is to pipe Get-Content -Raw to Select-String.
                #       The -Raw flag forces Get-Content to return the file contents as one string instead of an array of string, thus allowing the comments to be ignored properly.

                # Does the Object text contain the standard syntax for its CREATE line, ignoring case, whitespace, and []?
                $SearchString = "${ExcludeComments}CREATE\s+$($Object.ObjectType)\s+\[?$($Object.Schema)\]?.\[?$($SearchObject)\]?"
                If ($ObjectContent | Select-String -Pattern $SearchString) {$Object.MeetsSyntax = 'Y'} Else {$Object.MeetsSyntax = 'N'}

                # Does the file exist in the respective .sqlproj?
                $SearchString = $SearchObject
                If (Select-String -Path $SqlProj -Pattern $SearchString) {$Object.InSqlProj = 'Y'} Else {$Object.InSqlProj = 'N'}

                # Only check in FUNCTIONs, PROCEDUREs, and VIEWs
                If ($Object.ObjectType -eq "FUNCTION" -or $Object.ObjectType -eq "PROCEDURE" -or $Object.ObjectType -eq "VIEW")
                {
                    # Does the file contain SELECT * FROM that isn't preceded by an EXISTS ( statement?
                    $SearchString = "${ExcludeComments}(?<!(EXISTS\s*\(\s*))SELECT\s+\*\s+FROM"
                    If ($ObjectContent | Select-String -Pattern $SearchString) {$Object.ContainsSelectStar = 'Y'} Else {$Object.ContainsSelectStar = 'N'}
                }

                # Only check in FUNCTIONs and PROCEDUREs
                If ($Object.ObjectType -eq "FUNCTION" -or $Object.ObjectType -eq "PROCEDURE")
                {
                    # Does the file contain DELETED.*?
                    $SearchString = "${ExcludeComments}DELETED\.\*"
                    If ($ObjectContent | Select-String -Pattern $SearchString) {$Object.ContainsDeletedDotStar = 'Y'} Else {$Object.ContainsDeletedDotStar = 'N'}

                    # Does the file contain an INSERT without an explicit column list (INSERT without ( between it and SELECT or VALUES)?
                    $SearchString = "${ExcludeComments}INSERT[^(]+(SELECT|VALUES)"
                    If ($ObjectContent | Select-String -Pattern $SearchString) {$Object.ContainsAmbiguousInsert = 'Y'} Else {$Object.ContainsAmbiguousInsert = 'N'}

                    # Does the file contain a direct call to LogTableDirectly?
                    $SearchString = "${ExcludeComments}LogTableDirectly"
                    If ($ObjectContent | Select-String -Pattern $SearchString) {$Object.CallsLogTableDirectly = 'Y'} Else {$Object.CallsLogTableDirectly = 'N'}

                    # Does the file contain a call to LoggingProcedure?
                    $SearchString = "${ExcludeComments}LoggingProcedure"
                    If ($ObjectContent | Select-String -Pattern $SearchString) {$Object.CallsLoggingProcedure = 'Y'} Else {$Object.CallsLoggingProcedure = 'N'}
                }
            }
        }
    }
    End
    {
        If ($Truncate -eq $true)
        {
            # Drops destination table if exists
            # Note: This command requires the SqlServer PowerShell module be installed
            Invoke-Sqlcmd -Database $Database -ServerInstance $Server -Query "DROP TABLE IF EXISTS $Schema.$Table"
            Write-Output "Dropped table $Schema.$Table if it existed"
        }
        # Write $$DataObject to Sql Server environment
        # Note: This command requires the SqlServer PowerShell module be installed
        Write-SqlTableData -DatabaseName $Database -ServerInstance $Server -SchemaName $Schema -TableName $Table -InputData $DataObject -Force
        Write-Output "$($DataObject.Count) records loaded into $Server.$Database.$Schema.$Table"
    }
}

$Params = @{
    GitRoot  = "Path"
    Server   = "Server"
    Database = "Database"
    Schema   = "Schema"
    Table    = "Table"
    # Truncate = $true
}
Measure-Command{FindTemplateObjectDetails @Params | Out-Host}