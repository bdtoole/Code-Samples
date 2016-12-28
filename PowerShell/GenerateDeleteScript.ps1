<#
 # This script searches through a user-specified source directory to find all objects related to a user-specified report ID.
 # It then proceeds to create a drop script for all objects associated with the report.
 #
 # Note that this is not a one-button-does-all solution.  You will need to make sure the generated script is accurate
 #
 # To use this script:
 #   Modify reportsToProcess array to contain all Report objects to generate drop scripts for
 #
 # Assumptions:
 # 1.  Objects have the 3-digit number of the report in the file name
 # 2.  Objects have the same name as the file containing the code for the object
 # 3.  Source directory specified should be the CVS directory for the report in question
 # 4.  Object SQL files follow our coding standards.  Note that Index drop scripts pull code directly from the
 #     SQL scripts in CVS due to Index names being unique per Table and not per Database
 # 5.  Objects exist in Reporting database.  Not designed for other CVS modules
 #
 # Use Case Example - XXXX_YYY:
 # Select your local XXXX CVS directory as the source and any local directory as the output directory
 # The script will recursively search for all objects with YYY in the file name and generate drop scripts accordingly
 #>

#Create class to implement System.Windows.Forms.IWin32Window
#This allows the directory-picker dialog to be forced to the front of the screen when referenced with ShowDialog()
Add-Type -TypeDefinition @"
using System;
using System.Windows.Forms;

public class Win32Window : IWin32Window
{
    private IntPtr _hWnd;

    public Win32Window(IntPtr handle)
    {
        _hWnd = handle;
    }

    public IntPtr Handle
    {
        get { return _hWnd; }
    }
}
"@ -ReferencedAssemblies "System.Windows.Forms.dll"

Function Get-Directory
{
    Param(
    [alias("sd")]
    [String] $StartingDir,
    [alias("d")]
    [String] $Description
    )

    #Force the window to the front
    $owner = New-Object Win32Window -ArgumentList([System.Diagnostics.Process]::GetCurrentProcess().MainWindowHandle)

    Add-Type -AssemblyName System.Windows.Forms  
    $dialog = New-Object System.Windows.Forms.FolderBrowserDialog
    $dialog.Rootfolder = $StartingDir
    $dialog.Description = $Description
    $buttonClick = $dialog.ShowDialog($owner)

    If ($buttonClick -eq "OK")
    {
        Return $dialog.SelectedPath
    }
    Else
    {
        Write-Warning "Operation canceled by user."
        Exit 0
    }
}

Function Get-OutputFile
{
    Param(
    [alias("r")]
    [String] $report,
    [alias("p")]
    [String] $path
    )

    $SQLFileExists = Test-Path "$path\Drop_$($report)_Objects.sql"
    if ($SQLFileExists) {
        $SQLFile = Get-ChildItem "$path\Drop_$($report)_Objects.sql"
    } else {
        $SQLFile = New-Item -Path "$path\" -Name "Drop_$($report)_Objects.sql" -ItemType File
    }

    Return $SQLFile
}

Function Write-File
{
    Param(
    [alias("r")]
    [String] $report,
    [alias("i")]
    [String] $inputFileText,
    [alias("p")]
    [String] $path
    )

    Write-Verbose -Message "Writing Output File..." -Verbose

    $outputFile = Get-OutputFile -r $report -p $path

    $inputFileText -replace "`n", "`r`n" | Out-File $outputFile -encoding ASCII
}

Function Build-SQL
{
    Param(
    [alias("r")]
    [String] $report,
    [alias("b")]
    [String] $basePath,
    [alias("o")]
    [String] $outPath
    )
    
    $fileTypes = @(
        'idx'
      , 'proc'
      , 'tbl'
      , 'vw'
    )

    Write-Verbose -Message "Building SQL..." -Verbose
    
    $SQLCommand = "USE <db> `nGO`n`n"
    $objCount = 0

    $objSearch = $report.Substring($report.Length-3)
    $files = Get-ChildItem -Recurse $basePath -Include "*$objSearch*"

    ForEach ($f in $files)
    {
        If ($fileTypes -contains $f.Directory.BaseName)
        {
            # For Indexes, we reuse the existing code to drop due to the possibility of indexes not being
            # unique across tables 
            If ($f.Directory.BaseName -eq "idx")
            {
                $objCount++
                $commentString = "/* Object Count: $objCount */`n"

                $fileContent = Get-Content($f.FullName)
                $startVal = $fileContent.IndexOf("GO")+1
                $endVal = $fileContent.IndexOf("ELSE")
                $fileContent[$endVal] = "GO`n`n"
                $indexString = ($fileContent[$startVal..$endVal] | ? {$_}) -join "`n"

                $SQLCommand = $SQLCommand + $commentString + $indexString
            }
            # For Tables, Views, and Procedures, we create a simple EXIST check and DROP statement
            Else
            {
                $objCount++
                $commentString = "/* Object Count: $objCount */`n"
                $customString = "IF EXISTS (SELECT * FROM sys.objects WHERE NAME = "
                $customString = $customString + "'" + $f.BaseName + "')`n"
                $customString += "BEGIN`n"
                $debugString = "`n`t"
                If ($f.Directory.BaseName -eq "proc")
                {
                    $customString += "`tDROP PROCEDURE "
                    $debugString += "PRINT 'Dropped Procedure $($f.BaseName)' "
                }
                ElseIf ($f.Directory.BaseName -eq "tbl")
                {
                    $customString += "`tDROP TABLE "
                    $debugString += "PRINT 'Dropped Table $($f.BaseName)' "
                }
                ElseIf ($f.Directory.BaseName -eq "vw")
                {
                    $customString += "`tDROP VIEW "
                    $debugString += "PRINT 'Dropped View $($f.BaseName)' "
                }
                Else
                {
                    Write-Warning "Unable to finish building SQL statement"
                    Write-Warning "Unhandled case for folder $($f.Directory.BaseName)"
                    Exit 0
                }
                $customString += $f.BaseName
                $customString = $customString + $debugString
                $customString += "`nEND`nGO`n`n"

                $SQLCommand = $SQLCommand + $commentString + $customString
            }
        }
        # If file found is a Drop Objects script move on
        ElseIf ($f.BaseName -like "Drop_*_objects")
        {
            # Do nothing
            # This case should only happen if the base Report directory and the Output file directory match
            Write-Verbose "Found file $($f.BaseName) - doing nothing" -Verbose
        }
        Else
        {
            Write-Warning "Unhandled case for folder $($f.Directory.BaseName)"
            Exit 0
        }
    }

    Write-Output "$objCount objects found in directory tree for report $report"

    Write-File -r $report -i $SQLCommand -p $outPath
}

Function GenerateScript
{
    Param(
      [Parameter(Mandatory=$true, Position=0, HelpMessage="Input the Report ID")]
      [alias("r")]
      [ValidateScript({
        If ($_ -cmatch "\b[A-Z]{3,4}_[0-9]{3}\b")
        {
            $True
        }
        Else
        {
            Throw "`n$_ is not a valid Report ID.  `nInput must conform to current naming convention and is case sensitive."
        }
      })]
      [String] $ReportID,
      [alias("b")]
      [string] $baseDir,
      [alias("o")]
      [string] $outDir
    )

    Build-SQL -r $ReportID -b $baseDir -o $outDir
}

$reportsToProcess = @(
    'REPT_001'
  , 'REPT_002'
)

$BaseDir = Get-Directory -sd "DesktopDirectory" -d "Select Base Report Directory"
$OutputDir = Get-Directory -sd "DesktopDirectory" -d "Select Output Directory"

ForEach ($rpt in $reportsToProcess)
{
    GenerateScript -r $rpt -b $BaseDir -o $OutputDir
}
