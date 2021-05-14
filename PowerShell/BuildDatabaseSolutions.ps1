function BuildDatabaseSolutions
{
    param
    (
        [parameter(Mandatory=$false)][string] $verbosity = 'Minimal'
       ,[parameter(Mandatory=$false)][Bool] $loggingEnabled = $true
    )
    process
    {
        # $GITROOT poulated from User environment variable. If $GITROOT does not exist, prompt user
        $GITROOT = [Environment]::GetEnvironmentVariable("GITROOT","User")
        If (!$GITROOT)
        {
            do
            {
                # Prompt for input
                # Append trailing \ if omitted and replace / with \ for path consistency
                # This is done because Powershell recognizes both / and \ as path separators
                $GITROOT = (Join-Path (Read-Host -Prompt 'Please enter your git/TFS root directory') '\') -replace '/', '\'
                If (Test-Path $GITROOT)
                {
                    Write-Host "Setting git/TFS directory to: $GITROOT" -ForegroundColor Yellow
                    [Environment]::SetEnvironmentVariable("GITROOT", $GITROOT, "User")
                }
                else
                {
                    Write-Host "Invalid git/TFS root directory: $GITROOT" -ForegroundColor Red
                }
            }
            while (-not (Test-Path $GITROOT))
        }
        # LOGDIR default location is within parent directory of $GITROOT
        $LOGDIR = "$((Get-Item $GITROOT).Parent.FullName)\BuildLogs"
        # If running VS2017 with default installation settings, DO NOT CHANGE $msBuildExe
        $msBuildExe = 'C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\msbuild.exe'

        $solutions = @(
            "Solution1.sln"
           ,"Solution2.sln"
        )
        function Log
        # Note: Since Log function is nested within BuildDatabaseSolutions, it inherits the following variables 
        # defined within scope of BuildDatabaseSolutions:
        # $msg - message being displayed in console (and optionally logged)
        # $ForegroundColor - color of message in console
        # $logFile - log file name
        {
            param
            (
                [parameter(Mandatory=$false)][switch]$WriteToLog
            )
            process
            {
                if ($WriteToLog.IsPresent) { $msg >> $logFile }
                Write-Host $msg -ForegroundColor $ForegroundColor
            }
        }
        $startingDir = $(Get-Item -path .).FullName
        # Define log file name
        $logFile = "$LOGDIR\BuildLog_$(Get-Date -f yyyy-MM-dd_HHmmss).log"
        # Change directory to $GITROOT for processing
        Set-Location $GITROOT
        $gitBranch = $(git rev-parse --abbrev-ref HEAD)

        # If logging to file, create file
        if ($loggingEnabled) { New-Item -Path $logFile -ItemType file -Force > $null }

        $msg = "Git Branch: $gitBranch"
        $ForegroundColor = 'Blue'
        if ($loggingEnabled) { Log -WriteToLog } else { Log }

        foreach ($sol in $solutions)
        {
            # Get full path of solution
            $path = $(Get-ChildItem -recurse $sol).FullName

            $sw = [Diagnostics.Stopwatch]::StartNew()
            $msg = "Cleaning $($path)"
            $ForegroundColor = 'Green'
            if ($loggingEnabled)
            {
                Log -WriteToLog
                # Tee-Object sends output of command to both $logFile and the console
                & "$($msBuildExe)" "$($path)" /t:Clean /m /v:$verbosity | Tee-Object -Append $logFile | Write-Host
            }
            else
            {
                Log
                & "$($msBuildExe)" "$($path)" /t:Clean /m /v:$verbosity
            }
            

            $msg = "Building $($path)"
            $ForegroundColor = 'Green'
            if ($loggingEnabled)
            {
                Log -WriteToLog
                & "$($msBuildExe)" "$($path)" /t:Build /m /v:$verbosity | Tee-Object -Append $logFile | Write-Host
            }
            else
            {
                Log
                & "$($msBuildExe)" "$($path)" /t:Build /m /v:$verbosity
            }
            $sw.Stop()

            $msg = "Cleaned and built solution $sol in $($sw.Elapsed)"
            $ForegroundColor = 'Blue'
            if ($loggingEnabled) { Log -WriteToLog } else { Log }
        }
        Set-Location $startingDir
    }
}

BuildDatabaseSolutions #-verbosity 'Normal' -loggingEnabled $false