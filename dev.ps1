param(
    [string]$NgrokPort = "8000",
    [string]$UiPort = "8001"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

$services = @(
    @{
        Name = "ngrok"
        Command = "ngrok http $NgrokPort"
        Required = $true
        Env = @{}
    },
    @{
        Name = "inspector"
        Command = "uv run mcp dev mcp_server.py"
        Required = $false
        Env = @{
            MCP_EXPOSE_BROAD_DISCOVERY = "1"
            DMH_MCP_MODE = "debug"
        }
    },
    @{
        Name = "mcp-server"
        Command = "uv run mcp_server.py"
        Required = $true
        Env = @{
            MCP_TRANSPORT = "streamable-http"
            FASTMCP_HOST = "127.0.0.1"
            FASTMCP_PORT = $NgrokPort
            FASTMCP_LOG_LEVEL = "INFO"
            DMH_MCP_MODE = "prod"
        }
    },
    @{
        Name = "ui"
        Command = "uv run run_ui.py"
        Required = $true
        Env = @{
            UI_HOST = "127.0.0.1"
            UI_PORT = $UiPort
        }
    }
)

$jobs = New-Object System.Collections.Generic.List[object]
$serviceByName = @{}
$processedDone = New-Object 'System.Collections.Generic.HashSet[int]'

function Stop-PortListeners {
    param([int[]]$Ports)

    foreach ($port in ($Ports | Sort-Object -Unique)) {
        $listeners = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
        $procIds = @($listeners | Select-Object -ExpandProperty OwningProcess -Unique)

        foreach ($procId in $procIds) {
            if ($procId -and $procId -ne $PID) {
                Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

function Wait-PortReady {
    param(
        [int]$Port,
        [string]$ServiceName,
        [int]$TimeoutSeconds = 20
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
        if ($listeners.Count -gt 0) {
            Write-Host ("[{0}] listening on 127.0.0.1:{1}" -f $ServiceName, $Port)
            return $true
        }
        Start-Sleep -Milliseconds 250
    }

    Write-Host ("[{0}] failed to listen on 127.0.0.1:{1} within {2}s" -f $ServiceName, $Port, $TimeoutSeconds)
    return $false
}

function Stop-AllJobs {
    param([System.Collections.Generic.List[object]]$JobList)

    foreach ($job in $JobList) {
        if ($null -ne $job -and $job.State -eq "Running") {
            Stop-Job -Job $job -ErrorAction SilentlyContinue | Out-Null
        }
    }

    foreach ($job in $JobList) {
        if ($null -ne $job) {
            Wait-Job -Job $job -Timeout 5 -ErrorAction SilentlyContinue | Out-Null
            Remove-Job -Job $job -Force -ErrorAction SilentlyContinue | Out-Null
        }
    }
}

$requiredFailure = $false

try {
    foreach ($svc in $services) {
        $serviceByName[$svc.Name] = $svc

        $job = Start-Job -Name $svc.Name -ScriptBlock {
            param(
                [string]$Name,
                [string]$Command,
                [hashtable]$EnvMap,
                [string]$Root
            )

            Set-Location $Root

            foreach ($key in $EnvMap.Keys) {
                Set-Item -Path ("Env:" + $key) -Value $EnvMap[$key]
            }

            Write-Output ("[{0}] starting: {1}" -f $Name, $Command)

            try {
                Invoke-Expression $Command 2>&1 | ForEach-Object {
                    "[{0}] {1}" -f $Name, $_
                }

                if ($LASTEXITCODE -ne 0) {
                    throw "Command exited with code $LASTEXITCODE"
                }

                Write-Output ("[{0}] exited" -f $Name)
            }
            catch {
                Write-Output ("[{0}] failed: {1}" -f $Name, $_.Exception.Message)
                throw
            }
        } -ArgumentList $svc.Name, $svc.Command, $svc.Env, $projectRoot

        $jobs.Add($job) | Out-Null
    }

    Write-Host "All services started. Press Ctrl+C to stop."

    if (-not (Wait-PortReady -Port ([int]$NgrokPort) -ServiceName "mcp-server" -TimeoutSeconds 20)) {
        throw "mcp-server failed startup health check"
    }
    if (-not (Wait-PortReady -Port ([int]$UiPort) -ServiceName "ui" -TimeoutSeconds 20)) {
        throw "ui failed startup health check"
    }

    while ($true) {
        foreach ($job in $jobs) {
            Receive-Job -Job $job | ForEach-Object { Write-Host $_ }
        }

        $done = @($jobs | Where-Object { $_.State -in @("Completed", "Failed", "Stopped") })
        $newDone = @($done | Where-Object { -not $processedDone.Contains($_.Id) })

        foreach ($job in $newDone) {
            [void]$processedDone.Add($job.Id)

            $svc = $serviceByName[$job.Name]
            $requiredLabel = if ($svc.Required) { "required" } else { "optional" }
            Write-Host ("[{0}] state: {1} ({2})" -f $job.Name, $job.State, $requiredLabel)

            if ($svc.Required) {
                $requiredFailure = $true
            }
        }

        if ($requiredFailure) {
            throw "At least one required service stopped. Shutting down all services."
        }

        Start-Sleep -Milliseconds 300
    }
}
finally {
    Write-Host "Stopping services..."
    Stop-AllJobs -JobList $jobs
    Stop-PortListeners -Ports @([int]$NgrokPort, [int]$UiPort)
}