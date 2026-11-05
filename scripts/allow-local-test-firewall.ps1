# Run explicitly in an elevated PowerShell 7 session. Only these repository
# test programs may accept TCP connections on the IPv4 loopback used by fixtures.
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$reportDirectory = Join-Path $repoRoot 'bin\windows-tests'
[IO.Directory]::CreateDirectory($reportDirectory) | Out-Null
$reportPath = Join-Path $reportDirectory 'firewall-setup.txt'
trap {
    [IO.File]::WriteAllText($reportPath, ($_ | Out-String))
    exit 1
}

if (-not $IsWindows) {
    throw 'This firewall setup requires PowerShell 7 on Windows.'
}
$principal = [Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'An elevated PowerShell session is required to change Windows firewall rules.'
}

$hash = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($repoRoot))).Substring(0, 12)
$packages = @(
    'apps\job\rpc\internal\config',
    'apps\job\rpc\internal\execution',
    'apps\system\api\internal\jobrpc',
    'apps\system\api\internal\svc',
    'pkg\jobexecutor',
    'pkg\rpchost'
)
foreach ($package in $packages) {
    $packageKey = $package.Replace('\', '__')
    $ruleName = 'DogX-LocalTests-' + $hash + '-' + $packageKey
    $parameters = @{
        Name = $ruleName
        DisplayName = 'DogX local test: ' + $package
        Group = 'DogX local Go tests'
        Description = 'Local TCP tests only; does not permit LAN or public-network clients.'
        Program = Join-Path $repoRoot ('bin\windows-tests\' + $packageKey + '.test.exe')
        Direction = 'Inbound'
        Action = 'Allow'
        Enabled = 'True'
        Profile = 'Any'
        Protocol = 'TCP'
        LocalAddress = '127.0.0.1'
        RemoteAddress = '127.0.0.1'
        EdgeTraversalPolicy = 'Block'
    }
    if (Get-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue) {
        $updateParameters = $parameters.Clone()
        $updateParameters.Remove('DisplayName')
        $updateParameters.Remove('Group')
        Set-NetFirewallRule @updateParameters | Out-Null
    } else {
        New-NetFirewallRule @parameters | Out-Null
    }
}
$report = 'Configured ' + $packages.Count + ' loopback-only test program rules.'
[IO.File]::WriteAllText($reportPath, $report)
Write-Output $report
