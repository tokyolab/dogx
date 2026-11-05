# Used by go test -exec so Windows firewall rules target stable executable paths.
# This runner never changes firewall policy or requires elevated permissions.
$ErrorActionPreference = 'Stop'

if (-not $IsWindows) {
    throw 'This test runner requires PowerShell 7 on Windows.'
}
if ($args.Count -lt 1) {
    throw 'A Go test executable is required.'
}

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$packageDirectory = [IO.Path]::GetFullPath((Get-Location).Path)
$relativePackage = [IO.Path]::GetRelativePath($repoRoot, $packageDirectory)
if ([IO.Path]::IsPathRooted($relativePackage) -or $relativePackage -eq '..' -or $relativePackage.StartsWith('..\')) {
    throw 'The test package must be inside this repository.'
}
$source = (Resolve-Path -LiteralPath $args[0]).Path
if (-not $source.EndsWith('.test.exe', [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The input must be a compiled Go test executable.'
}

# Go runs tests from each package directory. Keep the complete relative path
# so packages named config, logic or svc do not overwrite one another.
$packageKey = $relativePackage.Replace('\', '__').Replace('/', '__')
if ($packageKey -eq '.') { $packageKey = 'root' }
$outputDirectory = Join-Path $repoRoot 'bin\windows-tests'
[IO.Directory]::CreateDirectory($outputDirectory) | Out-Null
$target = Join-Path $outputDirectory ($packageKey + '.test.exe')
Copy-Item -LiteralPath $source -Destination $target -Force
$testArguments = @($args | Select-Object -Skip 1)

# go test can forcibly terminate this dedicated PowerShell process. Attach the
# wrapper before launching children so that Windows also terminates the test
# and its subprocesses when the wrapper's non-inheritable job handle closes.
Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace DogX.LocalTests
{
    public static class TestProcessJob
    {
        // Retain the raw handle until process exit. Closing it after assigning
        // this process would terminate the wrapper before it returns the test's
        // exit code; finalizers must not take ownership of this handle.
        private static IntPtr ownedJob;

        [StructLayout(LayoutKind.Sequential)]
        private struct BasicLimits
        {
            public long PerProcessUserTimeLimit;
            public long PerJobUserTimeLimit;
            public uint LimitFlags;
            public UIntPtr MinimumWorkingSetSize;
            public UIntPtr MaximumWorkingSetSize;
            public uint ActiveProcessLimit;
            public UIntPtr Affinity;
            public uint PriorityClass;
            public uint SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct IoCounters
        {
            public ulong ReadOperationCount;
            public ulong WriteOperationCount;
            public ulong OtherOperationCount;
            public ulong ReadTransferCount;
            public ulong WriteTransferCount;
            public ulong OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct ExtendedLimits
        {
            public BasicLimits BasicLimitInformation;
            public IoCounters IoInfo;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr CreateJobObject(IntPtr attributes, string name);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool SetInformationJobObject(IntPtr job, int informationClass,
            ref ExtendedLimits information, uint length);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);

        [DllImport("kernel32.dll")]
        private static extern IntPtr GetCurrentProcess();

        [DllImport("kernel32.dll")]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr handle);

        public static void OwnProcessTree()
        {
            if (ownedJob != IntPtr.Zero)
                throw new InvalidOperationException("Test process ownership is already installed.");

            // Null security attributes create a handle children cannot inherit.
            IntPtr job = CreateJobObject(IntPtr.Zero, null);
            if (job == IntPtr.Zero)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Create test process job failed.");

            var limits = new ExtendedLimits();
            limits.BasicLimitInformation.LimitFlags = 0x2000; // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if (!SetInformationJobObject(job, 9, ref limits,
                (uint)Marshal.SizeOf(typeof(ExtendedLimits)))) // JobObjectExtendedLimitInformation
            {
                int error = Marshal.GetLastWin32Error();
                CloseHandle(job);
                throw new Win32Exception(error, "Configure test process job failed.");
            }
            if (!AssignProcessToJobObject(job, GetCurrentProcess()))
            {
                int error = Marshal.GetLastWin32Error();
                CloseHandle(job);
                throw new Win32Exception(error, "Assign test wrapper to process job failed.");
            }
            ownedJob = job;
        }
    }
}
'@
[DogX.LocalTests.TestProcessJob]::OwnProcessTree()
& $target @testArguments
exit $LASTEXITCODE
