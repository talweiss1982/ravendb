using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Basic;
using Newtonsoft.Json.Linq;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Xunit.Sdk;

namespace Tryouts
{
   
    public static class Program
    {
        private static Process CreateServerProcess()
        {
            var jsonSettings = new JObject
            {
                ["RunInMemory"] = false,
                ["Testing.ParentProcessId"] = Process.GetCurrentProcess().Id,
                ["Setup.Mode"] = "None",
                ["License.Eula.Accepted"] = true,
                ["Security.UnsecuredAccessAllowed"] = "PublicNetwork"
            };
            var path = @"C:\Work\ravendb4\src\Raven.Server\bin\Release\netcoreapp2.2\win-x64\publish\";
            File.WriteAllText(Path.Combine(path , "settings.json"), jsonSettings.ToString());
            var process = new Process()
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = Path.Combine(path , "Raven.Server.exe"),
                    Arguments = $"-c=\"{Path.Combine(path , "settings.json")}\"",
                    CreateNoWindow = true,
                    ErrorDialog = false,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    RedirectStandardInput = true
                }
            };
            return process;
        }

        public static void Main(string[] args)
        {
            using (var test = new StressTests.Server.Replication.ExternalReplicationStressTests())
            {
                test.ExternalReplicationShouldWorkWithSmallTimeoutStress();
            }
            
        }

        private static void RavenProcess_ErrorDataReceived(object sender, DataReceivedEventArgs e)
        {
            Console.Out.WriteLine(e.Data);
        }

        private static void RavenProcess_OutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            Console.Out.WriteLine(e.Data);
        }
    }
}
