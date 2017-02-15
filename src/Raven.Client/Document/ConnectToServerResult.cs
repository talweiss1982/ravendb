using System.IO;

namespace Raven.Client.Document
{
    public class ConnectToServerResult
    {
        public Stream Stream { get; set; }
        public string OAuthToken { get; set; }
    }
}
