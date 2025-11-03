// CrmInvoker/Program.cs  — C# 7.3–compatible
// Reads { "body": "<raw docusign json>" } from STDIN, executes your CRM Action,
// prints a tiny JSON result to STDOUT.

using System;
using System.Configuration;
using System.IO;
using System.Text;

using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Tooling.Connector;
using Nancy.Json;

namespace CrmInvoker
{
    internal static class Program
    {
        private static int Main()
        {
            // 1) read all stdin
            string input;
            using (var sr = new StreamReader(Console.OpenStandardInput(), Encoding.UTF8))
                input = sr.ReadToEnd();

            var ser = new JavaScriptSerializer();

            // 2) extract the "body" property without nullable features
            string body = "";
            try
            {
                var obj = ser.DeserializeObject(input) as System.Collections.Generic.Dictionary<string, object>;
                object v;
                if (obj != null && obj.TryGetValue("body", out v) && v != null)
                    body = v.ToString();
            }
            catch (Exception ex)
            {
                Console.Out.WriteLine(ser.Serialize(new
                {
                    ok = false,
                    source = "invoker",
                    where = "parse-stdin",
                    error = ex.Message
                }));
                return 1;
            }

            // 3) read CRM config from App.config
            var cs = ConfigurationManager.ConnectionStrings["CRMConnectionString"];
            string crmConn = (cs != null) ? cs.ConnectionString : null;
            string actionName = ConfigurationManager.AppSettings["CrmActionName"] ?? "ntw_DocuSign_Ingress";
            string paramName = ConfigurationManager.AppSettings["CrmActionParamName"] ?? "RequestBody";

            if (string.IsNullOrWhiteSpace(crmConn))
            {
                Console.Out.WriteLine(ser.Serialize(new
                {
                    ok = false,
                    source = "crm",
                    where = "execute",
                    error = "CRM ConnectionString cannot be null or empty."
                }));
                return 2;
            }

            try
            {
                using (var client = new CrmServiceClient(crmConn))
                {
                    if (!client.IsReady)
                        throw new Exception("CrmServiceClient not ready: " + client.LastCrmError);

                    var req = new OrganizationRequest(actionName);
                    req[paramName] = body ?? "";

                    client.Execute(req);
                }

                Console.Out.WriteLine(ser.Serialize(new { ok = true, source = "crm", where = "execute" }));
                return 0;
            }
            catch (Exception ex)
            {
                Console.Out.WriteLine(ser.Serialize(new
                {
                    ok = false,
                    source = "crm",
                    where = "execute",
                    error = ex.Message
                }));
                return 2;
            }
        }
    }
}
