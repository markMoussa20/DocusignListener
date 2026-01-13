// CrmInvoker/Program.cs — custom-API-only build
using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using Newtonsoft.Json.Linq;

namespace CrmInvoker
{
    internal static class Program
    {
        private const string VERSION = "CRMINVOKER-APIONLY-2025-11-12T15:00Z";
        private const int STATE_INACTIVE = 1;
        private const int STATUS_SIGNED = 202370005;

        private static int Main()
        {
            Console.Error.WriteLine($"[BOOT] {VERSION}");
            Console.Error.WriteLine($"[BOOT] EXE: {System.Reflection.Assembly.GetExecutingAssembly().Location}");
            Console.Error.WriteLine("[BOOT] USING CUSTOM API: ntw_UploadBase64ToFile");

            ServicePointManager.SecurityProtocol =
                SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

            // 1) Read stdin
            string stdin;
            using (var sr = new StreamReader(Console.OpenStandardInput(), Encoding.UTF8))
                stdin = sr.ReadToEnd();

            // 2) Parse wrapper { body: "..." }
            string rawBody = "";
            try { rawBody = (string)JToken.Parse(string.IsNullOrWhiteSpace(stdin) ? "{}" : stdin)["body"] ?? ""; }
            catch (Exception ex)
            {
                WriteResult(false, "invoker", "parse-stdin", ex.Message);
                return 1;
            }

            // 3) Parse DocuSign JSON (tolerant)
            string eventName = "", envelopeId = "", status = "", emailSubject = "", senderUser = "", senderMail = "", validationTokenIncoming = "";
            string pdfBase64 = "", docName = "";

            try
            {
                var root = string.IsNullOrWhiteSpace(rawBody) ? null : JToken.Parse(rawBody);
                eventName = (string)root?["event"] ?? (string)root?["eventType"] ?? (string)root?["type"] ?? "";
                envelopeId = (string)root?["data"]?["envelopeId"] ?? (string)root?["envelopeId"] ?? "";
                status = (string)root?["data"]?["envelopeSummary"]?["status"] ?? (string)root?["summary"]?["status"] ?? (string)root?["status"] ?? "";
                emailSubject = (string)root?["data"]?["envelopeSummary"]?["emailSubject"] ?? (string)root?["summary"]?["emailSubject"] ?? (string)root?["emailSubject"] ?? "";
                senderUser = (string)root?["data"]?["envelopeSummary"]?["sender"]?["userName"] ?? (string)root?["summary"]?["senderUserName"] ?? (string)root?["sender"]?["userName"] ?? "";
                senderMail = (string)root?["data"]?["envelopeSummary"]?["sender"]?["email"] ?? (string)root?["summary"]?["senderEmail"] ?? (string)root?["sender"]?["email"] ?? "";
                validationTokenIncoming = (string)root?["validationToken"] ?? "";

                var doc = root?["data"]?["envelopeSummary"]?["envelopeDocuments"]?.First;
                if (doc != null)
                {
                    pdfBase64 = (string)doc?["PDFBytes"] ?? (string)doc?["pdfBytes"] ?? "";
                    var n = (string)doc?["name"] ?? "SignedDocument";
                    docName = EnsurePdfName(n);
                }
            }
            catch (Exception ex) { Console.Error.WriteLine("[WARN] JSON parse: " + ex.Message); }

            if (string.IsNullOrWhiteSpace(eventName) || string.IsNullOrWhiteSpace(envelopeId))
            {
                WriteResult(false, "crm", "execute", "Missing required fields (event / envelopeId) in DocuSign payload.");
                return 2;
            }

            // 4) CRM connect
            var crmConnString = ConfigurationManager.ConnectionStrings["CRMConnectionString"]?.ConnectionString;
            try
            {
                using (var client = new CrmServiceClient(crmConnString))
                {
                    if (!client.IsReady)
                        throw new Exception("CrmServiceClient not ready: " + (client.LastCrmError ?? "unknown"));

                    var org = (IOrganizationService)client.OrganizationServiceProxy ?? client;
                    var correlationId = Guid.NewGuid().ToString("N");

                    var webhookLogId = CreateWebhookLog(org, correlationId, eventName, envelopeId, status, senderUser, senderMail, rawBody);

                    // Validate source (optional)
                    if (!IsValidSourceToken(org, ConfigurationManager.AppSettings["Env.WebhookValidationToken.Schema"] ?? "", validationTokenIncoming))
                    {
                        MarkWebhookLogFinal(org, webhookLogId, false, "Invalid validationToken");
                        WriteResult(false, "crm", "execute", "Invalid validationToken");
                        return 2;
                    }

                    // Find ntw_docusignlog by envelope id
                    var docusignLog = FindDocuSignLog(org, envelopeId);
                    if (docusignLog == null)
                    {
                        MarkWebhookLogFinal(org, webhookLogId, false, "No ntw_docusignlog for envelopeId");
                        WriteResult(false, "crm", "execute", "No ntw_docusignlog found for envelopeId");
                        return 2;
                    }
                    var docusignLogId = docusignLog.Id;

                    // Small audit note
                    CreateNote(org, docusignLogId, "DocuSign Event: " + eventName, BuildNoteBody(status, emailSubject, senderUser, senderMail));

                    // 6) On completed: upload via CUSTOM API only
                    if (IsCompleted(eventName))
                    {
                        if (!string.IsNullOrWhiteSpace(pdfBase64))
                        {
                            var fileName = string.IsNullOrWhiteSpace(docName) ? "SignedDocument.pdf" : docName;
                            var base64Payload = StripDataPrefix(pdfBase64);

                            Console.Error.WriteLine("[UPLOAD] Execute ntw_UploadBase64ToFile → ntw_docusignlog.ntw_signeddocument");
                            var req = new OrganizationRequest("ntw_UploadBase64ToFile")
                            {
                                ["EntityLogicalName"] = "ntw_docusignlog",
                                ["RecordId"] = docusignLogId.ToString(),
                                ["FileAttributeLogicalName"] = "ntw_signeddocument",
                                ["FileName"] = fileName,
                                ["Base64Payload"] = base64Payload,
                                ["VerboseTrace"] = true // matches screenshot: Boolean, optional=yes
                            };

                            try
                            {
                                var resp = org.Execute(req);

                                // Screenshot shows Response Properties: Field (string), MimeType (string), FileSize (int)
                                var field = resp.Results.Contains("Field") ? resp.Results["Field"] as string : null;
                                var mime = resp.Results.Contains("MimeType") ? resp.Results["MimeType"] as string : null;
                                var size = resp.Results.Contains("FileSize") ? Convert.ToInt32(resp.Results["FileSize"]) : 0;

                                var audit = new StringBuilder()
                                    .AppendLine("Stored via ntw_UploadBase64ToFile")
                                    .AppendLine("Entity: ntw_docusignlog")
                                    .AppendLine("Attribute: ntw_signeddocument")
                                    .AppendLine("Name: " + fileName)
                                    .AppendLine("MimeType: " + (mime ?? "<n/a>"))
                                    .AppendLine("FileSize: " + size + " bytes")
                                    .AppendLine("Field: " + (field ?? "<n/a>"))
                                    .ToString();

                                CreateNote(org, docusignLogId, "Signed PDF stored", audit);
                            }
                            catch (Exception ex)
                            {
                                CreateNote(org, docusignLogId, "Signed PDF upload failed", ex.ToString());
                                throw;
                            }
                        }

                        // Mark as inactive/signed
                        var upd = new Entity("ntw_docusignlog", docusignLogId)
                        {
                            ["statecode"] = new OptionSetValue(STATE_INACTIVE),
                            ["statuscode"] = new OptionSetValue(STATUS_SIGNED)
                        };
                        org.Update(upd);

                        MarkWebhookLogFinal(org, webhookLogId, true, null);
                        WriteResult(true, "crm", "execute", "completed", envelopeId);
                        return 0;
                    }

                    // Other cases
                    if (IsFinishLater(eventName)) { MarkWebhookLogFinal(org, webhookLogId, true, "Finish later"); WriteResult(true, "crm", "execute", "finish-later", envelopeId); return 0; }
                    if (IsDeclined(eventName)) { MarkWebhookLogFinal(org, webhookLogId, true, "Declined"); WriteResult(true, "crm", "execute", "declined", envelopeId); return 0; }

                    MarkWebhookLogFinal(org, webhookLogId, true, "Unhandled event");
                    WriteResult(true, "crm", "execute", "unhandled", envelopeId);
                    return 0;
                }
            }
            catch (Exception ex)
            {
                WriteResult(false, "crm", "execute", ex.Message);
                return 2;
            }
        }

        private static string BuildNoteBody(string summaryStatus, string emailSubject, string senderUserName, string senderEmail)
        {
            var sb = new StringBuilder();
            if (!string.IsNullOrWhiteSpace(summaryStatus)) sb.AppendLine("Status: " + summaryStatus);
            if (!string.IsNullOrWhiteSpace(emailSubject)) sb.AppendLine("Subject: " + emailSubject);
            if (!string.IsNullOrWhiteSpace(senderUserName) || !string.IsNullOrWhiteSpace(senderEmail))
                sb.AppendLine("Sender: " + senderUserName + (string.IsNullOrWhiteSpace(senderEmail) ? "" : (" <" + senderEmail + ">")));
            return sb.Length == 0 ? "DocuSign event received." : sb.ToString();
        }

        // ---------- helpers ----------
        private static void WriteResult(bool ok, string source, string where, string message, string envelopeId = null)
        {
            Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(
                envelopeId == null
                    ? new { ok, source, where, error = ok ? null : message, handled = ok ? message : null }
                    : new { ok, source, where, handled = message, envelopeId }
            ));
        }

        private static string StripDataPrefix(string b64)
        {
            var s = (b64 ?? "").Trim();
            var i = s.IndexOf("base64,", StringComparison.OrdinalIgnoreCase);
            return i >= 0 ? s.Substring(i + "base64,".Length) : s;
        }

        private static string EnsurePdfName(string name)
        {
            var n = (name ?? "Document").Trim();
            var i = n.LastIndexOf('.');
            if (i > 0) n = n.Substring(0, i);
            return n + ".pdf";
        }

        private static bool IsCompleted(string evt) =>
            evt.Equals("recipient-completed", StringComparison.OrdinalIgnoreCase) ||
            evt.Equals("envelope-completed", StringComparison.OrdinalIgnoreCase);
        private static bool IsFinishLater(string evt) => evt.Equals("recipient-finish-later", StringComparison.OrdinalIgnoreCase);
        private static bool IsDeclined(string evt) => evt.Equals("recipient-declined", StringComparison.OrdinalIgnoreCase);

        private static Guid CreateWebhookLog(IOrganizationService org, string corr, string evt, string envId,
            string summaryStatus, string senderUser, string senderMail, string payloadRaw)
        {
            const string Entity = "ntw_webhooklog", Name = "ntw_name", Source = "ntw_sourcesystem",
                         Payload = "ntw_payload", Corr = "ntw_correlationid", State = "statecode", Status = "statuscode";
            var activeState = int.TryParse(ConfigurationManager.AppSettings["WebhookLog.ActiveState"], out var a) ? a : 0;
            var receivedStatus = int.TryParse(ConfigurationManager.AppSettings["WebhookLog.ReceivedStatus"], out var b) ? b : 1;

            var e = new Entity(Entity);
            e[Name] = $"DocuSign Webhook {DateTime.UtcNow:u}";
            e[Source] = "DocuSign";
            e[Payload] = new JObject { ["event"] = evt ?? "", ["envelopeId"] = envId ?? "", ["status"] = summaryStatus ?? "", ["sender"] = senderUser ?? "", ["email"] = senderMail ?? "" }.ToString(Newtonsoft.Json.Formatting.None);
            e[Corr] = corr;
            e[State] = new OptionSetValue(activeState);
            e[Status] = new OptionSetValue(receivedStatus);
            return org.Create(e);
        }

        private static void MarkWebhookLogFinal(IOrganizationService org, Guid id, bool success, string reason)
        {
            const string Entity = "ntw_webhooklog", State = "statecode", Status = "statuscode", Payload = "ntw_payload";
            var inactiveState = int.TryParse(ConfigurationManager.AppSettings["WebhookLog.InactiveState"], out var a) ? a : 1;
            var processedStatus = int.TryParse(ConfigurationManager.AppSettings["WebhookLog.ProcessedStatus"], out var b) ? b : 2;
            var failedStatus = int.TryParse(ConfigurationManager.AppSettings["WebhookLog.FailedStatus"], out var c) ? c : 202370000;

            var upd = new Entity(Entity, id);
            upd[State] = new OptionSetValue(inactiveState);
            upd[Status] = new OptionSetValue(success ? processedStatus : failedStatus);
            if (!success && !string.IsNullOrWhiteSpace(reason)) upd[Payload] = "[FAIL:" + reason + "]";
            org.Update(upd);
        }

        private static bool IsValidSourceToken(IOrganizationService org, string schemaName, string incomingToken)
        {
            if (string.IsNullOrWhiteSpace(schemaName)) return true;
            var defQ = new QueryExpression("environmentvariabledefinition") { ColumnSet = new ColumnSet("environmentvariabledefinitionid", "schemaname") };
            defQ.Criteria.AddCondition("schemaname", ConditionOperator.Equal, schemaName);
            var defs = org.RetrieveMultiple(defQ);
            if (defs.Entities.Count == 0) return true;

            var valQ = new QueryExpression("environmentvariablevalue") { ColumnSet = new ColumnSet("value", "environmentvariabledefinitionid") };
            valQ.Criteria.AddCondition("environmentvariabledefinitionid", ConditionOperator.Equal, defs.Entities[0].Id);
            var vals = org.RetrieveMultiple(valQ);
            var expected = vals.Entities.Count == 0 ? null : vals.Entities[0].GetAttributeValue<string>("value");
            if (string.IsNullOrWhiteSpace(expected)) return true;

            if (string.IsNullOrWhiteSpace(incomingToken)) return false;
            return string.Equals(expected.Trim(), incomingToken.Trim(), StringComparison.Ordinal);
        }

        private static Entity FindDocuSignLog(IOrganizationService org, string envId)
        {
            const string Entity = "ntw_docusignlog", EnvelopeField = "ntw_envelopid";
            var q = new QueryExpression(Entity) { ColumnSet = new ColumnSet("activityid") };
            q.Criteria.AddCondition(EnvelopeField, ConditionOperator.Equal, envId);
            var r = org.RetrieveMultiple(q);
            return r.Entities.FirstOrDefault();
        }

        private static void CreateNote(IOrganizationService org, Guid logId, string subject, string body)
        {
            var note = new Entity("annotation");
            note["subject"] = string.IsNullOrWhiteSpace(subject) ? "DocuSign" : subject;
            note["notetext"] = string.IsNullOrWhiteSpace(body) ? subject : body;
            note["objectid"] = new EntityReference("ntw_docusignlog", logId);
            org.Create(note);
        }
    }
}
