// CrmInvoker/Program.cs  — .NET Framework 4.8
// NuGet: Microsoft.CrmSdk.CoreAssemblies 9.0.2.60
//        Microsoft.CrmSdk.XrmTooling.CoreAssembly 9.1.1.65
//        System.Configuration.ConfigurationManager 5.x
//        Newtonsoft.Json 13.x
//
// Reads { body: "<RAW_DOCUSIGN_JSON>" } from STDIN (written by the listener)
// and does ALL business logic directly against CRM (no Custom API call).
//
// Behavior:
// - Create ntw_webhooklog (Active/Received) for every call
// - Validate "validationToken" (if ENV configured)
// - Find ntw_docusignlog by envelopeId
// - Add audit note
// - On completed: attach PDF (file + note), mark Completed (config-driven)
// - On finish-later: mark as In-Progress (config-driven)
// - On declined: mark as Canceled/Declined (config-driven)
// - Mark webhooklog Processed/Failed accordingly
//
// Configure in CrmInvoker.exe.config (see section 3).

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
        private static int Main()
        {
            // TLS
            ServicePointManager.SecurityProtocol =
                SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;

            // 1) Read STDIN
            string stdin;
            using (var sr = new StreamReader(Console.OpenStandardInput(), Encoding.UTF8))
                stdin = sr.ReadToEnd();

            // Expect: { "body": "<RAW_DOCUSIGN_JSON>" }
            string rawBody = "";
            try
            {
                var outer = JToken.Parse(string.IsNullOrWhiteSpace(stdin) ? "{}" : stdin);
                rawBody = (string?)outer["body"] ?? "";
            }
            catch (Exception ex)
            {
                Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                {
                    ok = false,
                    source = "invoker",
                    where = "parse-stdin",
                    error = ex.Message
                }));
                return 1;
            }

            // 2) Parse DocuSign JSON (tolerant)
            string eventName = "";
            string envelopeId = "";
            string status = "";
            string emailSubject = "";
            string senderUser = "";
            string senderMail = "";
            string validationTokenIncoming = "";
            string pdfBase64 = "";
            string docName = "";

            try
            {
                var root = string.IsNullOrWhiteSpace(rawBody) ? null : JToken.Parse(rawBody);

                eventName =
                       (string?)root?["event"]
                    ?? (string?)root?["eventType"]
                    ?? (string?)root?["type"]
                    ?? "";

                envelopeId =
                       (string?)root?["data"]?["envelopeId"]
                    ?? (string?)root?["envelopeId"]
                    ?? "";

                status =
                       (string?)root?["data"]?["envelopeSummary"]?["status"]
                    ?? (string?)root?["summary"]?["status"]
                    ?? (string?)root?["status"]
                    ?? "";

                emailSubject =
                       (string?)root?["data"]?["envelopeSummary"]?["emailSubject"]
                    ?? (string?)root?["summary"]?["emailSubject"]
                    ?? (string?)root?["emailSubject"]
                    ?? "";

                senderUser =
                       (string?)root?["data"]?["envelopeSummary"]?["sender"]?["userName"]
                    ?? (string?)root?["summary"]?["senderUserName"]
                    ?? (string?)root?["sender"]?["userName"]
                    ?? "";

                senderMail =
                       (string?)root?["data"]?["envelopeSummary"]?["sender"]?["email"]
                    ?? (string?)root?["summary"]?["senderEmail"]
                    ?? (string?)root?["sender"]?["email"]
                    ?? "";

                // Optional validation token (some listeners put it in the JSON)
                validationTokenIncoming = (string?)root?["validationToken"] ?? "";

                // First document
                var doc = root?["data"]?["envelopeSummary"]?["envelopeDocuments"]?.First;
                if (doc != null)
                {
                    pdfBase64 = (string?)doc?["PDFBytes"] ?? (string?)doc?["pdfBytes"] ?? "";
                    var n = (string?)doc?["name"] ?? "SignedDocument";
                    docName = EnsurePdfName(n);
                }
            }
            catch (Exception ex)
            {
                // proceed; CRM will see missing requireds
                Console.Error.WriteLine("[Invoker] JSON parse warning: " + ex.Message);
            }

            // Quick pre-flight: event + envelopeId are mandatory for our logic
            if (string.IsNullOrWhiteSpace(eventName) || string.IsNullOrWhiteSpace(envelopeId))
            {
                Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                {
                    ok = false,
                    source = "crm",
                    where = "execute",
                    error = "Missing required fields (event / envelopeId) in DocuSign payload."
                }));
                return 2;
            }

            // 3) CRM connection
            var crmConnString = ConfigurationManager.ConnectionStrings["CRMConnectionString"]?.ConnectionString;

            try
            {
                using (var client = new CrmServiceClient(crmConnString))
                {
                    if (!client.IsReady)
                        throw new Exception("CrmServiceClient not ready: " + (client.LastCrmError ?? "unknown"));

                    var org = (IOrganizationService)client.OrganizationServiceProxy ?? client;

                    // Correlation for this run
                    var correlationId = Guid.NewGuid().ToString();

                    // 4) Create webhook log (Active/Received)
                    var webhookLogId = CreateWebhookLog(org,
                        correlationId,
                        eventName,
                        envelopeId,
                        status,
                        senderUser,
                        senderMail,
                        rawBody);

                    // 5) Validate source token (optional)
                    if (!IsValidSourceToken(org, validationTokenIncoming))
                    {
                        MarkWebhookLogFinal(org, webhookLogId, success: false, failedReason: "Invalid validationToken");
                        Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                        {
                            ok = false,
                            source = "crm",
                            where = "execute",
                            error = "Invalid validationToken"
                        }));
                        return 2;
                    }

                    // 6) Find DocuSign log by envelopeId
                    var docusignLog = FindDocuSignLog(org, envelopeId);
                    if (docusignLog == null)
                    {
                        MarkWebhookLogFinal(org, webhookLogId, success: false, failedReason: "No ntw_docusignlog for envelopeId");
                        Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                        {
                            ok = false,
                            source = "crm",
                            where = "execute",
                            error = "No ntw_docusignlog found for envelopeId"
                        }));
                        return 2;
                    }

                    var docusignLogId = docusignLog.Id;

                    // 7) Add audit note
                    var noteBody = BuildNoteBody(status, emailSubject, senderUser, senderMail);
                    CreateNoteOnDocusignLog(org, docusignLogId,
                        subject: $"DocuSign Event: {eventName}",
                        body: noteBody);

                    // 8) Branch by event
                    if (IsCompleted(eventName))
                    {
                        // attach PDF (file column + note)
                        if (!string.IsNullOrWhiteSpace(pdfBase64))
                        {
                            TryAttachSignedPdfToDocusignLog(org, docusignLogId, pdfBase64);  // file column if exists
                            // also keep a note copy
                            CreateNoteOnDocusignLog(org, docusignLogId,
                                subject: "Signed Document",
                                body: "Final signed PDF attached.",
                                pdfBase64Attachment: pdfBase64,
                                pdfName: string.IsNullOrWhiteSpace(docName) ? "SignedDocument.pdf" : docName,
                                mimeType: "application/pdf");
                        }

                        // set Completed (config-driven)
                        SetDocuSignLogStateAndStatus_Config(org, docusignLogId, "Completed");

                        MarkWebhookLogFinal(org, webhookLogId, success: true, failedReason: null);
                        Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                        {
                            ok = true,
                            source = "crm",
                            where = "execute",
                            handled = "completed",
                            envelopeId
                        }));
                        return 0;
                    }
                    else if (IsFinishLater(eventName))
                    {
                        SetDocuSignLogStateAndStatus_Config(org, docusignLogId, "FinishLater");
                        MarkWebhookLogFinal(org, webhookLogId, success: true, failedReason: null);
                        Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                        {
                            ok = true,
                            source = "crm",
                            where = "execute",
                            handled = "finish-later",
                            envelopeId
                        }));
                        return 0;
                    }
                    else if (IsDeclined(eventName))
                    {
                        SetDocuSignLogStateAndStatus_Config(org, docusignLogId, "Declined");
                        MarkWebhookLogFinal(org, webhookLogId, success: true, failedReason: null);
                        Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                        {
                            ok = true,
                            source = "crm",
                            where = "execute",
                            handled = "declined",
                            envelopeId
                        }));
                        return 0;
                    }
                    else
                    {
                        // Unhandled event: just mark processed
                        MarkWebhookLogFinal(org, webhookLogId, success: true, failedReason: "Unhandled event");
                        Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                        {
                            ok = true,
                            source = "crm",
                            where = "execute",
                            handled = "unhandled",
                            eventName,
                            envelopeId
                        }));
                        return 0;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Out.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(new
                {
                    ok = false,
                    source = "crm",
                    where = "execute",
                    error = ex.Message
                }));
                return 2;
            }
        }

        // —— Helpers ———————————————————————————————————————————————————————

        private static string EnsurePdfName(string nameFromPayload)
        {
            var n = (nameFromPayload ?? "Document").Trim();
            var i = n.LastIndexOf('.');
            if (i > 0) n = n.Substring(0, i);
            return n + ".pdf";
        }

        private static bool IsCompleted(string evt) =>
            evt.Equals("recipient-completed", StringComparison.OrdinalIgnoreCase) ||
            evt.Equals("envelope-completed", StringComparison.OrdinalIgnoreCase);

        private static bool IsFinishLater(string evt) =>
            evt.Equals("recipient-finish-later", StringComparison.OrdinalIgnoreCase);

        private static bool IsDeclined(string evt) =>
            evt.Equals("recipient-declined", StringComparison.OrdinalIgnoreCase);

        // ——— Config accessors (from appSettings) ———

        private static string GetAppSetting(string key, string fallback = "")
            => ConfigurationManager.AppSettings[key] ?? fallback;

        private static int? GetIntSetting(string key)
            => int.TryParse(ConfigurationManager.AppSettings[key], out var v) ? v : (int?)null;

        // ——— CRM ops ———

        private static Guid CreateWebhookLog(
            IOrganizationService org,
            string correlationId,
            string eventName,
            string envelopeId,
            string summaryStatus,
            string senderUserName,
            string senderEmail,
            string payloadRaw)
        {
            // ENTITY / FIELD NAMES — set to yours
            const string Entity = "ntw_webhooklog";
            const string Name = "ntw_name";
            const string Source = "ntw_sourcesystem";
            const string Payload = "ntw_payload";
            const string Corr = "ntw_correlationid";
            const string State = "statecode";
            const string Status = "statuscode";

            // status values (Active/Received) from config to avoid hardcoding
            var activeState = GetIntSetting("WebhookLog.ActiveState") ?? 0;
            var receivedStatus = GetIntSetting("WebhookLog.ReceivedStatus") ?? 1;

            var name = $"DocuSign Webhook {DateTime.UtcNow:u}";

            var e = new Entity(Entity);
            e[Name] = name;
            e[Source] = "DocuSign";
            e[Payload] = BuildCompactAuditJson(eventName, envelopeId, summaryStatus, senderUserName, senderEmail);
            e[Corr] = correlationId;
            e[State] = new OptionSetValue(activeState);
            e[Status] = new OptionSetValue(receivedStatus);

            var id = org.Create(e);
            return id;
        }

        private static void MarkWebhookLogFinal(
            IOrganizationService org,
            Guid webhookLogId,
            bool success,
            string failedReason)
        {
            const string Entity = "ntw_webhooklog";
            const string State = "statecode";
            const string Status = "statuscode";
            const string Payload = "ntw_payload";

            var inactiveState = GetIntSetting("WebhookLog.InactiveState") ?? 1;
            var processedStatus = GetIntSetting("WebhookLog.ProcessedStatus") ?? 2;
            var failedStatus = GetIntSetting("WebhookLog.FailedStatus") ?? 202370000;

            var upd = new Entity(Entity, webhookLogId);
            if (success)
            {
                upd[State] = new OptionSetValue(inactiveState);
                upd[Status] = new OptionSetValue(processedStatus);
            }
            else
            {
                upd[State] = new OptionSetValue(GetIntSetting("WebhookLog.InactiveState") ?? 1);
                upd[Status] = new OptionSetValue(failedStatus);
                if (!string.IsNullOrWhiteSpace(failedReason))
                    upd[Payload] = "[FAIL:" + failedReason + "]";
            }
            org.Update(upd);
        }

        private static string BuildCompactAuditJson(
            string eventName, string envelopeId, string status, string senderUser, string senderMail)
        {
            var o = new JObject
            {
                ["event"] = eventName ?? "",
                ["envelopeId"] = envelopeId ?? "",
                ["status"] = status ?? "",
                ["sender"] = senderUser ?? "",
                ["email"] = senderMail ?? ""
            };
            return o.ToString(Newtonsoft.Json.Formatting.None);
        }

        private static bool IsValidSourceToken(
            IOrganizationService org,
            string incomingToken)
        {
            var envVarSchema = GetAppSetting("Env.WebhookValidationToken.Schema", "");
            if (string.IsNullOrWhiteSpace(envVarSchema))
            {
                // No schema configured => allow all
                return true;
            }

            var expected = ReadEnvValue(org, envVarSchema);
            if (string.IsNullOrWhiteSpace(expected))
            {
                // Env var not set => allow all
                return true;
            }

            if (string.IsNullOrWhiteSpace(incomingToken))
                return false;

            return string.Equals(expected.Trim(), incomingToken.Trim(), StringComparison.Ordinal);
        }

        private static string ReadEnvValue(IOrganizationService org, string schemaName)
        {
            // environmentvariabledefinition / environmentvariablevalue
            var defQ = new QueryExpression("environmentvariabledefinition")
            {
                ColumnSet = new ColumnSet("environmentvariabledefinitionid", "schemaname")
            };
            defQ.Criteria.AddCondition("schemaname", ConditionOperator.Equal, schemaName);
            var defs = org.RetrieveMultiple(defQ);
            if (defs.Entities.Count == 0) return null;

            var defId = defs.Entities[0].Id;

            var valQ = new QueryExpression("environmentvariablevalue")
            {
                ColumnSet = new ColumnSet("value", "environmentvariabledefinitionid")
            };
            valQ.Criteria.AddCondition("environmentvariabledefinitionid", ConditionOperator.Equal, defId);
            var vals = org.RetrieveMultiple(valQ);
            if (vals.Entities.Count == 0) return null;

            return vals.Entities[0].GetAttributeValue<string>("value");
        }

        private static Entity FindDocuSignLog(IOrganizationService org, string envelopeId)
        {
            const string Entity = "ntw_docusignlog";
            const string EnvelopeField = "ntw_envelopid"; // adjust if different
            var q = new QueryExpression(Entity)
            {
                ColumnSet = new ColumnSet("activityid")
            };
            q.Criteria.AddCondition(EnvelopeField, ConditionOperator.Equal, envelopeId);
            var r = org.RetrieveMultiple(q);
            return r.Entities.FirstOrDefault();
        }

        private static void CreateNoteOnDocusignLog(
            IOrganizationService org,
            Guid docusignLogId,
            string subject,
            string body,
            string pdfBase64Attachment = null,
            string pdfName = null,
            string mimeType = null)
        {
            var note = new Entity("annotation");
            note["subject"] = string.IsNullOrWhiteSpace(subject) ? "DocuSign Event" : subject;
            note["notetext"] = string.IsNullOrWhiteSpace(body) ? subject : body;
            note["objectid"] = new EntityReference("ntw_docusignlog", docusignLogId);
            if (!string.IsNullOrWhiteSpace(pdfBase64Attachment) && !string.IsNullOrWhiteSpace(pdfName))
            {
                note["isdocument"] = true;
                note["filename"] = pdfName;
                note["mimetype"] = string.IsNullOrWhiteSpace(mimeType) ? "application/pdf" : mimeType;
                note["documentbody"] = pdfBase64Attachment; // base64
            }
            org.Create(note);
        }

        private static void TryAttachSignedPdfToDocusignLog(
            IOrganizationService org, Guid docusignLogId, string pdfBase64)
        {
            // If ntw_signeddocument (file column) exists, set it; else ignore.
            try
            {
                var bytes = Convert.FromBase64String(pdfBase64);
                var update = new Entity("ntw_docusignlog", docusignLogId);
                update["ntw_signeddocument"] = bytes; // adjust logical name if different
                org.Update(update);
            }
            catch
            {
                // swallow: some on-prem orgs don’t have File columns; note is already added.
            }
        }

        private static void SetDocuSignLogStateAndStatus_Config(IOrganizationService org, Guid id, string flavor)
        {
            // Avoid hard-coded option values; read from app.config (you can tune to your org)
            // Keys:
            //   DocusignLog.Completed.State / .Status
            //   DocusignLog.FinishLater.State / .Status
            //   DocusignLog.Declined.State / .Status
            var stateKey = $"DocusignLog.{flavor}.State";
            var statusKey = $"DocusignLog.{flavor}.Status";

            var st = GetIntSetting(stateKey);
            var sc = GetIntSetting(statusKey);
            if (st == null && sc == null) return;

            var upd = new Entity("ntw_docusignlog", id);
            if (st != null) upd["statecode"] = new OptionSetValue(st.Value);
            if (sc != null) upd["statuscode"] = new OptionSetValue(sc.Value);
            org.Update(upd);
        }

        private static string BuildNoteBody(
            string summaryStatus, string emailSubject, string senderUserName, string senderEmail)
        {
            var sb = new StringBuilder();
            if (!string.IsNullOrWhiteSpace(summaryStatus)) sb.AppendLine("Status: " + summaryStatus);
            if (!string.IsNullOrWhiteSpace(emailSubject)) sb.AppendLine("Subject: " + emailSubject);
            if (!string.IsNullOrWhiteSpace(senderUserName) || !string.IsNullOrWhiteSpace(senderEmail))
                sb.AppendLine("Sender: " + senderUserName + (string.IsNullOrWhiteSpace(senderEmail) ? "" : (" <" + senderEmail + ">")));
            return sb.Length == 0 ? "DocuSign event received." : sb.ToString();
        }
    }
}
