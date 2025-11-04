// CrmInvoker/Program.cs  — .NET Framework 4.8
// NuGet: Microsoft.CrmSdk.CoreAssemblies, Microsoft.CrmSdk.XrmTooling.CoreAssembly,
//        System.Configuration.ConfigurationManager, Newtonsoft.Json
//
// Reads { body: "<RAW_DOCUSIGN_JSON>" } from STDIN (written by the listener)
// and performs all CRM work *directly* (on-prem safe).
//
// Behavior:
// - Create ntw_webhooklog (Active/Received) for every call
// - Validate "validationToken" from payload against env var (optional; if not configured → allow)
// - Find ntw_docusignlog by Envelope ID
// - Add small audit note (no file) for traceability
// - On completed:
//      • Upload the signed PDF to ntw_docusignlog.ntw_signeddocument via File-Blocks API
//      • Set statecode = 1 (Inactive), statuscode = 202370005 (Signed)
// - On finish-later / declined: just mark webhook log processed with the reason

using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Collections.Generic;
using System.ServiceModel;                 // FaultException<OrganizationServiceFault>
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using Newtonsoft.Json.Linq;

namespace CrmInvoker
{
    internal static class Program
    {
        // Hard values you requested
        private const int STATE_INACTIVE = 1;        // statecode
        private const int STATUS_SIGNED = 202370005; // statuscode (Signed)

        private static int Main()
        {
            // TLS for older boxes
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
                rawBody = (string)outer["body"] ?? "";
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
                       (string)root?["event"]
                    ?? (string)root?["eventType"]
                    ?? (string)root?["type"]
                    ?? "";

                envelopeId =
                       (string)root?["data"]?["envelopeId"]
                    ?? (string)root?["envelopeId"]
                    ?? "";

                status =
                       (string)root?["data"]?["envelopeSummary"]?["status"]
                    ?? (string)root?["summary"]?["status"]
                    ?? (string)root?["status"]
                    ?? "";

                emailSubject =
                       (string)root?["data"]?["envelopeSummary"]?["emailSubject"]
                    ?? (string)root?["summary"]?["emailSubject"]
                    ?? (string)root?["emailSubject"]
                    ?? "";

                senderUser =
                       (string)root?["data"]?["envelopeSummary"]?["sender"]?["userName"]
                    ?? (string)root?["summary"]?["senderUserName"]
                    ?? (string)root?["sender"]?["userName"]
                    ?? "";

                senderMail =
                       (string)root?["data"]?["envelopeSummary"]?["sender"]?["email"]
                    ?? (string)root?["summary"]?["senderEmail"]
                    ?? (string)root?["sender"]?["email"]
                    ?? "";

                // Optional validation token
                validationTokenIncoming = (string)root?["validationToken"] ?? "";

                // First document (typical Connect payload)
                var doc = root?["data"]?["envelopeSummary"]?["envelopeDocuments"]?.First;
                if (doc != null)
                {
                    pdfBase64 = (string)doc?["PDFBytes"] ?? (string)doc?["pdfBytes"] ?? "";
                    var n = (string)doc?["name"] ?? "SignedDocument";
                    docName = EnsurePdfName(n);
                }
            }
            catch (Exception ex)
            {
                // proceed; CRM step will fail gracefully if required inputs are missing
                Console.Error.WriteLine("[Invoker] JSON parse warning: " + ex.Message);
            }

            // Quick pre-flight: event + envelopeId are mandatory
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
                    var correlationId = Guid.NewGuid().ToString("N");

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

                    // 7) Add a small audit note (no file) for traceability
                    var noteBody = BuildNoteBody(status, emailSubject, senderUser, senderMail);
                    CreateNoteOnDocusignLog(org, docusignLogId,
                        subject: $"DocuSign Event: {eventName}",
                        body: noteBody);

                    // 8) Branch by event
                    if (IsCompleted(eventName))
                    {
                        if (!string.IsNullOrWhiteSpace(pdfBase64))
                        {
                            var fileName = string.IsNullOrWhiteSpace(docName) ? "SignedDocument.pdf" : docName;
                            var base64Payload = StripDataPrefix(pdfBase64);

                            // === DIRECT FILE-COLUMN UPLOAD (Blocks API) ===
                            UploadToFileColumnViaBlocks(
                                org,
                                entityLogicalName: "ntw_docusignlog",
                                id: docusignLogId,
                                fileAttributeLogicalName: "ntw_signeddocument",
                                fileName: fileName,
                                mimeType: "application/pdf",
                                base64: base64Payload);
                        }

                        // Set to Inactive/Signed (hard values you requested)
                        var upd = new Entity("ntw_docusignlog", docusignLogId)
                        {
                            ["statecode"] = new OptionSetValue(STATE_INACTIVE),
                            ["statuscode"] = new OptionSetValue(STATUS_SIGNED)
                        };
                        org.Update(upd);

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
                        MarkWebhookLogFinal(org, webhookLogId, success: true, failedReason: "Finish later");
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
                        MarkWebhookLogFinal(org, webhookLogId, success: true, failedReason: "Declined");
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

        private static string BuildNoteBody(
         string summaryStatus,
         string emailSubject,
         string senderUserName,
         string senderEmail)
        {
            var sb = new StringBuilder();
            if (!string.IsNullOrWhiteSpace(summaryStatus)) sb.AppendLine("Status: " + summaryStatus);
            if (!string.IsNullOrWhiteSpace(emailSubject)) sb.AppendLine("Subject: " + emailSubject);
            if (!string.IsNullOrWhiteSpace(senderUserName) || !string.IsNullOrWhiteSpace(senderEmail))
                sb.AppendLine("Sender: " + senderUserName + (string.IsNullOrWhiteSpace(senderEmail) ? "" : (" <" + senderEmail + ">")));
            return sb.Length == 0 ? "DocuSign event received." : sb.ToString();
        }

        // ===== FILE BLOCKS UPLOAD (late-bound requests; on-prem compatible) =====
        private static void UploadToFileColumnViaBlocks(
            IOrganizationService org,
            string entityLogicalName,
            Guid id,
            string fileAttributeLogicalName,
            string fileName,
            string mimeType,
            string base64)
        {
            if (org == null) throw new InvalidPluginExecutionException("org is null.");
            if (string.IsNullOrWhiteSpace(entityLogicalName)) throw new InvalidPluginExecutionException("entityLogicalName is empty.");
            if (id == Guid.Empty) throw new InvalidPluginExecutionException("id is empty.");
            if (string.IsNullOrWhiteSpace(fileAttributeLogicalName)) throw new InvalidPluginExecutionException("fileAttributeLogicalName is empty.");
            if (string.IsNullOrWhiteSpace(fileName)) fileName = "file.bin";
            if (string.IsNullOrWhiteSpace(mimeType)) mimeType = "application/octet-stream";
            if (string.IsNullOrWhiteSpace(base64)) throw new InvalidPluginExecutionException("Base64 content is empty.");

            byte[] bytes;
            try { bytes = Convert.FromBase64String(base64); }
            catch (Exception ex) { throw new InvalidPluginExecutionException("Invalid Base64 content: " + ex.Message); }

            // Block size (default 4MB); can override via AppSettings["Uploader.BlockSize"]
            int blockSize = GetIntSetting("Uploader.BlockSize") ?? 4 * 1024 * 1024;

            // 1) Initialize
            var initReq = new OrganizationRequest("InitializeFileBlocksUpload")
            {
                ["Target"] = new EntityReference(entityLogicalName, id),
                ["FileAttributeName"] = fileAttributeLogicalName,
                ["FileName"] = fileName
            };
            var initResp = org.Execute(initReq);
            var token = (string)initResp.Results["FileContinuationToken"];

            // 2) Upload blocks
            var blockIds = new List<string>();
            int offset = 0;
            while (offset < bytes.Length)
            {
                int len = Math.Min(blockSize, bytes.Length - offset);
                var chunk = new byte[len];
                Buffer.BlockCopy(bytes, offset, chunk, 0, len);
                offset += len;

                var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));
                var upReq = new OrganizationRequest("UploadBlock")
                {
                    ["FileContinuationToken"] = token,
                    ["BlockId"] = blockId,
                    ["BlockData"] = chunk
                };
                org.Execute(upReq);
                blockIds.Add(blockId);
            }

            // 3) Commit
            var commitReq = new OrganizationRequest("CommitFileBlocksUpload")
            {
                ["FileContinuationToken"] = token,
                ["BlockList"] = blockIds.ToArray(),
                ["FileName"] = fileName,
                ["MimeType"] = mimeType
            };
            org.Execute(commitReq);
        }

        // ===== Utilities =====
        private static string StripDataPrefix(string b64)
        {
            var s = (b64 ?? "").Trim();
            var i = s.IndexOf("base64,", StringComparison.OrdinalIgnoreCase);
            return i >= 0 ? s.Substring(i + "base64,".Length) : s;
        }

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

        private static string GetAppSetting(string key, string fallback = "")
            => ConfigurationManager.AppSettings[key] ?? fallback;

        private static int? GetIntSetting(string key)
            => int.TryParse(ConfigurationManager.AppSettings[key], out var v) ? v : (int?)null;

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

            // status values (Active/Received) from config (fallback defaults)
            var activeState = GetIntSetting("WebhookLog.ActiveState") ?? 0;
            var receivedStatus = GetIntSetting("WebhookLog.ReceivedStatus") ?? 1;

            var e = new Entity(Entity);
            e[Name] = $"DocuSign Webhook {DateTime.UtcNow:u}";
            e[Source] = "DocuSign";
            e[Payload] = BuildCompactAuditJson(eventName, envelopeId, summaryStatus, senderUserName, senderEmail);
            e[Corr] = correlationId;
            e[State] = new OptionSetValue(activeState);
            e[Status] = new OptionSetValue(receivedStatus);

            return org.Create(e);
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
            upd[State] = new OptionSetValue(inactiveState);
            upd[Status] = new OptionSetValue(success ? processedStatus : failedStatus);
            if (!success && !string.IsNullOrWhiteSpace(failedReason))
                upd[Payload] = "[FAIL:" + failedReason + "]";
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
            const string EnvelopeField = "ntw_envelopid"; // your actual logical name
            var q = new QueryExpression(Entity)
            {
                ColumnSet = new ColumnSet("activityid")
            };
            q.Criteria.AddCondition(EnvelopeField, ConditionOperator.Equal, envelopeId);
            var r = org.RetrieveMultiple(q);
            return r.Entities.FirstOrDefault();
        }

        // ——— CreateNote helpers (no attachment variant only) ———
        private static void CreateNoteOnDocusignLog(
            IOrganizationService org,
            Guid docusignLogId,
            string subject,
            string body)
        {
            var note = new Entity("annotation");
            note["subject"] = string.IsNullOrWhiteSpace(subject) ? "DocuSign Event" : subject;
            note["notetext"] = string.IsNullOrWhiteSpace(body) ? subject : body;
            note["objectid"] = new EntityReference("ntw_docusignlog", docusignLogId);
            org.Create(note);
        }
    }
}
