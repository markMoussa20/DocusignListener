using System;
using System.Text;
using System.ServiceModel; // FaultException<T>
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace CrmInvoker.Helpers
{
    internal static class SignedFileUploadHelper
    {
        // -------- Public entry points --------

        // base64 overload
        public static bool TryUploadSmart(
            IOrganizationService org,
            string entityLogicalName,
            Guid id,
            string fileAttributeLogicalName,
            string fileName,
            string mimeType,
            string fileBase64)
        {
            if (string.IsNullOrWhiteSpace(fileBase64)) return false;
            var bytes = Convert.FromBase64String(StripDataPrefix(fileBase64));
            return TryUploadSmart(org, entityLogicalName, id, fileAttributeLogicalName, fileName, mimeType, bytes);
        }

        // bytes overload
        public static bool TryUploadSmart(
            IOrganizationService org,
            string entityLogicalName,
            Guid id,
            string fileAttributeLogicalName,
            string fileName,
            string mimeType,
            byte[] fileBytes)
        {
            if (org == null) throw new ArgumentNullException(nameof(org));
            if (id == Guid.Empty) return false;
            if (fileBytes == null || fileBytes.Length == 0) return false;

            fileName = string.IsNullOrWhiteSpace(fileName) ? "SignedDocument.pdf" : fileName;
            mimeType = string.IsNullOrWhiteSpace(mimeType) ? "application/pdf" : mimeType;

            // 1) Modern path: InitializeFileBlocksUpload → UploadBlock → Commit
            if (TryUploadWithFileBlocks(org, entityLogicalName, id, fileAttributeLogicalName, fileName, mimeType, fileBytes))
            {
                if (VerifyReadable(org, entityLogicalName, id, fileAttributeLogicalName))
                    return true; // good
            }

            // 2) On-prem/legacy: direct attribute write (byte[]) + set <attr>_name
            if (TryUploadDirectAttribute(org, entityLogicalName, id, fileAttributeLogicalName, fileName, fileBytes))
            {
                if (VerifyReadable(org, entityLogicalName, id, fileAttributeLogicalName))
                    return true; // good
            }

            // 3) Fallback: Note
            CreateNote(org, entityLogicalName, id, "Signed PDF", fileName, mimeType, fileBytes);

            // IMPORTANT: clear shadow name to avoid UI ghost link if it was set
            TryClearShadowName(org, entityLogicalName, id, fileAttributeLogicalName);

            return true; // stored as Note
        }

        // -------- Implementations --------

        private static bool TryUploadWithFileBlocks(
            IOrganizationService org,
            string entity,
            Guid id,
            string attr,
            string fileName,
            string mimeType,
            byte[] bytes)
        {
            try
            {
                var init = new InitializeFileBlocksUploadRequest
                {
                    Target = new EntityReference(entity, id),
                    FileAttributeName = attr,
                    FileName = fileName
                };
                var initResp = (InitializeFileBlocksUploadResponse)org.Execute(init);
                var token = initResp.FileContinuationToken;
                if (string.IsNullOrWhiteSpace(token)) return false;

                const int CHUNK = 4 * 1024 * 1024;
                int idx = 0, offset = 0;
                while (offset < bytes.Length)
                {
                    var len = Math.Min(CHUNK, bytes.Length - offset);
                    var chunk = new byte[len];
                    Buffer.BlockCopy(bytes, offset, chunk, 0, len);

                    var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes($"block-{idx:D8}"));
                    org.Execute(new UploadBlockRequest
                    {
                        BlockData = chunk,
                        BlockId = blockId,
                        FileContinuationToken = token
                    });

                    offset += len;
                    idx++;
                }

                org.Execute(new CommitFileBlocksUploadRequest
                {
                    FileContinuationToken = token,
                    FileName = fileName,
                    MimeType = mimeType
                });

                return true;
            }
            catch (FaultException<OrganizationServiceFault>)
            {
                return false; // API not supported on this server
            }
            catch
            {
                return false; // any other failure
            }
        }

        /// <summary>
        /// Some on-prem systems store file bytes directly on the column (like old image fields).
        /// We attempt an Update with byte[] and also set the shadow name "<attr>_name" when present.
        /// </summary>
        private static bool TryUploadDirectAttribute(
            IOrganizationService org,
            string entity,
            Guid id,
            string attr,
            string fileName,
            byte[] bytes)
        {
            try
            {
                var e = new Entity(entity, id);
                e[attr] = bytes;

                // if a shadow name column exists, set it so the UI shows the filename
                var shadow = attr + "_name";
                try
                {
                    e[shadow] = fileName;
                }
                catch { /* ignore if shadow not present */ }

                org.Update(e);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Make sure the thing we wrote can actually be read.
        /// We try file-blocks download first, then direct attribute read.
        /// </summary>
        private static bool VerifyReadable(IOrganizationService org, string entity, Guid id, string attr)
        {
            // A) file-blocks download probe
            try
            {
                var init = new OrganizationRequest("InitializeFileBlocksDownload")
                {
                    ["Target"] = new EntityReference(entity, id),
                    ["FileAttributeName"] = attr
                };
                var initResp = org.Execute(init);
                var token = (string)initResp.Results["FileContinuationToken"];
                if (!string.IsNullOrWhiteSpace(token))
                {
                    // try download a tiny block
                    var get = new OrganizationRequest("DownloadBlock")
                    {
                        ["FileContinuationToken"] = token,
                        ["Offset"] = 0L,
                        ["BlockLength"] = 1024
                    };
                    var getResp = org.Execute(get);
                    var data = getResp.Results["Data"] as byte[];
                    if (data != null && data.Length > 0) return true;
                }
            }
            catch { /* ignore */ }

            // B) direct attribute probe (byte[])
            try
            {
                var ent = org.Retrieve(entity, id, new ColumnSet(attr));
                if (ent.Contains(attr) && ent[attr] is byte[] b && b.Length > 0) return true;
            }
            catch { /* ignore */ }

            return false;
        }

        private static void TryClearShadowName(IOrganizationService org, string entity, Guid id, string attr)
        {
            try
            {
                var shadow = attr + "_name";
                var e = new Entity(entity, id);
                e[shadow] = null;
                org.Update(e);
            }
            catch { /* ignore */ }
        }

        private static void CreateNote(
            IOrganizationService org,
            string entity,
            Guid id,
            string subject,
            string fileName,
            string mimeType,
            byte[] bytes)
        {
            var note = new Entity("annotation")
            {
                ["subject"] = subject ?? "Signed PDF",
                ["filename"] = string.IsNullOrWhiteSpace(fileName) ? "SignedDocument.pdf" : fileName,
                ["mimetype"] = string.IsNullOrWhiteSpace(mimeType) ? "application/pdf" : mimeType,
                ["isdocument"] = true,
                ["documentbody"] = Convert.ToBase64String(bytes),
                ["objectid"] = new EntityReference(entity, id)
            };
            org.Create(note);
        }

        private static string StripDataPrefix(string b64)
        {
            var s = (b64 ?? string.Empty).Trim();
            var i = s.IndexOf("base64,", StringComparison.OrdinalIgnoreCase);
            return (i >= 0) ? s.Substring(i + "base64,".Length) : s;
        }
    }
}
