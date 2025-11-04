using System;
using Microsoft.Xrm.Sdk;

namespace CrmInvoker.Helpers
{
    /// <summary>
    /// Thin wrapper specialized for ntw_docusignlog.
    /// </summary>
    internal static class DocuSignLogSignedFileHelper
    {
        private const string ENTITY = "ntw_docusignlog";
        private const string FILE_ATTR = "ntw_signeddocument";

        /// <summary>
        /// Uploads the signed PDF to ntw_docusignlog.ntw_signeddocument if supported,
        /// or falls back to a note. Returns true if file column path was used.
        /// </summary>
        public static bool UploadSignedPdf(IOrganizationService org, Guid docusignLogId, byte[] pdfBytes, string fileName)
        {
            return SignedFileUploadHelper.TryUploadSmart(
      org,
      entityLogicalName: ENTITY,
      id: docusignLogId,
      fileAttributeLogicalName: FILE_ATTR,
      fileName: string.IsNullOrWhiteSpace(fileName) ? "SignedDocument.pdf" : fileName,
      mimeType: "application/pdf",
      fileBytes: pdfBytes
  );

        }
    }
}
