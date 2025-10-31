// Program.cs — diagnostics build
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Tooling.Connector;
using System.Security.Cryptography;
using System.Text;

var builder = WebApplication.CreateBuilder(args);

const bool REQUIRE_BASIC = true;
const bool REQUIRE_HMAC = false; // keep off while testing with Postman
const bool ACK_FAST = false; // do sync while debugging
const bool TRACE = true;

builder.WebHost.UseKestrel(o => o.Limits.MaxRequestBodySize = 50 * 1024 * 1024);
var app = builder.Build();

/* -------------------- CONFIG (EDIT THESE TWO LINES!) -------------------- */
// If you built a Custom Action (recommended on on-prem), this must be the Action's Unique Name.
const string CRM_OPERATION_NAME = "ntw_DocuSign_Ingress";   // <-- put the EXACT action unique name
// This must be the EXACT input parameter logical name in the Action.
const string CRM_INPUT_PARAM_NAME = "RequestBody";            // <-- e.g. "ntw_RequestBody" if that’s what you created
/* ----------------------------------------------------------------------- */

// Auth + CRM connection
const string BASIC_USER = "mhmoussa@netwaysdev.local";
const string BASIC_PASS = "MhM@123456";
const string DS_SECRET_B64 = "UVgtL9C2kRNesCFOFi3DQngDJT4+GCR8ETohooZhwfU=";

const string CRM_URL = "http://10.141.0.170/CrmUat"; // must include org
const string CRM_DOMAIN = "netwaysdev";
const string CRM_USERNAME = "mhmoussa@Netwaysdev.local";  // UPN or DOMAIN\user
const string CRM_PASSWORD = "MhM@123456";

app.MapGet("/", () => Results.Ok("Listener up"));
app.MapGet("/healthz", () => Results.Ok("ok"));

app.MapGet("/selftest", () =>
{
    try
    {
        using var client = new CrmServiceClient(BuildAdConnString(CRM_URL, CRM_DOMAIN, CRM_USERNAME, CRM_PASSWORD));
        if (!client.IsReady)
            return Results.Json(new { ok = false, where = "connect", error = client.LastCrmError });

        var resp = (WhoAmIResponse)client.Execute(new WhoAmIRequest());
        return Results.Json(new { ok = true, userId = resp.UserId.ToString() });
    }
    catch (Exception ex)
    {
        return Results.Json(new { ok = false, where = "exception", error = ex.Message });
    }
});

app.MapPost("/docusign/webhook", async (HttpContext ctx) =>
{
    var cid = Guid.NewGuid().ToString("N");

    // Basic auth
    if (REQUIRE_BASIC && !IsBasicAuthValid(ctx.Request.Headers.Authorization, BASIC_USER, BASIC_PASS, TRACE))
        return Results.Unauthorized();

    // Read body
    ctx.Request.EnableBuffering();
    byte[] raw;
    using (var ms = new MemoryStream())
    {
        await ctx.Request.Body.CopyToAsync(ms);
        raw = ms.ToArray();
    }
    ctx.Request.Body.Position = 0;

    if (REQUIRE_HMAC)
    {
        var sigHeader = ctx.Request.Headers["X-DocuSign-Signature-1"].ToString();
        if (string.IsNullOrWhiteSpace(sigHeader) || string.IsNullOrWhiteSpace(DS_SECRET_B64) || !VerifyHmac(DS_SECRET_B64, raw, sigHeader))
            return Results.Unauthorized();
    }

    var action = async () =>
    {
        try
        {
            var bodyText = Encoding.UTF8.GetString(raw);

            using var client = new CrmServiceClient(BuildAdConnString(CRM_URL, CRM_DOMAIN, CRM_USERNAME, CRM_PASSWORD));
            if (!client.IsReady)
                throw new Exception("CrmServiceClient not ready: " + client.LastCrmError);

            // IMPORTANT: operation name and parameter name must match your Action definition
            var req = new OrganizationRequest(CRM_OPERATION_NAME)
            {
                Parameters = { [CRM_INPUT_PARAM_NAME] = bodyText }
            };

            var result = client.Execute(req); // usually OrganizationResponse with no params for Actions
            if (TRACE) Console.WriteLine($"[{cid}] Executed '{CRM_OPERATION_NAME}' with param '{CRM_INPUT_PARAM_NAME}' len={bodyText.Length}");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{cid}] CRM call failed: {ex.Message}");
            throw;
        }
    };

    if (ACK_FAST)
    {
        _ = Task.Run(action);
        return Results.Json(new { ok = true, id = cid, queued = true });
    }
    else
    {
        await action();
        return Results.Json(new { ok = true, id = cid, queued = false });
    }
});

app.Run();

/* ---------------- helpers ---------------- */
static string BuildAdConnString(string url, string domain, string user, string pass)
{
    var userFmt = user.Contains('\\') || user.Contains('@') ? user : $"{domain}\\{user}";
    return $"""
AuthType=AD;
Url={url};
Domain={domain};
Username={userFmt};
Password={pass};
""";
}

static bool IsBasicAuthValid(string? authHeader, string expectedUser, string expectedPass, bool trace = false)
{
    if (string.IsNullOrEmpty(expectedUser) && string.IsNullOrEmpty(expectedPass)) return true;
    if (string.IsNullOrWhiteSpace(authHeader) || !authHeader.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase)) return false;

    try
    {
        var b64 = authHeader.Substring("Basic ".Length).Trim();
        var raw = Encoding.UTF8.GetString(Convert.FromBase64String(b64));
        var i = raw.IndexOf(':'); if (i < 0) return false;
        var u = raw[..i]; var p = raw[(i + 1)..];

        var ok = (u.Equals(expectedUser, StringComparison.OrdinalIgnoreCase) || NormalizeUser(u).Equals(NormalizeUser(expectedUser), StringComparison.OrdinalIgnoreCase))
                 && p == expectedPass;

        if (trace) Console.WriteLine($"[AUTH] user={u}, match={ok}");
        return ok;

        static string NormalizeUser(string s) => s.Contains('@')
            ? $"{s.Split('@')[1].Split('.')[0]}\\{s.Split('@')[0]}"
            : s;
    }
    catch { return false; }
}

static bool VerifyHmac(string secretB64, byte[] payload, string sigHeaderB64)
{
    try
    {
        using var hmac = new HMACSHA256(Convert.FromBase64String(secretB64));
        var calcB64 = Convert.ToBase64String(hmac.ComputeHash(payload));
        return CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(calcB64), Encoding.ASCII.GetBytes(sigHeaderB64));
    }
    catch { return false; }
}
