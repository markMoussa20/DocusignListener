// Program.cs — DocuSign Listener (NET 8) → shells to CrmInvoker (.NET Framework 4.8)
// All CRM logic is in CrmInvoker.exe. This app just receives and forwards.

using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseKestrel(o => o.Limits.MaxRequestBodySize = 50 * 1024 * 1024);
var app = builder.Build();

// -------- settings for local testing ----------
const bool REQUIRE_BASIC = true;
const bool REQUIRE_HMAC = false; // keep false for Postman
const bool ACK_FAST = false; // do sync while debugging
const bool TRACE = true;

const string BASIC_USER = "mhmoussa@netwaysdev.local";
const string BASIC_PASS = "MhM@123456";
const string DS_SECRET_B64 = ""; // fill when you enable HMAC
// ---------------------------------------------

app.MapGet("/healthz", () => Results.Ok("ok"));

app.MapPost("/docusign/webhook", async (HttpContext ctx) =>
{
    var id = Guid.NewGuid().ToString("N");

    try
    {
        // 1) Basic auth
        if (REQUIRE_BASIC && !IsBasicAuthValid(ctx.Request.Headers.Authorization, BASIC_USER, BASIC_PASS, TRACE))
            return Results.Json(new { ok = false, source = "listener", where = "auth", id, error = "401 basic-auth failed" }, statusCode: 401);

        // 2) Read body
        ctx.Request.EnableBuffering();
        byte[] raw;
        using (var ms = new MemoryStream()) { await ctx.Request.Body.CopyToAsync(ms); raw = ms.ToArray(); }
        ctx.Request.Body.Position = 0;

        // 3) Optional HMAC (DocuSign X-DocuSign-Signature-1)
        if (REQUIRE_HMAC)
        {
            var sig = ctx.Request.Headers["X-DocuSign-Signature-1"].ToString();
            if (string.IsNullOrWhiteSpace(sig) || !VerifyHmac(DS_SECRET_B64, raw, sig))
                return Results.Json(new { ok = false, source = "listener", where = "hmac", id, error = "bad/missing HMAC" }, statusCode: 401);
        }

        // 4) Parse small crumbs (for logging only)
        JsonNode? root;
        try { root = JsonNode.Parse(raw); }
        catch (Exception jex)
        {
            return Results.Json(new { ok = false, source = "listener", where = "json", id, error = jex.Message }, statusCode: 400);
        }

        // 5) Optional validation token forwarded to invoker (header name flexible)
        var validationToken =
               ctx.Request.Headers["validationToken"].ToString()
            ?? ctx.Request.Headers["X-Validation-Token"].ToString();

        var payload = new
        {
            body = Encoding.UTF8.GetString(raw), // original DocuSign JSON (no shape assumptions)

            // tiny crumbs (helpful in stdout)
            eventName = root?["event"]?.ToString(),
            envelopeId = root?["data"]?["envelopeId"]?.ToString(),

            // forwarded token (if present)
            validationToken = string.IsNullOrWhiteSpace(validationToken) ? null : validationToken
        };

        var invokerPath = ResolveInvokerPath(builder.Configuration);
        if (invokerPath == null)
        {
            return Results.Json(new
            {
                ok = false,
                source = "listener",
                where = "invoke",
                id,
                error = "CrmInvoker.exe not found. Expected under 'CrmInvoker\\CrmInvoker.exe' or 'CrmInvoker\\net48\\CrmInvoker.exe' beside the listener. " +
                        "Or set appsettings.json: { \"InvokerPath\": \"C:\\\\path\\\\CrmInvoker.exe\" }"
            }, statusCode: 500);
        }

        if (ACK_FAST)
        {
            _ = Task.Run(() => RunInvoker(invokerPath, payload, id, TRACE));
            return Results.Json(new { ok = true, source = "listener", queued = true, id });
        }
        else
        {
            var (ok, err, stdout) = await RunInvoker(invokerPath, payload, id, TRACE);
            return Results.Json(new { ok, source = "listener", queued = false, id, error = err, invoker = stdout });
        }
    }
    catch (Exception ex)
    {
        return Results.Json(new { ok = false, source = "listener", where = "exception", id, error = ex.Message }, statusCode: 500);
    }
});

app.Run();

static bool IsBasicAuthValid(string? authHeader, string user, string pass, bool trace = false)
{
    if (string.IsNullOrEmpty(user) && string.IsNullOrEmpty(pass)) return true;
    if (string.IsNullOrWhiteSpace(authHeader) || !authHeader.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase)) return false;
    try
    {
        var raw = Encoding.UTF8.GetString(Convert.FromBase64String(authHeader["Basic ".Length..].Trim()));
        var i = raw.IndexOf(':'); if (i < 0) return false;
        var u = raw[..i]; var p = raw[(i + 1)..];
        var ok = p == pass && (u.Equals(user, StringComparison.OrdinalIgnoreCase) || Normalize(u).Equals(Normalize(user), StringComparison.OrdinalIgnoreCase));
        if (trace) Console.WriteLine($"[AUTH] user={u}, ok={ok}");
        return ok;

        static string Normalize(string s) => s.Contains('@') ? $"{s.Split('@')[1].Split('.')[0]}\\{s.Split('@')[0]}" : s;
    }
    catch { return false; }
}

static bool VerifyHmac(string secretB64, byte[] payload, string sigB64)
{
    try
    {
        using var h = new HMACSHA256(Convert.FromBase64String(secretB64));
        var calc = Convert.ToBase64String(h.ComputeHash(payload));
        return CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(calc), Encoding.ASCII.GetBytes(sigB64));
    }
    catch { return false; }
}

static string? ResolveInvokerPath(IConfiguration cfg)
{
    // 1) explicit setting from appsettings.json or env
    var fromConfig = cfg["InvokerPath"];
    if (!string.IsNullOrWhiteSpace(fromConfig) && File.Exists(fromConfig))
        return Path.GetFullPath(fromConfig);

    // 2) relative probes near the listener output folder
    var baseDir = AppContext.BaseDirectory.TrimEnd('\\', '/');
    var probes = new[]
    {
        Path.Combine(baseDir, "CrmInvoker", "CrmInvoker.exe"),
        Path.Combine(baseDir, "CrmInvoker", "net48", "CrmInvoker.exe"),
        Path.Combine(baseDir, "CrmInvoker", "net8.0", "CrmInvoker.exe"),
    };
    foreach (var p in probes)
        if (File.Exists(p)) return p;

    return null;
}

static async Task<(bool ok, string? err, string? stdout)> RunInvoker(string exePath, object payload, string correlationId, bool trace)
{
    var json = JsonSerializer.Serialize(payload);
    var psi = new ProcessStartInfo
    {
        FileName = exePath,
        Arguments = "--stdin",
        RedirectStandardInput = true,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true,
        WorkingDirectory = Path.GetDirectoryName(exePath)!
    };

    using var p = Process.Start(psi)!;
    await p.StandardInput.WriteAsync(json);
    p.StandardInput.Close();

    var stdout = await p.StandardOutput.ReadToEndAsync();
    var stderr = await p.StandardError.ReadToEndAsync();
    await p.WaitForExitAsync();

    if (trace) Console.WriteLine($"[Invoker:{correlationId}] exit={p.ExitCode}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}");
    return (p.ExitCode == 0, p.ExitCode == 0 ? null : (stderr ?? "invoker failed"), stdout);
}
