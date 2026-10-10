// File: tests/MsmqCache.Tests/TcpServerFixture.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: xUnit fixtures: real server host on an ephemeral loopback port
// License: MIT

using MsmqCache;
using Xunit;

public abstract class TcpServerFixtureBase : IAsyncLifetime
{
    TestHost _host = null!;

    protected abstract string Password { get; }

    public int Port => _host.Port;
    public ConfigManager Config => _host.Config;
    public TestHost Host => _host;

    public async Task InitializeAsync()
    {
        var dir = Path.Combine(Path.GetTempPath(), "msmqcache-fx-" + Guid.NewGuid().ToString("N"));
        _host = await TestHost.StartAsync(new
        {
            EnableTcp = true,
            EnableMsmq = false,
            BindAddress = "127.0.0.1",
            Port = 0,
            Password = Password,
            DataDirectory = Path.Combine(dir, "data"),
            PersistenceEnabled = true,
            SnapshotIntervalSeconds = 3600,
            MaxValueLength = 1000
        }, dir);
    }

    public async Task DisposeAsync()
    {
        await _host.DisposeAsync();
        try { Directory.Delete(_host.Dir, true); } catch { }
    }
}

public sealed class OpenServerFixture : TcpServerFixtureBase
{
    protected override string Password => "";
}

public sealed class SecuredServerFixture : TcpServerFixtureBase
{
    protected override string Password => "s3cret-pw";
}
