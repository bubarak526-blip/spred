using BingX.Net.Clients;
using BingX.Net.Objects.Models;
using CryptoExchange.Net.Objects.Sockets;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using TradingBot.Models;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

public static class SymbolHelpers
{
    public static string NormalizeTicker(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
            return string.Empty;

        var normalized = symbol.Trim().ToUpperInvariant();
        normalized = normalized.Replace("-", string.Empty, StringComparison.Ordinal);
        normalized = normalized.Replace("_", string.Empty, StringComparison.Ordinal);
        return normalized;
    }

    public static string ToBingXSymbolFormat(string symbol)
    {
        var normalized = NormalizeTicker(symbol);
        if (!normalized.EndsWith("USDT", StringComparison.OrdinalIgnoreCase))
            return string.Empty;

        return normalized[..^4] + "-USDT";
    }
}

public static class ExchangeSymbolDiscovery
{
    public static async Task<List<CommonSymbol>> GetCommonPerpetualUSDTFuturesAsync(
        CancellationToken cancellationToken)
    {
        try
        {
            // Получаем список символов BingX через REST API
            var bingXRestClient = ExchangeRestClients.BingXRestClient;
            var bingXContractsResult = await bingXRestClient.PerpetualFuturesApi.ExchangeData.GetContractsAsync(string.Empty, cancellationToken);
            if (!bingXContractsResult.Success || bingXContractsResult.Data is null)
                throw new InvalidOperationException($"BingX contract discovery failed: {bingXContractsResult.Error?.Message}");

            // BingX символы (например: "BTC-USDT", "ETH-USDT")
            var bingXSymbols = bingXContractsResult.Data
                .Select(contract => contract.Symbol?.Trim().ToUpperInvariant() ?? "")
                .Where(symbol => !string.IsNullOrWhiteSpace(symbol) && symbol.EndsWith("USDT", StringComparison.Ordinal))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            Log.Debug($"[SYMBOLS] BingX найдено {bingXSymbols.Count} USDT фьючерсов");

            // Получаем список символов Gate через REST API
            var gateResponse = await ExchangeRestClients.GateRestClient.GetAsync("https://api.gateio.ws/api/v4/futures/usdt/contracts", cancellationToken);
            if (!gateResponse.IsSuccessStatusCode)
                throw new InvalidOperationException($"Gate contract discovery failed: {gateResponse.StatusCode}");

            var gateJson = await gateResponse.Content.ReadAsStringAsync(cancellationToken);
            var gateContracts = JsonSerializer.Deserialize<List<GateContract>>(gateJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            
            // Gate символы (например: "BTC_USDT", "ETH_USDT")
            var gateSymbols = gateContracts?
                .Select(contract => contract.Name?.Trim().ToUpperInvariant() ?? "")
                .Where(name => !string.IsNullOrWhiteSpace(name) && name.Contains("USDT", StringComparison.Ordinal))
                .Distinct(StringComparer.Ordinal)
                .ToList() ?? new List<string>();

            Log.Debug($"[SYMBOLS] Gate найдено {gateSymbols.Count} USDT фьючерсов");

            // Находим пересечение символов BingX и Gate
            var commonSymbols = new List<CommonSymbol>();
            
            foreach (var bingXSymbol in bingXSymbols)
            {
                // Преобразуем BingX формат (BTC-USDT или BTCUSDT) в базовый (BTC)
                string baseName = bingXSymbol
                    .Replace("-USDT", "", StringComparison.Ordinal)
                    .Replace("_USDT", "", StringComparison.Ordinal)
                    .Replace("USDT", "", StringComparison.Ordinal);

                // Ищем соответствие в Gate (BTC_USDT)
                var matchingGateSymbol = gateSymbols.FirstOrDefault(gs => 
                    gs.StartsWith(baseName, StringComparison.Ordinal) && gs.EndsWith("USDT", StringComparison.Ordinal));

                if (!string.IsNullOrEmpty(matchingGateSymbol))
                {
                    commonSymbols.Add(new CommonSymbol 
                    { 
                        Name = baseName,
                        BingXSymbol = bingXSymbol, 
                        GateSymbol = matchingGateSymbol 
                    });
                    Log.Debug($"[SYMBOLS] Совпадение: {baseName} → BingX:{bingXSymbol} / Gate:{matchingGateSymbol}");
                }
            }

            commonSymbols = commonSymbols
                .OrderBy(symbol => symbol.Name, StringComparer.Ordinal)
                .ToList();

            Log.Info($"[SYMBOLS] Найдено {commonSymbols.Count} общих ликвидных USDT-фьючерсных пар (BingX-Gate)");
            if (commonSymbols.Count == 0)
            {
                Log.Warn($"[SYMBOLS] BingX: {string.Join(", ", bingXSymbols.Take(5))}... ({bingXSymbols.Count} total)");
                Log.Warn($"[SYMBOLS] Gate: {string.Join(", ", gateSymbols.Take(5))}... ({gateSymbols.Count} total)");
            }
            return commonSymbols;
        }
        catch (Exception ex)
        {
            Log.Error($"[SYMBOLS] Symbol discovery error: {ex.Message}", ex);
            return new List<CommonSymbol>();
        }
    }

    private static string ToBingXSymbolFormat(string binanceSymbol)
    {
        if (string.IsNullOrWhiteSpace(binanceSymbol))
            return string.Empty;

        return binanceSymbol.Trim().ToUpperInvariant().Replace("USDT", "-USDT", StringComparison.OrdinalIgnoreCase);
    }

    private static string ToGateSymbolFormat(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
            return string.Empty;

        return symbol.Trim().ToUpperInvariant().Replace("USDT", "_USDT", StringComparison.OrdinalIgnoreCase);
    }

    private class GateContract
    {
        [System.Text.Json.Serialization.JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;
        
        [System.Text.Json.Serialization.JsonPropertyName("type")]
        public string Type { get; set; } = string.Empty;
    }
}

public static class ExchangeRestClients
{
    public static readonly BingXRestClient BingXRestClient = new();
    public static readonly System.Net.Http.HttpClient GateRestClient = new();

    static ExchangeRestClients()
    {
        // Оптимизация: включаем TCP keep-alive для HTTP соединений.
        System.Net.ServicePointManager.DefaultConnectionLimit = Config.HttpConnectionLimit;
        System.Net.ServicePointManager.ReusePort = true;
    }
}

public abstract class BaseOrderBookProvider : IDisposable
{
    protected readonly ConcurrentDictionary<string, OrderBookData> _orderBooks = new(StringComparer.OrdinalIgnoreCase);
    protected readonly TaskCompletionSource<bool> _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    protected readonly HashSet<string> _receivedSymbols = new(StringComparer.OrdinalIgnoreCase);
    protected readonly ConcurrentDictionary<string, byte> _desiredSymbols = new(StringComparer.OrdinalIgnoreCase);
    protected int _expectedSymbolCount = 0;

    private CancellationTokenSource? _monitorCts;
    private Task? _monitorTask;
    private static readonly TimeSpan HealthCheckInterval = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan StaleDataThreshold = TimeSpan.FromMilliseconds(Config.MaxOrderBookAgeMs * 3);

    public event Action<string>? OnOrderBookUpdated;

    private enum SubscriptionResult
    {
        Success,
        Failed,
        Unsupported
    }

    public async Task WaitForReadyAsync(CancellationToken cancellationToken)
    {
        var finished = await Task.WhenAny(_ready.Task, Task.Delay(TimeSpan.FromSeconds(Config.ProviderReadyTimeoutSec), cancellationToken));
        if (finished == _ready.Task)
        {
            await _ready.Task;
        }
        else
        {
            Log.Info($"{GetType().Name} ready wait timed out after {Config.ProviderReadyTimeoutSec} sec; received {_receivedSymbols.Count}/{_expectedSymbolCount} symbols.");
        }
    }

    public OrderBookData? TryGetOrderBook(string symbol)
    {
        return _orderBooks.TryGetValue(symbol, out var ob) ? ob : null;
    }

    protected void RaiseOrderBookUpdated(string symbol)
    {
        OnOrderBookUpdated?.Invoke(symbol);
    }

    protected virtual void CheckAndSetReady(string symbol)
    {
        lock (_receivedSymbols)
        {
            _receivedSymbols.Add(symbol);
            if (_expectedSymbolCount > 0 && _receivedSymbols.Count >= _expectedSymbolCount)
            {
                _ready.TrySetResult(true);
            }
        }
    }

    protected virtual int MaxParallelSubscriptions => 20;
    protected virtual int BatchSize => 20;

    private const int MaxSubscriptionAttempts = Config.MaxSubscriptionAttempts;
    private static readonly TimeSpan SubscriptionRetryDelay = TimeSpan.FromSeconds(Config.SubscriptionRetryDelaySec);

    public virtual async Task StartAsync(IEnumerable<string> symbols, CancellationToken cancellationToken)
    {
        var symbolArray = symbols?.Where(symbol => !string.IsNullOrWhiteSpace(symbol)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        if (symbolArray is null || symbolArray.Length == 0)
            throw new ArgumentException("No symbols supplied for websocket subscription.", nameof(symbols));

        _expectedSymbolCount = symbolArray.Length;
        _desiredSymbols.Clear();
        foreach (var symbol in symbolArray)
            _desiredSymbols[symbol] = 0;

        Log.Info($"{GetType().Name} subscription start: {_expectedSymbolCount} symbols...");
        await EnsureSubscribedAsync(symbolArray, cancellationToken);
        Log.Info($"{GetType().Name} subscription finished: {_receivedSymbols.Count}/{_expectedSymbolCount} symbols received.");

        StartHealthMonitor(cancellationToken);
    }

    private async Task EnsureSubscribedAsync(string[] symbolArray, CancellationToken cancellationToken)
    {
        var pendingSymbols = new HashSet<string>(symbolArray, StringComparer.OrdinalIgnoreCase);
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);

        var batchSize = BatchSize;
        var batches = pendingSymbols.Chunk(batchSize).ToList();

        foreach (var batch in batches)
        {
            if (cancellationToken.IsCancellationRequested || DateTime.UtcNow >= deadline)
                break;

            Log.Info($"{GetType().Name} subscribing to batch of {batch.Length} symbols...");
            var batchResult = await SubscribeToSymbolsAsync(batch, cancellationToken);
            if (batchResult.Success)
            {
                foreach (var symbol in batch)
                    pendingSymbols.Remove(symbol);
            }
            else
            {
                var errorDetails = string.IsNullOrEmpty(batchResult.ErrorMessage) 
                    ? "(no error details)" 
                    : batchResult.ErrorMessage;
                Log.Error($"{GetType().Name} batch subscription failed: {errorDetails}. Falling back to per-symbol registration.");

                var semaphore = new SemaphoreSlim(MaxParallelSubscriptions);
                try
                {
                    var tasks = batch.Select(async symbol =>
                    {
                        await semaphore.WaitAsync(cancellationToken);
                        try
                        {
                            var result = await TrySubscribeSymbolAsync(symbol, cancellationToken);
                            if (result == SubscriptionResult.Success)
                                return symbol;
                            if (result == SubscriptionResult.Unsupported)
                                return symbol;
                        }
                        finally
                        {
                            semaphore.Release();
                        }

                        return null;
                    }).ToArray();

                    var results = await Task.WhenAll(tasks);
                    foreach (var symbol in results.Where(s => s is not null))
                    {
                        pendingSymbols.Remove(symbol!);
                    }
                }
                finally
                {
                    semaphore.Dispose();
                }
            }

            // Небольшая пауза между батчами, чтобы не перегружать
            if (batches.IndexOf(batch) < batches.Count - 1)
                await Task.Delay(1000, cancellationToken);
        }

        if (pendingSymbols.Count > 0)
        {
            Log.Error($"{GetType().Name} could not subscribe to {pendingSymbols.Count} symbols after timeout. Продолжаем с доступными данными.");
        }
    }

    protected virtual async Task<(bool Success, string? ErrorMessage)> SubscribeToSymbolsAsync(IEnumerable<string> symbols, CancellationToken cancellationToken)
    {
        foreach (var symbol in symbols)
        {
            var (subscribed, errorMessage) = await SubscribeToSymbolAsync(symbol, cancellationToken);
            if (!subscribed)
                return (false, errorMessage);
        }

        return (true, null);
    }

    private async Task<SubscriptionResult> TrySubscribeSymbolAsync(string symbol, CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= MaxSubscriptionAttempts && !cancellationToken.IsCancellationRequested; attempt++)
        {
            try
            {
                var (subscribed, errorMessage) = await SubscribeToSymbolAsync(symbol, cancellationToken);
                if (subscribed)
                    return SubscriptionResult.Success;

                if (TrySuppressUnsupportedError(errorMessage))
                {
                    Log.Info($"{GetType().Name} skipping unsupported symbol {symbol}: {errorMessage}");
                    DecrementExpectedSymbolCount();
                    _desiredSymbols.TryRemove(symbol, out _);
                    return SubscriptionResult.Unsupported;
                }

                Log.Error($"{GetType().Name} orderbook subscription failed for {symbol} (attempt {attempt}): {errorMessage}");
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Log.Error($"{GetType().Name} orderbook subscription exception for {symbol} (attempt {attempt})", ex);
            }

            if (attempt < MaxSubscriptionAttempts)
                await Task.Delay(SubscriptionRetryDelay, cancellationToken);
        }

        return SubscriptionResult.Failed;
    }

    private void DecrementExpectedSymbolCount()
    {
        lock (_receivedSymbols)
        {
            if (_expectedSymbolCount <= 0)
                return;

            _expectedSymbolCount--;
            if (_expectedSymbolCount <= _receivedSymbols.Count)
                _ready.TrySetResult(true);
        }
    }

    private bool TrySuppressUnsupportedError(string? errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return false;

        if (errorMessage.Contains("dataType not support", StringComparison.OrdinalIgnoreCase) ||
            errorMessage.Contains("unsupported symbol", StringComparison.OrdinalIgnoreCase) ||
            errorMessage.Contains("not support", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return false;
    }

    private void StartHealthMonitor(CancellationToken cancellationToken)
    {
        StopHealthMonitor();
        _monitorCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _monitorTask = Task.Run(() => MonitorSubscriptionsAsync(_monitorCts.Token), _monitorCts.Token);
    }

    protected void StopHealthMonitor()
    {
        if (_monitorCts is null)
            return;

        _monitorCts.Cancel();
        try
        {
            _monitorTask?.Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // Ignore cancellation timing issues.
        }
        _monitorCts.Dispose();
        _monitorCts = null;
        _monitorTask = null;
    }

    private async Task MonitorSubscriptionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(HealthCheckInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            foreach (var symbol in _desiredSymbols.Keys)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                if (_orderBooks.TryGetValue(symbol, out var book) && (DateTime.UtcNow - book.Timestamp) < StaleDataThreshold)
                    continue;

                var result = await TrySubscribeSymbolAsync(symbol, cancellationToken);
                if (result == SubscriptionResult.Unsupported)
                    continue;
            }
        }
    }

    protected abstract Task<(bool Success, string? ErrorMessage)> SubscribeToSymbolAsync(string symbol, CancellationToken cancellationToken);

    public void SetExpectedSymbolCount(int count)
    {
        _expectedSymbolCount = count;
    }

    public abstract void Dispose();
}

