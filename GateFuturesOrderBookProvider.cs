using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TradingBot.Models;

public class GateFuturesOrderBookProvider : BaseOrderBookProvider
{
    private readonly List<GateFuturesWsClient> _wsClients = new();
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _subscriptionCts = new();
    private const int MaxSubscriptionsPerClient = 100;

    // Глобальный стакан для всех символов
    public ConcurrentDictionary<string, SortedDictionary<decimal, long>> Bids = new();
    public ConcurrentDictionary<string, SortedDictionary<decimal, long>> Asks = new();

    public GateFuturesOrderBookProvider(IEnumerable<string> symbols)
    {
        var symbolList = symbols.ToList();
        _expectedSymbolCount = symbolList.Count;

        // Разбиваем символы на группы по 100 (лимит Gate)
        var chunks = symbolList
            .Select((s, i) => (s, i))
            .GroupBy(x => x.i / MaxSubscriptionsPerClient)
            .Select(g => g.Select(x => x.s).ToList())
            .ToList();

        Log.Info($"[Gate Provider] {symbolList.Count} символов → {chunks.Count} WS-клиентов");

        foreach (var chunk in chunks)
        {
            var wsClient = new GateFuturesWsClient(Config.GateApiKey, Config.GateApiSecret, Bids, Asks, _receivedSymbols, CheckAndSetReady);
            // Присваиваем символы клиенту (для GetSubscribedSymbols)
            foreach (var symbol in chunk)
            {
                wsClient.AssignSymbol(symbol);
            }
            _wsClients.Add(wsClient);

            // Пробрасываем события от всех клиентов
            wsClient.OnOrderBookUpdate += (contract, update) =>
            {
                // Применяем апдейт к локальному стакану
                wsClient.ApplyOrderBookUpdate(update);

                // Получаем топ уровни
                var (bids, asks) = wsClient.GetTopLevels(contract, depth: 20);
                if (bids.Count > 0 && asks.Count > 0)
                {
                    var bidLevels = bids.Select(b => new OrderBookLevel { Price = b.Price, Quantity = b.Size }).ToList();
                    var askLevels = asks.Select(a => new OrderBookLevel { Price = a.Price, Quantity = a.Size }).ToList();
                    var orderBook = new OrderBookData(bidLevels, askLevels, DateTime.UtcNow);

                    _orderBooks[contract] = orderBook;
                    _receivedSymbols.Add(contract);
                    RaiseOrderBookUpdated(contract);

                    CheckAndSetReady(contract);
                }
            };

            wsClient.OnRawMessage += raw =>
            {
                // Логируем сырые сообщения для дебага первых 10 сек
                if (Config.LogRawMessages && DateTime.UtcNow < DateTime.UtcNow.AddSeconds(10))
                    Log.Debug($"[Gate WS] {raw[..Math.Min(200, raw.Length)]}");
            };

            // Регистрируем символы для этого клиента
            foreach (var symbol in chunk)
            {
                _desiredSymbols[symbol] = 0;
            }
        }
    }

    protected override Task<(bool Success, string? ErrorMessage)> SubscribeToSymbolAsync(string symbol, CancellationToken cancellationToken)
        => Task.FromResult<(bool, string?)>((true, null));

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Подключаем все клиенты параллельно
            var connectTasks = _wsClients.Select(client => client.ConnectAsync()).ToList();
            await Task.WhenAll(connectTasks);

            // Подписываемся на апдейты (теперь с начальным снапшотом)
            Log.Info("[Gate Provider] Подписываемся на order book с начальным снапшотом...");
            var subscribeTasks = new List<Task>();
            foreach (var client in _wsClients)
            {
                var clientSymbols = client.GetSubscribedSymbols();
                foreach (var symbol in clientSymbols)
                {
                    subscribeTasks.Add(client.SubscribeOrderBook(symbol, limit: 20, interval: "200ms"));
                }
            }
            await Task.WhenAll(subscribeTasks);

            Log.Info($"[Gate Provider] ✓ Подписано {_expectedSymbolCount} символов через {_wsClients.Count} клиентов");
        }
        catch (Exception ex)
        {
            Log.Error($"[Gate Provider] Failed to start: {ex.Message}");
            throw;
        }
    }

    public IEnumerable<string> GetSubscribedSymbols()
    {
        return _wsClients.SelectMany(c => c.GetSubscribedSymbols()).Distinct();
    }

    protected override void CheckAndSetReady(string symbol)
    {
        base.CheckAndSetReady(symbol);
        Log.Info($"[Gate] Symbol {symbol} ready, total: {_receivedSymbols.Count}/{_expectedSymbolCount}");
        if (_receivedSymbols.Count == _expectedSymbolCount)
        {
            Log.Info("[Gate] All symbols received, provider ready!");
        }
    }

    public override void Dispose()
    {
        StopHealthMonitor();
        foreach (var client in _wsClients)
        {
            client?.Dispose();
        }
        _wsClients.Clear();
    }
}

// Вспомогательный класс для WebSocket клиента Gate
public class GateFuturesWsClient : IAsyncDisposable, IDisposable
{
    private const string UrlUsdt = "wss://fx-ws.gateio.ws/v4/ws/usdt";
    private readonly string _apiKey;
    private readonly string _apiSecret;

    private ClientWebSocket? _ws;
    private readonly CancellationTokenSource _cts = new();

    // Глобальный стакан (передается из провайдера)
    private readonly ConcurrentDictionary<string, SortedDictionary<decimal, long>> _bids;
    private readonly ConcurrentDictionary<string, SortedDictionary<decimal, long>> _asks;

    // Локи для синхронизации обновлений по контракту
    private readonly ConcurrentDictionary<string, object> _locks = new();

    // Для готовности
    private readonly HashSet<string> _receivedSymbols;
    private readonly Action<string> _checkAndSetReady;

    // Отслеживание подписанных символов
    private readonly HashSet<string> _subscribedSymbols = new();

    public event Action<string, GateOrderBookUpdate>? OnOrderBookUpdate;
    public event Action<string>? OnRawMessage;

    public GateFuturesWsClient(string apiKey, string apiSecret, ConcurrentDictionary<string, SortedDictionary<decimal, long>> bids, ConcurrentDictionary<string, SortedDictionary<decimal, long>> asks, HashSet<string> receivedSymbols, Action<string> checkAndSetReady)
    {
        _apiKey = apiKey;
        _apiSecret = apiSecret;
        _bids = bids;
        _asks = asks;
        _receivedSymbols = receivedSymbols;
        _checkAndSetReady = checkAndSetReady;
    }

    public async Task ConnectAsync()
    {
        _ws = new ClientWebSocket();
        await _ws.ConnectAsync(new Uri(UrlUsdt), _cts.Token);
        Log.Info("[Gate Futures WS] Connected");

        _ = Task.Run(ReceiveLoopAsync);
        _ = Task.Run(PingLoopAsync);
    }

    public async Task SubscribeOrderBook(string contract, int limit = 20, string interval = "200ms")
    {
        var payload = new object[] { contract, limit, interval };
        await SubscribeAsync("futures.order_book", payload);
        _subscribedSymbols.Add(contract);
    }

    public void AssignSymbol(string symbol)
    {
        _subscribedSymbols.Add(symbol);
    }

    public IEnumerable<string> GetSubscribedSymbols()
    {
        return _subscribedSymbols.ToArray();
    }

    private async Task SubscribeAsync(string channel, object payload)
    {
        Log.Info($"[Gate] Subscribing to {channel}");
        long time = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var req = new
        {
            time,
            channel,
            @event = "subscribe",
            payload
        };

        string json = JsonSerializer.Serialize(req);
        byte[] bytes = Encoding.UTF8.GetBytes(json);
        try
        {
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, _cts.Token);
        }
        catch (Exception ex)
        {
            Log.Error($"[Gate] Send error for {channel}: {ex.Message}");
            throw;
        }
    }

    private async Task PingLoopAsync()
    {
#pragma warning disable CS8602 // Dereference of a possibly null reference
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                if (_ws == null) continue;
                await Task.Delay(10_000, _cts.Token);
                var ping = new { time = DateTimeOffset.UtcNow.ToUnixTimeSeconds(), channel = "futures.ping" };
                string json = JsonSerializer.Serialize(ping);
                byte[] bytes = Encoding.UTF8.GetBytes(json);
                if (_ws != null)
                    await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, _cts.Token);
            }
            catch (Exception ex)
            {
                Log.Error($"[Gate Futures WS] Ping error: {ex.Message}");
                // При ошибке пинга, ReceiveLoop переподключится
            }
        }
#pragma warning restore CS8602
    }

    private async Task ReceiveLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                if (_ws!.State != WebSocketState.Open)
                {
                    Log.Warn("[Gate Futures WS] Connection lost, attempting reconnect...");
                    await Task.Delay(5000, _cts.Token); // Ждём 5 сек перед переподключением
                    await ConnectAsync();
                    // Переподписываемся на все символы
                    foreach (var symbol in _subscribedSymbols)
                    {
                        await SubscribeOrderBook(symbol);
                    }
                    continue;
                }

                var buffer = new byte[65536];
                var sb = new StringBuilder();
                WebSocketReceiveResult result;
                do
                {
                    result = await _ws.ReceiveAsync(buffer, _cts.Token);
                    sb.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
                } while (!result.EndOfMessage);

                string raw = sb.ToString();
                Log.Info($"[Gate] Raw received length: {raw.Length}");
                OnRawMessage?.Invoke(raw);
                HandleMessage(raw);
            }
            catch (Exception ex)
            {
                Log.Error($"[Gate Futures WS] ReceiveLoop error: {ex.Message}");
                // При ошибке переподключимся в следующем цикле
            }
        }
    }

    private void HandleMessage(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var root = doc.RootElement;
            if (!root.TryGetProperty("channel", out var chanEl)) return;

            string channel = chanEl.GetString()!;
            Log.Info($"[Gate] Received message for {channel}");
            if (!root.TryGetProperty("result", out var result)) return;

            if (channel == "futures.order_book")
            {
                // Проверяем тип события
                if (result.TryGetProperty("e", out var eventEl))
                {
                    string eventType = eventEl.GetString()!;
                    if (eventType == "all")
                    {
                        // Это начальный снапшот
                        HandleOrderBookSnapshot(result);
                    }
                    else if (eventType == "update")
                    {
                        // Это инкрементальный апдейт
                        var update = JsonSerializer.Deserialize<GateOrderBookUpdate>(result.GetRawText())!;
                        OnOrderBookUpdate?.Invoke(update.Contract, update);
                    }
                }
                else
                {
                    // Возможно, это update без 'e'
                    try
                    {
                        var update = JsonSerializer.Deserialize<GateOrderBookUpdate>(result.GetRawText())!;
                        OnOrderBookUpdate?.Invoke(update.Contract, update);
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"[Gate] Failed to deserialize update: {ex.Message}, raw: {result.GetRawText().Substring(0, 200)}");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Log.Error($"[Gate ParseError] {ex.Message}, raw: {raw.Substring(0, Math.Min(200, raw.Length))}");
        }
    }

    private void HandleOrderBookSnapshot(JsonElement result)
    {
        try
        {
            if (!result.TryGetProperty("contract", out var contractEl)) return;
            string contract = contractEl.GetString()!;

            var bids = _bids.GetOrAdd(contract, _ => new SortedDictionary<decimal, long>(Comparer<decimal>.Create((a, b) => b.CompareTo(a))));
            var asks = _asks.GetOrAdd(contract, _ => new SortedDictionary<decimal, long>());

            var lockObj = _locks.GetOrAdd(contract, _ => new object());
            lock (lockObj)
            {
                if (result.TryGetProperty("bids", out var bidsEl) && bidsEl.ValueKind == JsonValueKind.Array)
                {
                    bids.Clear();
                    foreach (var bid in bidsEl.EnumerateArray())
                    {
                        if (bid.ValueKind == JsonValueKind.Array && bid.GetArrayLength() >= 2)
                        {
                            var price = decimal.Parse(bid[0].GetString()!, System.Globalization.CultureInfo.InvariantCulture);
                            var size = (long)decimal.Parse(bid[1].GetString()!, System.Globalization.CultureInfo.InvariantCulture);
                            bids[price] = size;
                        }
                    }
                }

                if (result.TryGetProperty("asks", out var asksEl) && asksEl.ValueKind == JsonValueKind.Array)
                {
                    asks.Clear();
                    foreach (var ask in asksEl.EnumerateArray())
                    {
                        if (ask.ValueKind == JsonValueKind.Array && ask.GetArrayLength() >= 2)
                        {
                            var price = decimal.Parse(ask[0].GetString()!, System.Globalization.CultureInfo.InvariantCulture);
                            var size = (long)decimal.Parse(ask[1].GetString()!, System.Globalization.CultureInfo.InvariantCulture);
                            asks[price] = size;
                        }
                    }
                }
            }

            // Помечаем как полученный
            _receivedSymbols.Add(contract);
            _checkAndSetReady(contract);
        }
        catch (Exception ex)
        {
            Log.Warn($"[Gate Provider] Ошибка обработки снапшота: {ex.Message}");
        }
    }

    public void ApplyOrderBookUpdate(GateOrderBookUpdate upd)
    {
        var bids = _bids.GetOrAdd(upd.Contract, _ => new SortedDictionary<decimal, long>(Comparer<decimal>.Create((a, b) => b.CompareTo(a))));
        var asks = _asks.GetOrAdd(upd.Contract, _ => new SortedDictionary<decimal, long>());

        lock (bids)
        {
            foreach (var level in upd.Bids ?? new())
            {
                decimal price = decimal.Parse(level.P);
                if (level.S == 0) bids.Remove(price);
                else bids[price] = level.S;
            }
        }
        lock (asks)
        {
            foreach (var level in upd.Asks ?? new())
            {
                decimal price = decimal.Parse(level.P);
                if (level.S == 0) asks.Remove(price);
                else asks[price] = level.S;
            }
        }
    }

    public (List<(decimal Price, long Size)> Bids, List<(decimal Price, long Size)> Asks) GetTopLevels(string contract, int depth = 10)
    {
        var bids = new List<(decimal, long)>();
        var asks = new List<(decimal, long)>();

        if (_bids.TryGetValue(contract, out var bidBook))
            lock (bidBook)
                foreach (var kv in bidBook) { bids.Add((kv.Key, kv.Value)); if (bids.Count == depth) break; }

        if (_asks.TryGetValue(contract, out var askBook))
            lock (askBook)
                foreach (var kv in askBook) { asks.Add((kv.Key, kv.Value)); if (asks.Count == depth) break; }

        return (bids, asks);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_ws?.State == WebSocketState.Open)
            await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        _ws?.Dispose();
    }

    public void Dispose()
    {
        _cts.Cancel();
        if (_ws?.State == WebSocketState.Open)
            _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None).Wait();
        _ws?.Dispose();
    }
}

public class GateOrderBookUpdate
{
    public long T { get; set; }
    public string S { get; set; } = string.Empty;
    public long U { get; set; }
    public long Uu { get; set; }
    [System.Text.Json.Serialization.JsonPropertyName("b")]
    public List<List<string>> B { get; set; } = new();
    [System.Text.Json.Serialization.JsonPropertyName("a")]
    public List<List<string>> A { get; set; } = new();

    public string Contract => S;

    public List<GateOrderBookLevel> Bids => B.Select(x => new GateOrderBookLevel { P = x[0], S = long.Parse(x[1], System.Globalization.CultureInfo.InvariantCulture) }).ToList();
    public List<GateOrderBookLevel> Asks => A.Select(x => new GateOrderBookLevel { P = x[0], S = long.Parse(x[1], System.Globalization.CultureInfo.InvariantCulture) }).ToList();
}

public class GateOrderBookLevel
{
    public string P { get; set; } = string.Empty;
    public long S { get; set; }
}