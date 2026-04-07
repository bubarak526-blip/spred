using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using TradingBot.Models;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using CryptoExchange.Net;
using CryptoExchange.Net.Objects;
using CryptoExchange.Net.Objects.Sockets;
using BingX.Net.Objects.Models;
using Newtonsoft.Json.Linq;
using CryptoExchange.Net.Objects.Errors;
using System.Globalization;

// Первый определяем ApiClient (используется в заглушках)
public class ApiClient
{
    private readonly HttpClient _httpClient;

    public ApiClient()
    {
        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Add("X-BX-APIKEY", Config.BingXApiKey);
    }

    public async Task<List<BingXContract>> GetContractsAsync(string category, CancellationToken cancellationToken)
    {
        var contracts = new List<BingXContract>();
        try
        {
            string url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts";

            var response = await _httpClient.GetAsync(url, cancellationToken);
            var content = await response.Content.ReadAsStringAsync();
            
            var json = JObject.Parse(content);
            var data = json["data"] as JArray;

            if (data != null)
            {
                foreach (var contract in data)
                {
                    var symbol = contract["symbol"]?.ToString();
                    if (!string.IsNullOrEmpty(symbol) && symbol.EndsWith("-USDT"))
                    {
                        contracts.Add(new BingXContract { Symbol = symbol });
                    }
                }
            }
            else
            {
                Log.Error($"BingX Contracts Error: {content}");
            }
        }
        catch (Exception ex)
        {
            Log.Error("Ошибка при получении контрактов BingX", ex);
        }
        return contracts;
    }

    private string CreateSignature(string parameters, string secret)
    {
        byte[] keyByte = Encoding.UTF8.GetBytes(secret);
        byte[] messageBytes = Encoding.UTF8.GetBytes(parameters);
        using var hmacsha256 = new HMACSHA256(keyByte);
        byte[] hashmessage = hmacsha256.ComputeHash(messageBytes);
        return BitConverter.ToString(hashmessage).Replace("-", "").ToLower();
    }
}

public class BingXContract
{
    public string Symbol { get; set; } = string.Empty;
}

public class BingXTickerUpdate
{
    public string Symbol { get; set; } = string.Empty;
    public decimal BestAsk { get; set; }
    public decimal BestBid { get; set; }
    public decimal BestAskQty { get; set; }
    public decimal BestBidQty { get; set; }
}

/// <summary>
    /// Реальный WebSocket клиент для BingX Perpetual Futures
    /// Подключается к wss://open-api-swap.bingx.com/swap-market и потребляет depth данные
    /// </summary>


/// <summary>
    /// Реальный WebSocket клиент для BingX Perpetual Futures
    /// Подключается к wss://open-api-swap.bingx.com/swap-market и потребляет depth данные
    /// </summary>
public class BingXWebSocketClient : IDisposable
{
    private const string WebSocketUrl = "wss://open-api-swap.bingx.com/swap-market";
    private ClientWebSocket? _webSocket;
    private CancellationTokenSource? _cts;
    private Task? _receiveTask;
    private long _requestIdCounter = 0;
    
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<(bool Success, string? Error)>> _pendingAcks = new();
    private readonly Dictionary<string, Action<BingXOrderBook>> _depthHandlers = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _handlerLock = new();
    private Action<List<BingXTickerUpdate>>? _allTickerHandler;

    private string CreateSignature(string parameters, string secret)
    {
        byte[] keyByte = Encoding.UTF8.GetBytes(secret);
        byte[] messageBytes = Encoding.UTF8.GetBytes(parameters);
        using var hmacsha256 = new HMACSHA256(keyByte);
        byte[] hashmessage = hmacsha256.ComputeHash(messageBytes);
        return BitConverter.ToString(hashmessage).Replace("-", "").ToLower();
    }

    public async Task<bool> ConnectAsync(CancellationToken ct = default)
    {
        await _connectLock.WaitAsync(ct);
        try
        {
            if (_webSocket?.State == WebSocketState.Open && _receiveTask?.IsCompleted == false)
            {
                return true;
            }

            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _webSocket = new ClientWebSocket();
            _webSocket.Options.SetRequestHeader("User-Agent", "Mozilla/5.0");

            await _webSocket.ConnectAsync(new Uri(WebSocketUrl), _cts.Token);
            _receiveTask = ReceiveLoopAsync(_cts.Token);
            Log.Info("[BingX WS] ✓ Соединение к wss://open-api-swap.bingx.com/swap-market установлено");
            return true;
        }
        catch (Exception ex)
        {
            Log.Error($"[BingX WS] Connect Error: {ex.Message}");
            return false;
        }
        finally { _connectLock.Release(); }
    }

    // Метод для подписки на тикер одного символа (используем depth с глубиной 1)
    public async Task<bool> SubscribeTickerAsync(string symbol, Action<BingXTickerUpdate> handler, CancellationToken ct = default)
    {
        var result = await SubscribeAsync(symbol, orderBook => 
        {
            if (orderBook.Bids.Count > 0 && orderBook.Asks.Count > 0)
            {
                var update = new BingXTickerUpdate
                {
                    Symbol = orderBook.Symbol,
                    BestBid = orderBook.Bids[0].Price,
                    BestAsk = orderBook.Asks[0].Price,
                    BestBidQty = orderBook.Bids[0].Quantity,
                    BestAskQty = orderBook.Asks[0].Quantity
                };
                handler(update);
            }
        }, ct);
        return result.Success;
    }

    // Метод для подписки на все тикеры (если работает)
    public async Task<bool> SubscribeAllTickersAsync(Action<List<BingXTickerUpdate>> handler, CancellationToken ct = default)
    {
        if (_webSocket?.State != WebSocketState.Open) await ConnectAsync(ct);

        if (_webSocket?.State != WebSocketState.Open) return false;

        _allTickerHandler = handler;

        var request = new
        {
            id = Guid.NewGuid().ToString(),
            reqType = "sub",
            dataType = "allTicker" // Именно так указано в доках V2
        };
        
        var json = Newtonsoft.Json.JsonConvert.SerializeObject(request);
        await _webSocket.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(json)), 
            WebSocketMessageType.Text, true, ct);
        
        Log.Info("BingX: Подписка на поток всех тикеров (allTicker) отправлена.");
        return true;
    }



    // ✅ Возвращает (успех, ошибка) вместо bool
    public async Task<(bool Success, string? Error)> SubscribeAsync(string symbol, Action<BingXOrderBook> handler, CancellationToken ct = default)
    {
        if (_webSocket?.State != WebSocketState.Open)
        {
            Log.Error($"[BingX SubscribeAsync] Socket not ready! State: {_webSocket?.State}");
            return (false, "Socket not ready");
        }

        // Преобразуем в формат BingX: BTC-USDT
        string bingxSym = symbol.Contains("-") ? symbol : SymbolHelpers.ToBingXSymbolFormat(symbol);
        if (string.IsNullOrEmpty(bingxSym))
        {
            return (false, "Invalid symbol format");
        }

        lock (_handlerLock) _depthHandlers[bingxSym] = handler;

        string id = Interlocked.Increment(ref _requestIdCounter).ToString();
        var tcs = new TaskCompletionSource<(bool, string?)>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingAcks[id] = tcs;

        // Формат: {symbol}@depth{level}@{interval}
        // BTC-USDT и ETH-USDT поддерживают 200ms, остальные 500ms
        string interval = (bingxSym.Equals("BTC-USDT", StringComparison.OrdinalIgnoreCase) || 
                          bingxSym.Equals("ETH-USDT", StringComparison.OrdinalIgnoreCase)) ? "200ms" : "500ms";
        var dataType = string.Format("{0}@depth20@{1}", bingxSym.ToUpperInvariant(), interval);

        var payload = new { 
            id,
            reqType = "sub",
            dataType
        };
        var payloadJson = Newtonsoft.Json.JsonConvert.SerializeObject(payload);
        var bytes = Encoding.UTF8.GetBytes(payloadJson);
        if (_webSocket != null)
            await _webSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, ct);

        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));  // 🔥 Уменьшили с 12 до 3 сек
            var result = await tcs.Task.WaitAsync(timeout.Token);
            return result.Item1 ? (true, null) : (false, $"Server rejected: {result.Item2}");
        }
        catch (OperationCanceledException)
        {
            return (false, "ACK Timeout (invalid symbol or 100-sub limit reached)");
        }
        finally { _pendingAcks.TryRemove(id, out _); }
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        var buffer = new byte[1024 * 128]; // Увеличиваем буфер
        try
        {
            while (_webSocket?.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                var result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    Log.Debug("[BingX WS] Receive loop ended: WebSocket close received.");
                    break;
                }

                // --- РАСПАКОВКА GZIP (Обязательно по документации) ---
                string message;
                using (var msi = new MemoryStream(buffer, 0, result.Count))
                using (var mso = new MemoryStream())
                {
                    using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                    {
                        await gs.CopyToAsync(mso);
                    }
                    message = Encoding.UTF8.GetString(mso.ToArray());
                }

                // Обработка Ping-Pong
                if (message == "Ping")
                {
                    await _webSocket.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes("Pong")), 
                        WebSocketMessageType.Text, true, ct);
                    continue;
                }

                Process(message);
            }
        }
        catch (OperationCanceledException)
        {
            Log.Debug("[BingX WS] Receive loop canceled.");
        }
        catch (Exception ex)
        {
            Log.Error($"[BingX WS] Ошибка: {ex.Message}");
        }
    }

    private void Process(string json)
    {
        // 🔥 ОТЛАДКА: пишем первые 200 символов каждого сообщения
        Log.Debug(string.Format("[BingX WS RAW] {0}", json.Substring(0, Math.Min(200, json.Length))));
        
        if (json == "Ping")
        {
            _webSocket?.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes("Pong")), 
                                 WebSocketMessageType.Text, true, _cts?.Token ?? CancellationToken.None);
            return;
        }

        try
        {
            var j = Newtonsoft.Json.Linq.JObject.Parse(json);

            // 1. Heartbeat
            if (j["ping"] != null)
            {
                var pong = new JObject { ["pong"] = j["ping"], ["time"] = j["time"] };
                _webSocket?.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(pong.ToString(Newtonsoft.Json.Formatting.None))), 
                                     WebSocketMessageType.Text, true, _cts?.Token ?? CancellationToken.None);
                return;
            }

            // 2. ACK подписки
            if (j["id"] != null && j["code"] != null)
            {
                string id = j["id"]!.ToString();
                int code = j["code"]!.Value<int>();
                string msg = j["msg"]?.ToString() ?? "No details provided";
                
                if (_pendingAcks.TryGetValue(id, out var tcs))
                    tcs.TrySetResult((code == 0, code == 0 ? null : msg));
                return;
            }

            // 3. Данные стакана
            string? dt = j["dataType"]?.ToString();
            if (!string.IsNullOrEmpty(dt) && dt.Contains("@depth", StringComparison.OrdinalIgnoreCase))
            {
                string sym = dt.Split('@')[0].ToUpperInvariant(); // 🔥 Нормализуем регистр
                var bids = ParseSide(j["data"]?["bids"]);
                var asks = ParseSide(j["data"]?["asks"]);

                // 🔥 ДИАГНОСТИКА: логируем первые получение данных
                if (sym == "BTC-USDT" && _depthHandlers.Count == 1)  // Первый раз для BTC
                {
                    // Log.Info($"[BingX WS RX] dataType='{dt}' -> sym='{sym}', bids={bids.Count}, asks={asks.Count}");
                    // Log.Info($"[BingX WS RX] Handlers registered: {_depthHandlers.Count}, keys: {string.Join(", ", _depthHandlers.Keys.Take(5))}");
                    // Log.Info($"[BingX WS RX] Looking for handler: '{sym}' in handlers...");
                }

                if (bids.Count > 0 && asks.Count > 0)
                {
                    lock (_handlerLock)
                    {
                        if (_depthHandlers.TryGetValue(sym, out var cb))
                        {
                            cb(new BingXOrderBook { Symbol = sym, Bids = bids, Asks = asks });
                            if (sym == "BTC-USDT" && _depthHandlers.Count < 10)
                                Log.Info($"[BingX WS RX] ✓ Handler found for {sym}, calling it");
                        }
                        else
                        {
                            // Для отладки: если хендлер не найден, значит символ не совпал при подписке
                            if (sym == "BTC-USDT" || sym.StartsWith("BTC"))
                                Log.Error($"[BingX WS] ✗ Handler NOT found for '{sym}'! Registered handlers: {string.Join(",", _depthHandlers.Keys.Take(10))}");
                        }
                    }
                }
            }
            // 4. Данные всех тикеров
            else if (dt == "allTicker")
            {
                var data = j["data"];
                if (data != null && _allTickerHandler != null)
                {
                    var updates = new List<BingXTickerUpdate>();
                    foreach (var item in data)
                    {
                        var update = new BingXTickerUpdate
                        {
                            Symbol = item["symbol"]?.ToString() ?? "",
                            BestBid = item["bidPrice"]?.Value<decimal>() ?? 0,
                            BestBidQty = item["bidQty"]?.Value<decimal>() ?? 0,
                            BestAsk = item["askPrice"]?.Value<decimal>() ?? 0,
                            BestAskQty = item["askQty"]?.Value<decimal>() ?? 0
                        };
                        updates.Add(update);
                    }
                    _allTickerHandler(updates);
                }
            }
        }
        catch (Exception ex) 
        { 
            Log.Error($"BingX WS Process error: {ex.Message}"); 
        }
    }

    private static bool IsUnsupportedBingXError(string? msg)
    {
        if (string.IsNullOrEmpty(msg))
            return false;

        return msg.Contains("dataType not support", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("unsupported symbol", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("not support", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("invalid symbol format", StringComparison.OrdinalIgnoreCase);
    }

    private static List<(decimal Price, decimal Quantity)> ParseSide(Newtonsoft.Json.Linq.JToken? token)
    {
        var res = new List<(decimal, decimal)>();
        if (token is Newtonsoft.Json.Linq.JArray arr)
            foreach (var item in arr)
                if (item is Newtonsoft.Json.Linq.JArray p && p.Count >= 2)
                    try { res.Add((p[0]!.Value<decimal>(), p[1]!.Value<decimal>())); } catch { }
        return res;
    }

    private static string Decompress(byte[] buffer, int count)
    {
        using var input = new MemoryStream(buffer, 0, count);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _webSocket?.Dispose();
        _cts?.Dispose();
        _connectLock.Dispose();
    }
}

namespace BingX.Net.Clients
{
public class BingXSocketClient : IDisposable
{
    private BingXWebSocketClient? _realWebSocketClient;
    private BingXSocketApiClient? _perpetualFuturesApi;
    
    public void Dispose() => _realWebSocketClient?.Dispose();

    public BingXSocketApiClient PerpetualFuturesApi
    {
        get
        {
            _perpetualFuturesApi ??= new BingXSocketApiClient(GetOrCreateWebSocketClient());
            return _perpetualFuturesApi;
        }
    }

    private BingXWebSocketClient GetOrCreateWebSocketClient()
    {
        _realWebSocketClient ??= new BingXWebSocketClient();
        return _realWebSocketClient;
    }

    public async Task<CallResult<UpdateSubscription>> SubscribeToPartialOrderBookUpdatesAsync(
        string symbol, int depth, int updateIntervalMs,
        Action<string, DataEvent<BingXOrderBook>> handler, CancellationToken cancellationToken)
    {
        var ws = GetOrCreateWebSocketClient();
        var connected = await ws.ConnectAsync(cancellationToken);
        if (!connected) return new CallResult<UpdateSubscription>(new ServerError(new ErrorInfo(ErrorType.Unknown, "WS connection failed")));

        void DepthCallback(BingXOrderBook ob)
        {
            var evt = new DataEvent<BingXOrderBook>(
                "",
                new BingXOrderBook { Symbol = ob.Symbol, Bids = ob.Bids, Asks = ob.Asks },
                DateTime.UtcNow, null);
            handler(symbol, evt);
        }

        var (ok, err) = await ws.SubscribeAsync(symbol, DepthCallback, cancellationToken);
        return ok 
            ? new CallResult<UpdateSubscription>(new UpdateSubscription(null!, null!))
            : new CallResult<UpdateSubscription>(new ServerError(new ErrorInfo(ErrorType.Unknown, err ?? "Subscription failed")));
    }
}

public class BingXExchangeDataSocketClient
{
    private readonly BingXWebSocketClient _ws;
    public BingXExchangeDataSocketClient(BingXWebSocketClient ws) => _ws = ws;

    public async Task<CallResult<UpdateSubscription>> SubscribeToPartialOrderBookUpdatesAsync(
        string symbol, int depth, int? updateIntervalMs, 
        Action<DataEvent<BingXOrderBook>> handler, CancellationToken ct)
    {
        var connected = await _ws.ConnectAsync(ct);
        if (!connected) return new CallResult<UpdateSubscription>(new ServerError(new ErrorInfo(ErrorType.Unknown, "WS connection failed")));

        void OnDepth(BingXOrderBook ob)
        {
            var evt = new DataEvent<BingXOrderBook>(
                "", new BingXOrderBook { Symbol = ob.Symbol, Bids = ob.Bids, Asks = ob.Asks },
                DateTime.UtcNow, null);
            handler(evt);
        }

        var (ok, err) = await _ws.SubscribeAsync(symbol, OnDepth, ct);
        return ok 
            ? new CallResult<UpdateSubscription>(new UpdateSubscription(null!, null!))
            : new CallResult<UpdateSubscription>(new ServerError(new ErrorInfo(ErrorType.Unknown, err ?? "Unknown subscription error")));
    }
}

public class BingXSocketApiClient
{
    private readonly BingXWebSocketClient _webSocketClient;
    private BingXExchangeDataSocketClient? _exchangeData;

    public BingXSocketApiClient(BingXWebSocketClient webSocketClient)
    {
        _webSocketClient = webSocketClient;
    }

    public BingXExchangeDataSocketClient ExchangeData
    {
        get
        {
            _exchangeData ??= new BingXExchangeDataSocketClient(_webSocketClient);
            return _exchangeData;
        }
    }
}

    public class BingXRestClient : IDisposable
    {
        public void Dispose() { }

        public BingXRestApiClient PerpetualFuturesApi => new();
    }

    public class BingXRestApiClient
    {
        public BingXExchangeDataRestClient ExchangeData => new();
    }

    public class BingXExchangeDataRestClient
    {
        private readonly ApiClient _apiClient = new();

        public async Task<WebCallResult<List<BingX.Net.Objects.Models.BingXContract>>> GetContractsAsync(string category, CancellationToken cancellationToken)
        {
            var contracts = await _apiClient.GetContractsAsync(category, cancellationToken);
            var resultContracts = contracts.ConvertAll(c => new BingX.Net.Objects.Models.BingXContract { Symbol = c.Symbol });
            return new WebCallResult<List<BingX.Net.Objects.Models.BingXContract>>(
                HttpStatusCode.OK, null, null, TimeSpan.Zero, 0, null, 0, null, null, HttpMethod.Get, null, ResultDataSource.Cache, resultContracts, null);
        }
    }
}

namespace BingX.Net.Objects.Models
{
    public class BingXOrderBook
    {
        public string Symbol { get; set; } = string.Empty;
        public List<(decimal Price, decimal Quantity)> Bids { get; set; } = new();
        public List<(decimal Price, decimal Quantity)> Asks { get; set; } = new();
    }

    public class BingXContract
    {
        public string Symbol { get; set; } = string.Empty;
    }

    public class BingXFundingRate
    {
        public decimal LastFundingRate { get; set; }
    }
}

namespace TradingBot.External
{
    public class BingXApiClient : IDisposable
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey;
        private readonly string _apiSecret;
        private const string BaseUrl = "https://open-api.bingx.com";

        public BingXApiClient(string apiKey, string apiSecret)
        {
            _apiKey = apiKey;
            _apiSecret = apiSecret;
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Add("X-BX-APIKEY", _apiKey);
            _httpClient.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0");
        }

        /// <summary>
        /// Получение доступного баланса USDT
        /// </summary>
        public async Task<decimal> GetBalanceAsync()
        {
            try
            {
                var result = await SendRequestAsync(HttpMethod.Get, "/openApi/swap/v2/user/balance");
                
                if (result["code"]?.Value<int>() != 0)
                {
                    Log.Warn($"[BingX] API вернул ошибку: {result}");
                    return 0m;
                }

                // 🔹 НОВЫЙ ФОРМАТ: data.balance.availableMargin
                var balanceObj = result["data"]?["balance"];
                if (balanceObj != null)
                {
                    var available = balanceObj["availableMargin"]?.Value<decimal>() 
                                 ?? balanceObj["balance"]?.Value<decimal>() 
                                 ?? balanceObj["equity"]?.Value<decimal>() ?? 0m;
                    return available;
                }

                // 🔹 СТАРЫЙ ФОРМАТ (фоллбэк): data[] массив
                if (result["data"] is JArray dataArray)
                {
                    var usdt = dataArray.FirstOrDefault(a => a["asset"]?.ToString() == "USDT");
                    return usdt?["availableBalance"]?.Value<decimal>() 
                        ?? usdt?["balance"]?.Value<decimal>() ?? 0m;
                }

                Log.Warn($"[BingX] Неизвестный формат ответа баланса: {result}");
                return 0m;
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Ошибка запроса баланса: {ex.Message}");
                return 0m;
            }
        }

        /// <summary>
        /// Получение списка всех активных бессрочных контрактов
        /// </summary>
        public async Task<List<string>> GetActiveContractsAsync()
        {
            var symbols = new List<string>();
            try
            {
                // Для рыночных данных подпись обычно не нужна, но используем общий метод
                var result = await SendRequestAsync(HttpMethod.Get, "/openApi/swap/v2/quote/contracts", signed: false);
                var data = result["data"] as JArray;

                if (data != null)
                {
                    foreach (var item in data)
                    {
                        // Статус 1 обычно означает активный контракт
                        if (item["status"]?.Value<int>() == 1)
                        {
                            var symbol = item["symbol"]?.ToString();
                            if (!string.IsNullOrEmpty(symbol))
                                symbols.Add(symbol);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Ошибка получения контрактов: {ex.Message}");
            }
            return symbols;
        }

        public async Task<OrderBookData?> GetOrderBookAsync(string symbol, int limit = 5)
        {
            try
            {
                var parameters = new Dictionary<string, string>
                {
                    { "symbol", symbol },
                    { "limit", limit.ToString(System.Globalization.CultureInfo.InvariantCulture) }
                };

                var result = await SendRequestAsync(HttpMethod.Get, "/openApi/swap/v2/quote/depth", parameters, signed: false);
                var data = result["data"];
                if (data == null)
                    return null;

                var bids = ParseOrderBookSide(data["bids"]);
                var asks = ParseOrderBookSide(data["asks"]);
                return new OrderBookData(bids, asks, DateTime.UtcNow);
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Ошибка загрузки стакана {symbol}: {ex.Message}");
                return null;
            }
        }

        private static List<OrderBookLevel> ParseOrderBookSide(JToken? token)
        {
            var result = new List<OrderBookLevel>();
            if (token is JArray array)
            {
                foreach (var item in array)
                {
                    if (item is JArray side && side.Count >= 2)
                    {
                        if (decimal.TryParse(side[0]?.ToString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var price) &&
                            decimal.TryParse(side[1]?.ToString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var qty))
                        {
                            result.Add(new OrderBookLevel { Price = price, Quantity = qty });
                        }
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Выставление рыночного ордера (Market Order)
        /// Возвращает РЕАЛЬНЫЙ OrderId от BingX
        /// </summary>
        public async Task<BingXOrderResult> PlaceMarketOrderAsync(string symbol, string side, decimal quantity)
        {
            var parameters = new Dictionary<string, string>
            {
                { "symbol", symbol }, // ✅ Без пробелов
                { "side", side.ToUpper() },
                { "type", "MARKET" },
                { "quantity", quantity.ToString(System.Globalization.CultureInfo.InvariantCulture) },
                { "positionSide", "BOTH" } // 🔥 ОБЯЗАТЕЛЬНО для USDT-M фьючерсов
            };

            try
            {
                var result = await SendRequestAsync(HttpMethod.Post, "/openApi/swap/v2/trade/order", parameters);
                
                if (result["code"]?.Value<int>() == 0)
                {
                    // 🔥 ИСПРАВЛЕНИЕ: Парсим реальный OrderId из ответа
                    var orderId = result["data"]?["orderId"]?.Value<string>();
                    if (string.IsNullOrEmpty(orderId))
                    {
                        orderId = result["data"]?["clientOrderId"]?.Value<string>();
                    }
                    
                    var status = result["data"]?["status"]?.Value<string>() ?? "NEW";
                    
                    Log.Info($"[BingX] Ордер выставлен: {symbol} {side} {quantity} qty @ MARKET. OrderId: {orderId}, Status: {status}");
                    
                    return new BingXOrderResult
                    {
                        Success = true,
                        OrderId = orderId,
                        Symbol = symbol,
                        Status = status
                    };
                }
                
                var errorMsg = result["msg"]?.Value<string>() ?? "Unknown error";
                Log.Error($"[BingX] Order failed: code={result["code"]}, msg={errorMsg}");
                
                return new BingXOrderResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = errorMsg
                };
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Ошибка ордера: {ex.Message}", ex);
                return new BingXOrderResult
                {
                    Success = false,
                    Symbol = symbol,
                    ErrorMessage = ex.Message
                };
            }
        }

        /// <summary>
        /// Отмена ордера по OrderId
        /// </summary>
        public async Task<bool> CancelOrderAsync(string symbol, string orderId)
        {
            var parameters = new Dictionary<string, string>
            {
                { "symbol", symbol },
                { "orderId", orderId }
            };

            try
            {
                var result = await SendRequestAsync(HttpMethod.Post, "/openApi/swap/v2/trade/cancelOrder", parameters);
                
                if (result["code"]?.Value<int>() == 0)
                {
                    Log.Info($"[BingX] Ордер отменен: {symbol} OrderId: {orderId}");
                    return true;
                }
                
                var errorMsg = result["msg"]?.Value<string>();
                Log.Error($"[BingX] Ошибка отмены ордера: {orderId} - {errorMsg}");
                return false;
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Ошибка при отмене ордера {orderId}: {ex.Message}", ex);
                return false;
            }
        }

        /// <summary>
        /// Получает статус ордера по ID
        /// </summary>
        public async Task<BingXOrderStatus?> GetOrderStatusAsync(string symbol, string orderId)
        {
            var parameters = new Dictionary<string, string>
            {
                { "symbol", symbol },
                { "orderId", orderId }
            };

            try
            {
                var result = await SendRequestAsync(HttpMethod.Get, "/openApi/swap/v2/trade/order", parameters);
                
                if (result["code"]?.Value<int>() == 0)
                {
                    var data = result["data"];
                    if (data != null)
                    {
                        var status = data["status"]?.Value<string>();
                        var executedQty = decimal.TryParse(
                            data["executedQty"]?.Value<string>() ?? "0",
                            out var qty
                        ) ? qty : 0m;

                        Log.Debug($"[BingX] Статус ордера {orderId}: {status}, заполнено {executedQty}");
                        
                        return new BingXOrderStatus
                        {
                            Status = status ?? "UNKNOWN",
                            ExecutedQty = executedQty
                        };
                    }
                }
                
                var errorMsg = result["msg"]?.Value<string>();
                Log.Error($"[BingX] Ошибка получения статуса ордера {orderId}: {errorMsg}");
                return null;
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Ошибка при получении статуса ордера {orderId}: {ex.Message}", ex);
                return null;
            }
        }

        // --- Внутренние механизмы ---


        /// <summary>
        /// Размещает ордер стоп-лосса на BingX (Conditional Order)
        /// </summary>
        public async Task<BingXOrderResult> PlaceStopLossOrderAsync(
            string symbol,
            string side,
            decimal quantity,
            decimal triggerPrice)
        {
            var parameters = new Dictionary<string, string>
            {
                { "symbol", symbol },
                { "side", side },
                { "quantity", quantity.ToString() },
                { "type", "LIMIT" },
                { "price", triggerPrice.ToString() },
                { "triggerPrice", triggerPrice.ToString() },
                { "triggerType", "MARK_PRICE" }
            };

            try
            {
                var result = await SendRequestAsync(HttpMethod.Post, "/openApi/swap/v2/trade/order", parameters);
                
                if (result["code"]?.Value<int>() == 0)
                {
                    var data = result["data"];
                    var orderId = data?["orderId"]?.Value<string>();
                    var orderStatus = data?["status"]?.Value<string>();
                    
                    Log.Info($"[BingX] STOP_LOSS placed: {symbol} {side} {quantity} @ {triggerPrice} (ID: {orderId})");
                    
                    return new BingXOrderResult
                    {
                        Success = true,
                        OrderId = orderId ?? "",
                        Symbol = symbol,
                        Status = orderStatus ?? "0"
                    };
                }
                
                var errorMsg = result["msg"]?.Value<string>();
                Log.Error($"[BingX] Error placing STOP_LOSS: {symbol} - {errorMsg}");
                return new BingXOrderResult { Success = false, ErrorMessage = errorMsg };
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Exception placing STOP_LOSS {symbol}: {ex.Message}", ex);
                return new BingXOrderResult { Success = false, ErrorMessage = ex.Message };
            }
        }

        /// <summary>
        /// Размещает ордер тейк-профита на BingX (Conditional Order)
        /// </summary>
        public async Task<BingXOrderResult> PlaceTakeProfitOrderAsync(
            string symbol,
            string side,
            decimal quantity,
            decimal triggerPrice)
        {
            var parameters = new Dictionary<string, string>
            {
                { "symbol", symbol },
                { "side", side },
                { "quantity", quantity.ToString() },
                { "type", "LIMIT" },
                { "price", triggerPrice.ToString() },
                { "triggerPrice", triggerPrice.ToString() },
                { "triggerType", "MARK_PRICE" }
            };

            try
            {
                var result = await SendRequestAsync(HttpMethod.Post, "/openApi/swap/v2/trade/order", parameters);
                
                if (result["code"]?.Value<int>() == 0)
                {
                    var data = result["data"];
                    var orderId = data?["orderId"]?.Value<string>();
                    var orderStatus = data?["status"]?.Value<string>();
                    
                    Log.Info($"[BingX] TAKE_PROFIT placed: {symbol} {side} {quantity} @ {triggerPrice} (ID: {orderId})");
                    
                    return new BingXOrderResult
                    {
                        Success = true,
                        OrderId = orderId ?? "",
                        Symbol = symbol,
                        Status = orderStatus ?? "0"
                    };
                }
                
                var errorMsg = result["msg"]?.Value<string>();
                Log.Error($"[BingX] Error placing TAKE_PROFIT: {symbol} - {errorMsg}");
                return new BingXOrderResult { Success = false, ErrorMessage = errorMsg };
            }
            catch (Exception ex)
            {
                Log.Error($"[BingX] Exception placing TAKE_PROFIT {symbol}: {ex.Message}", ex);
                return new BingXOrderResult { Success = false, ErrorMessage = ex.Message };
            }
        }


        private async Task<JObject> SendRequestAsync(HttpMethod method, string endpoint, Dictionary<string, string>? parameters = null, bool signed = true)
        {
            parameters ??= new Dictionary<string, string>();
            
            if (signed)
            {
                parameters["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
                var queryString = BuildQueryString(parameters);
                var signature = CreateSignature(queryString, _apiSecret);
                queryString += $"&signature={signature}";
                endpoint = $"{endpoint}?{queryString}";
            }
            else if (parameters.Count > 0)
            {
                endpoint = $"{endpoint}?{BuildQueryString(parameters)}";
            }

            HttpResponseMessage response;
            if (method == HttpMethod.Get)
            {
                response = await _httpClient.GetAsync($"{BaseUrl}{endpoint}");
            }
            else
            {
                response = await _httpClient.PostAsync($"{BaseUrl}{endpoint}", null);
            }

            var content = await response.Content.ReadAsStringAsync();
            return JObject.Parse(content);
        }

        private string BuildQueryString(Dictionary<string, string> parameters)
        {
            // ✅ Без пробелов вокруг &
            return string.Join("&", parameters.Select(kvp => $"{kvp.Key}={Uri.EscapeDataString(kvp.Value)}"));
        }

        private string CreateSignature(string parameters, string secret)
        {
            byte[] keyByte = Encoding.UTF8.GetBytes(secret);
            byte[] messageBytes = Encoding.UTF8.GetBytes(parameters);
            using var hmacsha256 = new HMACSHA256(keyByte);
            byte[] hashmessage = hmacsha256.ComputeHash(messageBytes);
            // ✅ Убираем дефисы полностью, без пробелов
            return BitConverter.ToString(hashmessage).Replace("-", "").ToLower();
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }

    public class BingXOrderStatus
    {
        public string Status { get; set; } = string.Empty;
        public decimal ExecutedQty { get; set; }
    }
}
