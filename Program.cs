using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Rendering;
using TradingBot.Models;
using TradingBot.External;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

class Program
{
    // Параметры позиции
    private static readonly decimal TotalOrderSize = Config.TotalOrderSize;

    // Параметры спреда
    // Используются конфигурационные значения из Config

    // Состояние
    private static readonly ConcurrentDictionary<string, SpreadState> ActiveSpreads = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, DateTime> ClosedSpreads = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, SpreadAnalysis> LatestAnalysis = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, CommonSymbol> SymbolsByName = new(StringComparer.OrdinalIgnoreCase);
    
    // Статистика с момента запуска
    private static int _successfulCloses = 0;
    private static string _lastSpreadStatus = "Ожидание";
    private static decimal _bingXBalance = 0;
    private static decimal _gateBalance = 0;
    private static bool _bingXApiConnected = false;
    private static bool _gateApiConnected = false;
    
    private static int _lastTopSpreadsCount = -1;
    private static long _totalCheckedSymbols = 0;
    private static long _totalProfitableSymbols = 0;
    
    private static List<CommonSymbol> _commonSymbols = new();
    private static List<BingXFuturesOrderBookProvider> _bingXProviders = new();
    private static GateFuturesOrderBookProvider? _gateProvider;
    private static CancellationTokenSource? _cts;
    private static Task? _liveRenderTask;
    private static DateTime _lastRender = DateTime.UtcNow;
    private static bool _hasLoggedAnyLatestAnalysis = false;

    // НОВЫЕ КОМПОНЕНТЫ ДЛЯ ИСПОЛНЕНИЯ ОРДЕРОВ
    private static SpreadAnalyzer? _spreadAnalyzer;
    private static ExecutionWorker? _executionWorker;
    private static BingXApiClient? _bingXApiClient;
    private static GateApiClient? _gateApiClient;

    static async Task Main()
    {
        // Инициализируем логирование: в файл пишем только важные сообщения,
        // а для консоли оставляем только предупреждения и ошибки после запуска таблицы.
        Log.Initialize(fileLogLevel: LogLevel.Info, consoleLogLevel: LogLevel.Info);
        
        Log.Info("═══════════════════════════════════════════════════════════");
        Log.Info("    SPREADER - Арбитражный бот BingX ↔ Gate");
        Log.Info("═══════════════════════════════════════════════════════════");
        Log.Info($"Параметры позиции: {Config.PositionSizeUSDT} USDT × {Config.Leverage}x = {Config.TotalOrderSize} USDT");
        Log.Info($"Минимальный спред: {Config.MinimumSpreadPercent}% | Комиссия: {Config.FeePercent}%");

        _cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            _cts.Cancel();
        };

        try
        {
            _commonSymbols = await ExchangeSymbolDiscovery.GetCommonPerpetualUSDTFuturesAsync(_cts.Token);
            if (!_commonSymbols.Any())
            {
                Log.Info("Общих USDT-фьючерсных пар не найдено.");
                return;
            }

            var filteredSymbols = _commonSymbols
                .Where(p => !p.Name.Contains("1000000", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.BingXSymbol.Contains("1000000", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.GateSymbol.Contains("1000000", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.Name.Contains("1000", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.BingXSymbol.Contains("1000", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.GateSymbol.Contains("1000", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.Name.Equals("0G-USDT", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.BingXSymbol.Equals("0G-USDT", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.GateSymbol.Equals("0G-USDT", StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (filteredSymbols.Count != _commonSymbols.Count)
            {
                Log.Info($"Отфильтровано {_commonSymbols.Count - filteredSymbols.Count} подозрительных пар для WS.");
            }

            _commonSymbols = filteredSymbols;
            Log.Info($"✓ Найдено {_commonSymbols.Count} общих USDT-фьючерсных пар");

            var bingXValidSymbols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                var bingXContracts = await ExchangeRestClients.BingXRestClient.PerpetualFuturesApi.ExchangeData.GetContractsAsync(string.Empty, _cts.Token);
                if (bingXContracts.Success && bingXContracts.Data != null)
                {
                    foreach (var contract in bingXContracts.Data)
                    {
                        if (!string.IsNullOrEmpty(contract.Symbol) && contract.Symbol.EndsWith("-USDT", StringComparison.OrdinalIgnoreCase))
                            bingXValidSymbols.Add(contract.Symbol);
                    }
                    Log.Info($"[BingX] Загружено {bingXValidSymbols.Count} валидных фьючерсных пар BingX");
                }
                else
                {
                    Log.Warn($"[BingX] Не удалось получить список контрактов: {bingXContracts.Error?.Message}");
                }
            }
            catch (Exception ex)
            {
                Log.Warn($"[BingX] Не удалось загрузить список контрактов: {ex.Message}");
            }

            if (bingXValidSymbols.Count > 0)
            {
                var beforeFilter = _commonSymbols.Count;
                _commonSymbols = _commonSymbols.Where(p => bingXValidSymbols.Contains(p.BingXSymbol)).ToList();
                if (_commonSymbols.Count != beforeFilter)
                    Log.Info($"[BingX] Отфильтровано {beforeFilter - _commonSymbols.Count} недоступных фьючерсных пар BingX.");
            }
            
            // Log.Debug($"Кэширование {_commonSymbols.Count} символов...");

            // Кэшируем символы для быстрого поиска (вызывается 100s раз в секунду)
            foreach (var symbol in _commonSymbols)
            {
                SymbolsByName[symbol.Name] = symbol;
                SymbolsByName[symbol.BingXSymbol] = symbol;
                SymbolsByName[symbol.GateSymbol] = symbol;
            }

            // Log.Debug("Кэширование завершено.");
            Log.Info($"Подключение к BingX ({_commonSymbols.Count} пар)...");
            var bingXProvider = new BingXFuturesOrderBookProvider();
            _bingXProviders.Add(bingXProvider);
            await bingXProvider.StartAsync(_commonSymbols.Select(s => s.BingXSymbol), _cts.Token);

            Log.Info($"Подключение к Gate ({_commonSymbols.Count} пар)...");
            _gateProvider = new GateFuturesOrderBookProvider(_commonSymbols.Select(s => s.GateSymbol));
            await _gateProvider.StartAsync(_cts.Token);
            
            Log.Info("Погрузка данных со стаканов...");
            // Ждём, пока все провайдеры получат все символы
            var readyTasks = new List<Task>();
            readyTasks.AddRange(_bingXProviders.Select(p => p.WaitForReadyAsync(_cts.Token)));
            readyTasks.Add(_gateProvider.WaitForReadyAsync(_cts.Token));
            await Task.WhenAll(readyTasks);
            // Log.Debug("Все провайдеры готовы");
            Log.Info("✓ Все источники данных подключены и готовы");

            // Log.Debug("Инициализация API клиентов...");
            // ИНИЦИАЛИЗАЦИЯ КОМПОНЕНТОВ ИСПОЛНЕНИЯ ОРДЕРОВ
            _bingXApiClient = new BingXApiClient(Config.BingXApiKey, Config.BingXApiSecret);
            _gateApiClient = new GateApiClient(Config.GateApiKey, Config.GateApiSecret);
            // Log.Debug("API клиенты инициализированы");
            
            // Log.Debug("Инициализация SpreadAnalyzer...");
            _spreadAnalyzer = new SpreadAnalyzer(
                _bingXProviders,
                _gateProvider,
                _bingXApiClient,
                _gateApiClient,
                Config.TotalOrderSize,
                Config.MinimumSpreadPercent,
                Config.FeePercent,
                Config.MaxOpenSpreads
            );
            // Log.Debug("Инициализация ExecutionWorker...");
            _executionWorker = new ExecutionWorker(
                _bingXProviders,
                _gateProvider,
                _bingXApiClient,
                _gateApiClient,
                Config.TotalOrderSize,
                Config.MinimumSpreadPercent,
                Config.CloseThresholdPercent,
                Config.FeePercent,
                Config.MaxOpenSpreads
            );
            // Log.Debug("Компоненты анализа и исполнения инициализированы");

            // Инициализируем статусы API (пока считаем подключенными)
            _bingXApiConnected = !string.IsNullOrEmpty(Config.BingXApiKey) && !string.IsNullOrEmpty(Config.BingXApiSecret);
            _gateApiConnected = !string.IsNullOrEmpty(Config.GateApiKey) && !string.IsNullOrEmpty(Config.GateApiSecret);
            // Log.Debug($"Статус API: BingX={_bingXApiConnected}, Gate={_gateApiConnected}");

            // Получаем начальные балансы
            // Log.Debug("Загрузка начальных балансов...");
            await UpdateBalancesAsync(_cts.Token);
            // Запускаем рендеринг через Spectre.Console Live
            // Log.Debug("Запуск рендеринга таблицы...");
            StartLiveRender(_cts.Token);
            Log.SetConsoleLogLevel(LogLevel.Error); // скрываем все обычные логи, чтобы таблица не ломалась выводом

            // Запускаем периодическое обновление балансов
            // Log.Debug("Запуск цикла обновления балансов...");
            var balanceTimer = new PeriodicTimer(TimeSpan.FromSeconds(30)); // Обновление каждые 30 секунд
            var balanceUpdateTask = RunBalanceUpdateLoopAsync(balanceTimer, _cts.Token);

            // Запускаем периодический вывод статистики анализа в файл логов
            var statsTimer = new PeriodicTimer(TimeSpan.FromSeconds(30));
            var statsTask = RunStatsLoggingLoopAsync(statsTimer, _cts.Token);

            // Подписываемся на обновления от обеих бирж (ОДИН РАЗ)
            // Log.Debug("Подписка на обновления стаканов...");
            SubscribeToOrderBookUpdates();

            // ЗАПУСК ОСНОВНОГО ЦИКЛА ИСПОЛНЕНИЯ ОРДЕРОВ
            // Log.Debug("Запуск основного цикла анализа спредов и исполнения...");
            Log.Info("✓ Запуск аналитики спредов...");
            var executionTask = RunExecutionLoopAsync(_cts.Token);
            var monitoringTask = RunMonitoringAsync(_commonSymbols.Select(s => s.Name).ToList());

            // Ждём отмены
            await Task.Delay(Timeout.Infinite, _cts.Token);
            await Task.WhenAll(monitoringTask, balanceUpdateTask, statsTask, executionTask);
        }
        catch (OperationCanceledException)
        {
            // Ожидаемое завершение по Ctrl+C
        }
        catch (Exception ex)
        {
            Log.Error("Критическая ошибка", ex);
        }
        finally
        {
            // 1️⃣ Сначала отменяем все задачи
            _cts?.Cancel();
            
            // 2️⃣ Ждём, пока они заметят отмену (500мс достаточно)
            await Task.Delay(500);
            
            // 3️⃣ Только теперь диспозим ресурсы
            _liveRenderTask = null;
            _bingXApiClient?.Dispose();
            _gateApiClient?.Dispose();
            foreach (var provider in _bingXProviders) provider?.Dispose();
            _gateProvider?.Dispose();
            _cts?.Dispose();
            
            Log.Info("Программа завершена.");
            await Log.DisposeAsync();
        }
    }

    static async Task RunMonitoringAsync(List<string> commonSymbols)
    {
#pragma warning disable CS8602 // Suppress nullable warnings in this method
        var sortedSymbols = commonSymbols.OrderBy(s => s).ToList();
        
        while (true)
        {
            foreach (var symbol in sortedSymbols)
            {
                // Берем данные из провайдеров (которые обновляются в фоне по WebSocket)
                var bingXOB = _bingXProviders.Select(p => p.TryGetOrderBook(symbol)).FirstOrDefault(ob => ob != null);
                var gateOB = _gateProvider?.TryGetOrderBook(symbol);

                if (gateOB == null || bingXOB == null || gateOB.Bids.Count == 0 || bingXOB.Bids.Count == 0) continue;

                if (bingXOB == null) continue; // Дополнительная проверка для компилятора

                // Расчет спреда (BingX Buy -> Gate Sell)
                decimal spread = (gateOB.Bids[0].Price - bingXOB.Asks[0].Price) / bingXOB.Asks[0].Price * 100;

                if (spread >= 3.0m) 
                {
                    // Проверяем, хватит ли объема
                    if (gateOB.Bids[0].Quantity > 0.1m && bingXOB.Asks[0].Quantity > 0.1m) 
                    {
                        Log.Info($"[!] СИГНАЛ: {symbol} | Спред: {spread:F2}%");
                        // await ExecuteTrade(symbol, spread); // TODO: интегрировать с ExecutionWorker
                    }
                }
            }
            
            // Маленькая пауза, чтобы не забивать поток, но сохранять скорость
            await Task.Delay(1); 
        }
    }

    static void SubscribeToOrderBookUpdates()
    {
        if (!_bingXProviders.Any() || _gateProvider is null)
            return;

        foreach (var provider in _bingXProviders)
        {
            provider.OnOrderBookUpdated += (symbol) => OnOrderBookChanged(symbol);
        }
        _gateProvider.OnOrderBookUpdated += (symbol) => OnOrderBookChanged(symbol);
    }

    static async void OnOrderBookChanged(string symbolName)
    {
        _totalCheckedSymbols++;

        if (!_bingXProviders.Any() || _gateProvider is null)
            return;

        if (!TryGetCommonSymbol(symbolName, out var symbol) || symbol is null)
            return;

        var analyses = await _spreadAnalyzer.AnalyzeSpreadsAsync();
        
        var analysis = analyses.FirstOrDefault(a => a.Symbol == symbol.Name);
        if (analysis == null)
        {
            LatestAnalysis.TryRemove(symbol.Name, out _);
            return;
        }

        LatestAnalysis[symbol.Name] = analysis;
        _totalProfitableSymbols++;

        var isNewAnalysis = !LatestAnalysis.ContainsKey(symbol.Name);

        if (!_hasLoggedAnyLatestAnalysis && LatestAnalysis.Count > 0)
        {
            Log.Info($"[TOP] Первое попадание в LatestAnalysis: {analysis.Symbol} {analysis.NetPercent:+0.000;-0.000}%");
            _hasLoggedAnyLatestAnalysis = true;
        }

        if (isNewAnalysis)
        {
            Log.Info($"[TOP] Новая пара в LatestAnalysis: {analysis.Symbol} {analysis.NetPercent:+0.000;-0.000}%");
        }

        RenderIfNeeded();
    }

    static bool TryGetCommonSymbol(string symbolName, out CommonSymbol? symbol)
    {
        if (SymbolsByName.TryGetValue(symbolName, out symbol))
            return true;

        symbolName = SymbolHelpers.NormalizeTicker(symbolName);
        return !string.IsNullOrEmpty(symbolName) && SymbolsByName.TryGetValue(symbolName, out symbol);
    }

    private sealed record SpreadMetrics(
        SpreadDirection Direction,
        decimal RawSpread,
        decimal RawPercent,
        decimal NetPercent,
        bool HasLiquidity,
        decimal OrderQty,
        decimal BingXPrice,
        decimal GatePrice);

    private static decimal SumAvailableQuantity(IEnumerable<(decimal Price, decimal Quantity)> levels, decimal requiredQuantity)
    {
        var total = 0m;
        foreach (var level in levels)
        {
            total += level.Quantity;
            if (total >= requiredQuantity)
                return total;
        }

        return total;
    }

    private static SpreadMetrics CalculateSpreadMetrics(
        SpreadDirection direction,
        decimal buyPrice,
        decimal sellPrice,
        IEnumerable<OrderBookLevel> buyLevels,
        IEnumerable<OrderBookLevel> sellLevels)
    {
        // 🔥 КРИТИЧНО: Защита от DivideByZero
        if (buyPrice <= 0m || sellPrice <= 0m)
        return new SpreadMetrics(direction, 0m, 0m, 0m, false, 0m, buyPrice, sellPrice);

        var rawSpread = sellPrice - buyPrice;
        var avgPrice = (buyPrice + sellPrice) / 2;
        var rawPercent = avgPrice != 0 ? rawSpread / avgPrice * 100m : 0;
        var totalCostPercent = Config.FeePercent * 2;
        var netPercent = rawPercent - totalCostPercent;
        var orderQty = avgPrice != 0 ? TotalOrderSize / avgPrice : 0;
        var buyAvailableQuantity = SumAvailableQuantity(buyLevels.Select(l => (l.Price, l.Quantity)), orderQty);
        var sellAvailableQuantity = SumAvailableQuantity(sellLevels.Select(l => (l.Price, l.Quantity)), orderQty);
        var hasLiquidity = buyAvailableQuantity >= orderQty && sellAvailableQuantity >= orderQty;

        return new SpreadMetrics(direction, rawSpread, rawPercent, netPercent, hasLiquidity, orderQty, buyPrice, sellPrice);
    }

    private static SpreadAnalysis? AnalyzeSpread(CommonSymbol symbol)
    {
        if (!_bingXProviders.Any() || _gateProvider is null)
            return null;

        var normalizedBingX = symbol.BingXSymbol.Replace("-", "").ToUpperInvariant();
        var bingXOb = _bingXProviders.Select(p => p.TryGetOrderBook(normalizedBingX)).FirstOrDefault(ob => ob != null)
                     ?? _bingXProviders.Select(p => p.TryGetOrderBook(symbol.BingXSymbol)).FirstOrDefault(ob => ob != null);
        var gateOb = _gateProvider.TryGetOrderBook(symbol.GateSymbol);

        // ✅ ПРОВЕРКА 1: Стаканы существуют
        if (gateOb == null || bingXOb == null)
            return null;

        // ✅ ПРОВЕРКА 2: В стаканах есть данные (не пустые списки)
        if (gateOb.Bids.Count == 0 || gateOb.Asks.Count == 0)
            return null;
        
        if (bingXOb.Bids.Count == 0 || bingXOb.Asks.Count == 0)
            return null;

        // ✅ ПРОВЕРКА 3: Цены больше 0
        if (gateOb.Bids[0].Price <= 0 || gateOb.Asks[0].Price <= 0 ||
            bingXOb.Bids[0].Price <= 0 || bingXOb.Asks[0].Price <= 0)
            return null;

        var now = DateTime.UtcNow;
        var gateAge = now - gateOb.Timestamp;
        var bingXAge = now - bingXOb.Timestamp;
        
        if (gateAge.TotalMilliseconds > Config.MaxOrderBookAgeMs || bingXAge.TotalMilliseconds > Config.MaxOrderBookAgeMs)
            return null;

        var delta = Math.Abs((gateOb.Timestamp - bingXOb.Timestamp).TotalMilliseconds);
        if (delta > Config.MaxOrderBookTimeDeltaMs)
            return null;

        // Теперь БЕЗОПАСНО обращаемся к [0], так как проверили Count > 0 и цены > 0
        var gateBestBid = gateOb.Bids[0].Price;
        var gateBestAsk = gateOb.Asks[0].Price;
        var bingXBestBid = bingXOb.Bids[0].Price;
        var bingXBestAsk = bingXOb.Asks[0].Price;

        // Направление 1: Купить на BingX, Продать на Gate
        var optionA = CalculateSpreadMetrics(
            SpreadDirection.BingXToGate,
            bingXBestAsk, 
            gateBestBid,
            bingXOb.Asks,
            gateOb.Bids);

        // Направление 2: Купить на Gate, Продать на BingX
        var optionB = CalculateSpreadMetrics(
            SpreadDirection.GateToBingX,
            gateBestAsk,
            bingXBestBid,
            gateOb.Asks,
            bingXOb.Bids);

        var chosenOption = optionA.RawPercent >= optionB.RawPercent ? optionA : optionB;

        return new SpreadAnalysis
        {
            Symbol = symbol.Name,
            Direction = (TradingBot.Models.SpreadDirection)chosenOption.Direction,
            BuyPrice = chosenOption.BingXPrice,
            SellPrice = chosenOption.GatePrice,
            BingXPrice = chosenOption.BingXPrice,
            GateSymbol = symbol.GateSymbol,
            RawSpread = chosenOption.RawSpread,
            RawPercent = chosenOption.RawPercent,
            NetPercent = chosenOption.NetPercent,
            OrderQtyCoins = chosenOption.OrderQty,
            HasLiquidity = chosenOption.HasLiquidity,
            Timestamp = DateTime.UtcNow
        };
    }

    private static bool HasValidOrderBooksForOpen(SpreadAnalysis spread)
    {
        var (buyExchange, sellExchange) = GetExchangesFromDirection(spread.Direction);

        var buyOb = GetOrderBook(buyExchange, spread);
        var sellOb = GetOrderBook(sellExchange, spread);

        if (buyOb == null || sellOb == null)
            return false;

        if (buyOb.Bids.Count == 0 || buyOb.Asks.Count == 0 ||
            sellOb.Bids.Count == 0 || sellOb.Asks.Count == 0)
            return false;

        if (buyOb.Bids[0].Price <= 0 || buyOb.Asks[0].Price <= 0 ||
            sellOb.Bids[0].Price <= 0 || sellOb.Asks[0].Price <= 0)
            return false;

        var now = DateTime.UtcNow;
        if ((now - buyOb.Timestamp).TotalMilliseconds > Config.MaxOrderBookAgeMs ||
            (now - sellOb.Timestamp).TotalMilliseconds > Config.MaxOrderBookAgeMs)
            return false;

        var delta = Math.Abs((buyOb.Timestamp - sellOb.Timestamp).TotalMilliseconds);
        return delta <= Config.MaxOrderBookTimeDeltaMs;
    }

    private static (ExchangeType BuyExchange, ExchangeType SellExchange) GetExchangesFromDirection(SpreadDirection direction)
    {
        return direction switch
        {
            SpreadDirection.BingXToGate => (ExchangeType.BingX, ExchangeType.Gate),
            SpreadDirection.GateToBingX => (ExchangeType.Gate, ExchangeType.BingX),
            _ => (ExchangeType.BingX, ExchangeType.Gate)
        };
    }

    private static OrderBookData? GetOrderBook(ExchangeType exchange, SpreadAnalysis spread)
    {
        return exchange switch
        {
            ExchangeType.BingX => _bingXProviders.Select(p => p.TryGetOrderBook(spread.BingXSymbol.Replace("-", "").ToUpperInvariant())).FirstOrDefault(ob => ob != null),
            ExchangeType.Gate => _gateProvider?.TryGetOrderBook(spread.GateSymbol),
            _ => null
        };
    }

    private static void UpdateSpreadState(SpreadAnalysis analysis)
    {
        // СТАРАЯ ЛОГИКА УДАЛЕНА - теперь используется ExecutionWorker
        // Просто обновляем LatestAnalysis для отображения
        LatestAnalysis[analysis.Symbol] = analysis;
    }

    private static void TryOpenBestAvailableSpread()
    {
        // СТАРАЯ ЛОГИКА УДАЛЕНА - теперь используется ExecutionWorker в RunExecutionLoopAsync
    }


    private static void RenderIfNeeded()
    {
        if (_liveRenderTask is not null)
            return;

        var now = DateTime.UtcNow;
        if ((now - _lastRender).TotalMilliseconds < Config.RenderIntervalMs)
            return;

        _lastRender = now;
        Render(now);
    }

    private static void Render(DateTime now)
    {
        var dashboard = BuildDashboard(now);
        AnsiConsole.Write(dashboard);
    }

    private static void StartLiveRender(CancellationToken cancellationToken)
    {
        if (_liveRenderTask is not null)
            return;

        _liveRenderTask = Task.Run(() =>
        {
            var initialDashboard = BuildDashboard(DateTime.UtcNow);
            var live = AnsiConsole.Live(initialDashboard)
                .AutoClear(true)
                .Overflow(VerticalOverflow.Ellipsis);

            live.Start(ctx =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    AnsiConsole.Clear();
                    ctx.UpdateTarget(BuildDashboard(DateTime.UtcNow));
                    ctx.Refresh();
                    Thread.Sleep(Config.RenderIntervalMs);
                }
            });
        }, cancellationToken);
    }

    private static IRenderable BuildDashboard(DateTime now)
    {
        // Получаем реальные балансы асинхронно (но для синхронного метода dashboard используем кэшированные значения)
        // Обновление балансов происходит в фоне
        
        // === АКТИВНЫЕ СПРЕДЫ (левая верхняя таблица) ===
        var activeTable = new Table()
            .RoundedBorder()
            .AddColumns(
                new TableColumn("Символ").Centered(),
                new TableColumn("Вход %").Centered(),
                new TableColumn("Закрытие %").Centered(),
                new TableColumn("Направление").Centered(),
                new TableColumn("PNL %").Centered(),
                new TableColumn("Время").Centered());

        var activePositions = _executionWorker?.GetOpenPositions() ?? new Dictionary<string, OpenPosition?>();
        
        if (activePositions.Count == 0)
        {
            activeTable.AddRow("[grey]Нет активных спредов[/]", "-", "-", "-", "-", "-");
        }
        else
        {
            foreach (var (symbol, position) in activePositions.OrderByDescending(x => x.Value?.OpenTime ?? DateTime.MinValue))
            {
                if (position == null) continue;
                var currentPnL = _executionWorker?.GetCurrentPnL(symbol) ?? 0;
                var pnlColor = currentPnL >= 0 ? "[green]" : "[red]";
                var pnlText = $"{pnlColor}{currentPnL:+0.00;-0.00}%[/]";
                
                activeTable.AddRow(
                    position.Symbol,
                    $"{position.EntrySpread:+0.00;-0.00}%",
                    $"{Config.CloseThresholdPercent:F2}%",
                    position.Direction == (TradingBot.Models.SpreadDirection)SpreadDirection.BingXToGate ? "X→G" : "G→X",
                    pnlText,
                    FormatAge(position.OpenDuration.TotalSeconds));
            }
        }

        // === КАНДИДАТЫ (левая нижняя таблица) ===
        var candidateTable = new Table()
            .RoundedBorder()
            .AddColumns(
                new TableColumn("Монета").Centered(),
                new TableColumn("Направление").Centered(),
                new TableColumn("Разница %").Centered(),
                new TableColumn("Схождение %").Centered(),
                new TableColumn("Age").Centered());

        var currentCandidates = LatestAnalysis.Values
            .Where(x => !ActiveSpreads.ContainsKey(x.Symbol))
            .Where(x => x.NetPercent >= Config.MinimumSpreadPercent)
            .OrderByDescending(x => x.NetPercent)
            .Take(Config.MaxDisplayRows)
            .ToArray();

        if (currentCandidates.Length == 0)
        {
            candidateTable.AddRow("[grey]Нет кандидатов[/]", "-", "-", "-", "-");
        }
        else
        {
            foreach (var candidate in currentCandidates)
            {
                // 🔥 ПРОВЕРКА АКТУАЛЬНОСТИ ДАННЫХ
                var age = now - candidate.Timestamp;
                bool isStale = age.TotalMilliseconds > 2000; // Предупреждение если данные старше 2 секунд

                string percentText = FormatPercent(candidate.RawPercent);
                
                // Если данные старые, зачеркиваем профит или делаем серым/красным, чтобы не вводить в заблуждение
                // Но саму строку НЕ удаляем, как ты просил
                if (isStale)
                {
                    percentText = $"[grey]{candidate.RawPercent:F2}% ⚠️[/]"; 
                }

                candidateTable.AddRow(
                    candidate.Symbol,
                    candidate.DirectionLabel,
                    percentText,
                    $"{Config.CloseThresholdPercent:F2}%",
                    $"{age.TotalMilliseconds:F0}ms");
            }
        }

        // === ТОП СПРЕДЫ (новая таблица внизу) ===
        var topSpreadsTable = new Table()
            .RoundedBorder()
            .AddColumns(
                new TableColumn("#").Centered(),
                new TableColumn("Монета").Centered(),
                new TableColumn("Направление").Centered(),
                new TableColumn("Спред %").Centered(),
                new TableColumn("Чистая %").Centered(),
                new TableColumn("Age").Centered());

        var topSpreads = LatestAnalysis.Values
            .Where(x => !ActiveSpreads.ContainsKey(x.Symbol))
            .OrderByDescending(x => x.NetPercent)
            .Take(10)
            .ToArray();

        if (topSpreads.Length != _lastTopSpreadsCount)
        {
            if (topSpreads.Length == 0)
            {
                Log.Info("[TOP] Нет доступных топ-спредов в LatestAnalysis");
            }
            else
            {
                Log.Info($"[TOP] Найдено {topSpreads.Length} топ-спредов. Лучший: {topSpreads[0].Symbol} {topSpreads[0].NetPercent:+0.000;-0.000}%");
            }
            _lastTopSpreadsCount = topSpreads.Length;
        }

        if (topSpreads.Length == 0)
        {
            topSpreadsTable.AddRow("-", "[grey]Нет спредов[/]", "-", "-", "-", "-");
        }
        else
        {
            for (int i = 0; i < topSpreads.Length; i++)
            {
                var spread = topSpreads[i];
                var age = now - spread.Timestamp;
                
                var spreadColor = spread.NetPercent >= 0 ? "[green]" : "[red]";
                var spreadText = $"{spreadColor}{spread.RawPercent:F3}%[/]";
                var netText = $"{spreadColor}{spread.NetPercent:+0.000;-0.000}%[/]";

                topSpreadsTable.AddRow(
                    $"[bold]{i + 1}[/]",
                    spread.Symbol,
                    spread.DirectionLabel,
                    spreadText,
                    netText,
                    $"{age.TotalSeconds:F1}s");
            }
        }

        // === СТАТИСТИКА (правая таблица) ===
        var statsTable = new Table()
            .AddColumns(
                new TableColumn("Параметр").LeftAligned(),
                new TableColumn("Значение").LeftAligned());
        statsTable.AddRow("Время", now.ToString("HH:mm:ss"));
        
        // Статус API ключей
        var bingXStatus = _bingXApiConnected ? "[green]✓[/]" : "[red]✗[/]";
        var gateStatus = _gateApiConnected ? "[green]✓[/]" : "[red]✗[/]";
        statsTable.AddRow("BingX API", bingXStatus);
        statsTable.AddRow("Gate API", gateStatus);
        
        // Балансы
        statsTable.AddRow("BingX баланс", $"{_bingXBalance:F2} USDT");
        statsTable.AddRow("Gate баланс", $"{_gateBalance:F2} USDT");
        statsTable.AddRow("Общий баланс", $"{(_bingXBalance + _gateBalance):F2} USDT");
        
        // Позиции
        statsTable.AddRow("Открытые позиции", activePositions.Count.ToString());
        statsTable.AddRow("Успешные закрытия", _successfulCloses.ToString());
        statsTable.AddRow("Последний спред", _lastSpreadStatus);
        
        statsTable.Border = TableBorder.Rounded;
        statsTable.HideHeaders();

        // === СОЗДАНИЕ МАКЕТА ===
        var activePanel = new Panel(activeTable)
            .Header("[bold green]АКТИВНЫЕ СПРЕДЫ[/]")
            .Border(BoxBorder.Rounded)
            .Padding(1, 1);

        var candidatePanel = new Panel(candidateTable)
            .Header("[bold yellow]КАНДИДАТЫ[/]")
            .Border(BoxBorder.Rounded)
            .Padding(1, 1);

        var topSpreadsPanel = new Panel(topSpreadsTable)
            .Header("[bold cyan]⭐ ТОП СПРЕДЫ (обновляется каждые 5-10 сек)[/]")
            .Border(BoxBorder.Rounded)
            .Padding(1, 1);

        var statsPanel = new Panel(statsTable)
            .Header("[bold blue]СТАТИСТИКА[/]")
            .Border(BoxBorder.Rounded)
            .Padding(1, 1);

        var grid = new Grid();
        grid.AddColumn(new GridColumn().NoWrap().PadRight(2));
        grid.AddColumn(new GridColumn().NoWrap().Width(50));
        grid.AddRow(new Rows(activePanel, candidatePanel), statsPanel);
        grid.AddRow(topSpreadsPanel);

        return grid;
    }

    private static string FormatPercent(decimal value)
    {
        if (value > 0)
            return $"[green]{value:+0.00;-0.00}[/]";

        if (value < 0)
            return $"[red]{value:+0.00;-0.00}[/]";

        return $"[grey]{value:+0.00;-0.00}[/]";
    }

    private static string FormatAge(double ageSeconds) => $"[grey]{ageSeconds:0.0}s[/]";

    private static string FormatLiquidity(bool hasLiquidity) => hasLiquidity ? "[green]✓[/]" : "[red]✗[/]";

    private static async Task RunBalanceUpdateLoopAsync(PeriodicTimer periodicTimer, CancellationToken cancellationToken)
    {
        try
        {
            while (await periodicTimer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    await UpdateBalancesAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Log.Error("Ошибка периодического обновления балансов", ex);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // ожидаемое завершение при отмене
        }
    }

    private static async Task RunStatsLoggingLoopAsync(PeriodicTimer periodicTimer, CancellationToken cancellationToken)
    {
        try
        {
            while (await periodicTimer.WaitForNextTickAsync(cancellationToken))
            {
                try
                {
                    var profitablePercent = _totalCheckedSymbols > 0 
                        ? (_totalProfitableSymbols * 100.0 / _totalCheckedSymbols) 
                        : 0.0;
                    var latestCount = LatestAnalysis.Count;
                    var activeCount = ActiveSpreads.Count;

                    Log.Info($"[STATS] Проверено {_totalCheckedSymbols} (прибыльных {_totalProfitableSymbols}, {profitablePercent:F1}%) | В LatestAnalysis: {latestCount} | Открытых позиций: {activeCount}");
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Log.Error("Ошибка логирования статистики", ex);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // ожидаемое завершение при отмене
        }
    }

    private static async Task RunExecutionLoopAsync(CancellationToken cancellationToken)
    {
        Log.Info("Запуск цикла исполнения ордеров...");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // 1. СКАНИРОВАНИЕ ОТКРЫТЫХ ПОЗИЦИЙ НА ЗАКРЫТИЕ
                    if (_executionWorker != null)
                    {
                        var closedResults = await _executionWorker.ScanAndClosePositionsAsync(cancellationToken);
                        foreach (var closed in closedResults)
                        {
                            if (closed.Success)
                            {
                                // Log.Info($"[EXEC] ✓ Закрыта позиция {closed.Symbol} ({closed.CloseReason}). PNL: {closed.TotalProfit:F3}%");
                                
                                // Синхронизируем с локальным состоянием
                                ActiveSpreads.TryRemove(closed.Symbol, out _);
                                ClosedSpreads[closed.Symbol] = DateTime.UtcNow;
                                
                                // ⭐ КРИТИЧНО: Удаляем из SpreadAnalyzer, чтобы пара снова была доступна для анализа/открытия
                                if (_spreadAnalyzer != null)
                                {
                                    _spreadAnalyzer.UnregisterPosition(closed.Symbol);
                                    // 🔥 УБРАЛИ ЧАСТЫЙ DEBUG ЛОГ
                                    // Log.Debug($"Пара {closed.Symbol} теперь доступна для переанализа");
                                }
                                
                                // Обновляем статистику успешных закрытий
                                _successfulCloses++;
                                _lastSpreadStatus = $"Закрыт {closed.Symbol} ({closed.CloseReason})";
                            }
                            else
                            {
                                Log.Warn($"[EXEC] ✗ Ошибка закрытия {closed.Symbol}: {closed.ErrorMessage}");
                                _lastSpreadStatus = $"Ошибка закрытия {closed.Symbol}";
                            }
                        }
                    }

                    // 1.5 ОБРАБОТКА ОЧЕРЕДИ ОТКАТА (для критических deadlock позиций)
                    // ProcessRollbackQueueAsync removed - not available in ExecutionWorker

                    // 2. АНАЛИЗ НОВЫХ СПРЕДОВ И ПОПЫТКА ОТКРЫТИЯ
                    var analyzer = _spreadAnalyzer;
                    var executor = _executionWorker;
                    if (analyzer != null && executor != null)
                    {
                        foreach (var symbol in _commonSymbols)
                        {
                            try
                            {
                                // Анализируем спред через SpreadAnalyzer
                                var spreadAnalyses = await analyzer.AnalyzeSpreadsAsync();
                                
                                foreach (var spreadAnalysis in spreadAnalyses.Where(sa => sa.Symbol == symbol.Name))
                                {
                                    // 🔥 ДОПОЛНИТЕЛЬНАЯ ЗАЩИТА: Проверяем, что цены и количество валидны (не нули)
                                    if (spreadAnalysis.BingXPrice <= 0m || spreadAnalysis.GatePrice <= 0m || spreadAnalysis.OrderQtyCoins <= 0m)
                                    {
                                        Log.Warn($"[EXEC] ⚠ Пропускаем {symbol.Name}: невалидные цены или количество (BingX: {spreadAnalysis.BingXPrice}, Gate: {spreadAnalysis.GatePrice}, Qty: {spreadAnalysis.OrderQtyCoins})");
                                        continue;
                                    }

                                    // Проверяем, нет ли уже открытой позиции
                                    if (executor.GetPositionStatus(symbol.Name) == null && !ActiveSpreads.ContainsKey(symbol.Name))
                                    {
                                        // Пытаемся открыть спред
                                        var execResult = await executor.TryOpenSpreadAsync(spreadAnalysis, cancellationToken);
                                        
                                        if (execResult.Success)
                                        {
                                            // Log.Info($"[EXEC] ✓ Открыт спред {symbol.Name} ({spreadAnalysis.DirectionLabel}). Вход: {spreadAnalysis.NetPercent:F3}%");
                                            
                                            // Синхронизируем с локальным состоянием для отображения
                                            ActiveSpreads[symbol.Name] = new SpreadState
                                            {
                                                Spread = spreadAnalysis,
                                                OpenedAt = DateTime.UtcNow
                                            };
                                            
                                            // ⭐ КРИТИЧНО: Регистрируем в SpreadAnalyzer, чтобы он не переанализировал ее заново
                                            if (_spreadAnalyzer != null)
                                            {
                                                _spreadAnalyzer.RegisterOpenPosition(spreadAnalysis);
                                                // 🔥 УБРАЛИ ЧАСТЫЙ DEBUG ЛОГ
                                                // Log.Debug($"Пара {symbol.Name} зарегистрирована как открытая, больше не будет анализироваться");
                                            }
                                            
                                            // Обновляем статистику
                                            _lastSpreadStatus = $"Открыт {symbol.Name}";
                                        }
                                        else
                                        {
                                            Log.Warn($"[EXEC] ✗ Не удалось открыть {symbol.Name}: {execResult.ErrorMessage}");
                                            _lastSpreadStatus = $"Ошибка открытия {symbol.Name}";
                                        }
                                    }

                                    // Сохраняем результат анализа для отображения только если стаканы доступны для открытия
                                    if (spreadAnalysis != null && HasValidOrderBooksForOpen(spreadAnalysis))
                                    {
                                        LatestAnalysis[symbol.Name] = spreadAnalysis;
                                    }
                                    else
                                    {
                                        LatestAnalysis.TryRemove(symbol.Name, out _);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Log.Error($"[EXEC] Ошибка анализа {symbol.Name}: {ex.Message}", ex);
                            }
                        }
                    }

                    // Ждем перед следующей итерацией
                    await Task.Delay(1000, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.Error("[EXEC] Ошибка в цикле исполнения", ex);
                    await Task.Delay(5000, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ожидаемое завершение
        }

        Log.Info("Цикл исполнения ордеров завершен.");
    }

    private static bool AreSameSigned(decimal a, decimal b) => Math.Sign(a) == Math.Sign(b);

    private static bool ShouldCloseSpread(decimal rawPercent) => rawPercent < Config.CloseThresholdPercent;

    // Используем SpreadAnalysis из Models.cs вместо локального класса

    /// <summary>
    /// Обновляет балансы с бирж в реальном времени
    /// </summary>
    private static async Task UpdateBalancesAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_executionWorker == null)
                return;

            // Получаем балансы параллельно
            var bingXBalanceTask = _executionWorker.GetBingXBalanceAsync(CancellationToken.None);
            var gateBalanceTask = _executionWorker.GetGateBalanceAsync(CancellationToken.None);

            await Task.WhenAll(bingXBalanceTask, gateBalanceTask);

            var bingXBalance = await bingXBalanceTask;
            var gateBalance = await gateBalanceTask;

            // Обновляем статусы API и балансы
            _bingXApiConnected = bingXBalance.IsValid;
            _gateApiConnected = gateBalance.IsValid;

            if (bingXBalance.IsValid)
            {
                _bingXBalance = bingXBalance.AvailableBalance;
                Log.Info($"[BALANCE] BingX: {_bingXBalance:F2} USDT");
            }
            else
            {
                Log.Warn($"[BALANCE] Ошибка получения баланса BingX: {bingXBalance.ErrorMessage}");
            }

            if (gateBalance.IsValid)
            {
                _gateBalance = gateBalance.AvailableBalance;
                Log.Info($"[BALANCE] Gate: {_gateBalance:F2} USDT");
            }
            else
            {
                Log.Warn($"[BALANCE] Ошибка получения баланса Gate: {gateBalance.ErrorMessage}");
            }
        }
        catch (Exception ex)
        {
            Log.Error("Ошибка обновления балансов", ex);
        }
    }
}
