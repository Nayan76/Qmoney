package com.crio.warmup.stock;

import com.crio.warmup.stock.dto.*;
import com.crio.warmup.stock.log.UncaughtExceptionHandler;
import com.crio.warmup.stock.portfolio.PortfolioManager;
import com.crio.warmup.stock.portfolio.PortfolioManagerFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.web.client.RestTemplate;

public class PortfolioManagerApplication {
  // This is the comment i am adding.
  public static String getToken() {
    return "17a63defaa6efdfbd16bc421ba5886d3a0f8b754";
  }

  private static File resolveFileFromResources(String filename) throws URISyntaxException {
    return Paths.get(Thread.currentThread().getContextClassLoader().getResource(filename).toURI())
        .toFile();
  }

  // TODO: CRIO_TASK_MODULE_REST_API
  // Find out the closing price of each stock on the end_date and return the list
  // of all symbols in ascending order by its close value on end date.

  // Note:
  // 1. You may have to register on Tiingo to get the api_token.
  // 2. Look at args parameter and the module instructions carefully.
  // 2. You can copy relevant code from #mainReadFile to parse the Json.
  // 3. Use RestTemplate#getForObject in order to call the API,
  // and deserialize the results in List<Candle>
  private static String readFileAsString(String file) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(resolveFileFromResources(file).toPath()));
  }

  private static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }

    public static void printJsonObject(Object object) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writeValueAsString(object));
    }
    //////////////////////////////////////////////////////////////////////////////
    // MODULE 2 - Read File & Quotes
    //////////////////////////////////////////////////////////////////////////////
  public static List<String> mainReadFile(String[] args) throws IOException, URISyntaxException {
    if (args.length < 1) {
      throw new IllegalArgumentException("Missing filename argument.");
    }
    List<PortfolioTrade> trades = readTradesFromJson(args[0]);
    return trades.stream()
        .map(PortfolioTrade::getSymbol)
        .collect(Collectors.toList());
  }

  public static List<String> debugOutputs() {
    return Arrays.asList(
    "trades.json", 
    "trades.json", 
    "ObjectMapper", 
    "mainReadFile"
  );
  }

  public static List<String> mainReadQuotes(String[] args) throws IOException, URISyntaxException {
    String filename = args[0];
    LocalDate endDate = LocalDate.parse(args[1]);
    String token = getToken();

    List<PortfolioTrade> trades = readTradesFromJson(filename);
    RestTemplate restTemplate = new RestTemplate();

    List<TotalReturnsDto> returns = trades.stream().map(trade -> {
      if (trade.getPurchaseDate().isAfter(endDate)) {
        throw new RuntimeException("Purchase date cannot be after end date");
      }
      String url = prepareUrl(trade, endDate, token);
      TiingoCandle[] candles = restTemplate.getForObject(url, TiingoCandle[].class);
      if (candles != null && candles.length > 0) {
        Double closingPrice = candles[candles.length - 1].getClose();
        return new TotalReturnsDto(trade.getSymbol(), closingPrice);
      } else {
        return new TotalReturnsDto(trade.getSymbol(), 0.0);
      }
    }).collect(Collectors.toList());

    return returns.stream()
        .sorted(Comparator.comparing(TotalReturnsDto::getClosingPrice))
        .map(TotalReturnsDto::getSymbol)
        .collect(Collectors.toList());
  }

  public static List<PortfolioTrade> readTradesFromJson(String filename)
      throws IOException, URISyntaxException {
    ObjectMapper objectMapper = getObjectMapper();

    File file = resolveFileFromResources(filename);
    return objectMapper.readValue(file,
        new TypeReference<List<PortfolioTrade>>() {});
  }

  public static String prepareUrl(PortfolioTrade trade, LocalDate endDate, String token) {
    return "https://api.tiingo.com/tiingo/daily/" + trade.getSymbol() + "/prices?startDate=" +
        trade.getPurchaseDate() + "&endDate=" + endDate + "&token=" + token;
  }

    //////////////////////////////////////////////////////////////////////////////
    // MODULE 3 - Calculations
    //////////////////////////////////////////////////////////////////////////////

  // TODO: CRIO_TASK_MODULE_CALCULATIONS
  //  Now that you have the list of PortfolioTrade and their data, calculate annualized returns
  //  for the stocks provided in the Json.
  //  Use the function you just wrote #calculateAnnualizedReturns.
  //  Return the list of AnnualizedReturns sorted by annualizedReturns in descending order.

  // Note:
  // 1. You may need to copy relevant code from #mainReadQuotes to parse the Json.
  // 2. Remember to get the latest quotes from Tiingo API.

  public static List<Candle> fetchCandles(PortfolioTrade trade, LocalDate endDate, String token) {
    try {
      String url = prepareUrl(trade, endDate, token);
      RestTemplate restTemplate = new RestTemplate();
      TiingoCandle[] candles = restTemplate.getForObject(url, TiingoCandle[].class);
  
      // Sleep for a short duration to avoid rate limiting
      Thread.sleep(200); // 200 ms = 5 requests/sec max
  
      if (candles != null) {
        return Arrays.asList(candles);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Restore interrupted flag
    }
    return Collections.emptyList();
  }

  public static Double getOpeningPriceOnStartDate(List<Candle> candles) {
    return candles.get(0).getOpen();
  }

  public static Double getClosingPriceOnEndDate(List<Candle> candles) {
    return candles.get(candles.size() - 1).getClose();
  }

  public static List<AnnualizedReturn> mainCalculateSingleReturn(String[] args)
      throws IOException, URISyntaxException {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: <jsonFile> <yyyy-MM-dd>");
    }

    String filename = args[0];
    LocalDate endDate = LocalDate.parse(args[1]);
    String token = getToken();

    List<PortfolioTrade> trades = readTradesFromJson(filename);

    // Step 2: For each trade, fetch candle data and calculate returns
    return trades.stream()
      .map(trade -> {
        List<Candle> candles = fetchCandles(trade, endDate, token);
        if (candles.isEmpty()) {
          return new AnnualizedReturn(trade.getSymbol(), 0.0, 0.0);
        }
        Double buyPrice = getOpeningPriceOnStartDate(candles);
        Double sellPrice = getClosingPriceOnEndDate(candles);
        return calculateAnnualizedReturns(endDate, trade, buyPrice, sellPrice);
      })
      .sorted(Comparator.comparing(AnnualizedReturn::getAnnualizedReturn).reversed())
      .collect(Collectors.toList());
  }

  // TODO: CRIO_TASK_MODULE_CALCULATIONS
  //  Return the populated list of AnnualizedReturn for all stocks.
  //  Annualized returns should be calculated in two steps:
  //   1. Calculate totalReturn = (sell_value - buy_value) / buy_value.
  //      1.1 Store the same as totalReturns
  //   2. Calculate extrapolated annualized returns by scaling the same in years span.
  //      The formula is:
  //      annualized_returns = (1 + total_returns) ^ (1 / total_num_years) - 1
  //      2.1 Store the same as annualized_returns
  //  Test the same using below specified command. The build should be successful.
  //     ./gradlew test --tests PortfolioManagerApplicationTest.testCalculateAnnualizedReturn

  public static AnnualizedReturn calculateAnnualizedReturns(LocalDate endDate,
      PortfolioTrade trade, Double buyPrice, Double sellPrice) {
    double totalReturn = (sellPrice - buyPrice) / buyPrice;
    long daysBetween = ChronoUnit.DAYS.between(trade.getPurchaseDate(), endDate);
    double totalYears = daysBetween / 365.24;
    double annualizedReturn = Math.pow((1 + totalReturn), (1 / totalYears)) - 1;

    return new AnnualizedReturn(trade.getSymbol(), annualizedReturn, totalReturn);
  }
    //////////////////////////////////////////////////////////////////////////////
    // MODULE 4 - Refactor
    //////////////////////////////////////////////////////////////////////////////

  public static List<AnnualizedReturn> mainCalculateReturnsAfterRefactor(String[] args)
          throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: <jsonFile> <yyyy-MM-dd>");
    }

    String filename = args[0];
    LocalDate endDate = LocalDate.parse(args[1]);

    // Read trades from JSON
    List<PortfolioTrade> portfolioTrades = readTradesFromJson(filename);

    // Create RestTemplate
    RestTemplate restTemplate = new RestTemplate();

    // Get PortfolioManager from factory
    PortfolioManager portfolioManager = PortfolioManagerFactory.getPortfolioManager(restTemplate);

    // Calculate and return annualized returns
    return portfolioManager.calculateAnnualizedReturn(portfolioTrades, endDate);
  }
    //////////////////////////////////////////////////////////////////////////////
    // MAIN ENTRY POINT
    /////////////////////////////////////////////////////////////////////////////

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    ThreadContext.put("runId", UUID.randomUUID().toString());

//    printJsonObject(mainReadQuotes(args));
//    printJsonObject(mainCalculateSingleReturn(args));
    printJsonObject(mainCalculateReturnsAfterRefactor(args));
  }
}




