package io.github.ash123.assignmentelbit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.jetbrains.annotations.NonBlocking;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Async
@NonBlocking
@RestController
public class AssignmentElbitController
{
	private static final HttpClient client = HttpClient.newHttpClient();
	private static final ObjectMapper OBJECT_MAPPER = new JsonMapper()
			.enable(SerializationFeature.INDENT_OUTPUT)
			.registerModule(new JavaTimeModule());
	private static final ConcurrentHashMap<String, Set<String>> USER_TO_COUNTRIES_MAP = new ConcurrentHashMap<>();

	private CompletableFuture<Map<LocalDate, Integer>> getDatesHistoryByCountryAndStatus(String country, Status status)
	{
		return client.sendAsync(
						HttpRequest.newBuilder(URI.create("https://covid-api.mmediagroup.fr/v1/history/?country=" + country + "&status=" + status.name().toLowerCase()))
								.GET()
								.build(),
						HttpResponse.BodyHandlers.ofString()
				)
				.thenApplyAsync(HttpResponse::body)
				.thenApplyAsync(body ->
				{
					try
					{
						return OBJECT_MAPPER.readTree(body).path("All").path("dates");
					} catch (JsonProcessingException e)
					{
						e.printStackTrace();
						return MissingNode.getInstance();
					}
				})
				.thenApplyAsync(dates -> OBJECT_MAPPER.convertValue(dates, new TypeReference<>()
				{
				}));
//		response.join();
	}

	@GetMapping("/1/daily-new-confirmed-cases")
	private CompletableFuture<Integer> getDailyNewCases(
			@RequestParam
					String country,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate localDate)
	{
		return getDatesHistoryByCountryAndStatus(country, Status.DEATHS)
				.thenApplyAsync(dates -> dates.get(localDate) - dates.get(localDate.minusDays(1)));
	}

	@GetMapping("/2/register")
	public CompletableFuture<Void> register(@RequestParam String user)
	{
		return CompletableFuture.runAsync(() -> USER_TO_COUNTRIES_MAP.computeIfAbsent(user, key -> new HashSet<>()));
	}

	@GetMapping("/2/add-country")
	public CompletableFuture<Void> addCountry(@RequestParam String user, @RequestParam String country)
	{
		return CompletableFuture.runAsync(() -> USER_TO_COUNTRIES_MAP.compute(user, (key, set) ->
		{
			if (set == null)
				throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
			set.add(country);
			return set;
		}));
	}

	@GetMapping("/2/remove-country")
	public CompletableFuture<Void> removeCountry(@RequestParam String user, @RequestParam String country)
	{
		return CompletableFuture.runAsync(() -> USER_TO_COUNTRIES_MAP.compute(user, (key, countries) ->
		{
			if (countries == null)
				throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
			countries.remove(country);
			return countries;
		}));
	}

	@GetMapping("/2/get-countries")
	public CompletableFuture<Collection<String>> getCountries(@RequestParam String user)
	{
		return CompletableFuture.supplyAsync(() -> USER_TO_COUNTRIES_MAP.compute(user, (key, countries) ->
		{
			if (countries == null)
				throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
			return countries;
		}));
	}

	private CompletableFuture<Map<String, Map<LocalDate, Integer>>> getCasesPerCountryBetweenDatesByStatus(
			String user,
			LocalDate from,
			LocalDate to,
			Status status
	)
	{
		return CompletableFuture.supplyAsync(() ->
		{
			final Map<String, Map<LocalDate, Integer>> search = USER_TO_COUNTRIES_MAP.search(
					Runtime.getRuntime().availableProcessors(),
					(key, countries) -> key.equals(user) ?
							new ArrayList<>(countries).parallelStream()
									.collect(Collectors.toMap(
											Function.identity(),
											country -> getDatesHistoryByCountryAndStatus(country, status)
													.thenApplyAsync(dates -> from.datesUntil(to).parallel()
															.collect(Collectors.toMap(
																	Function.identity(),
																	localDate -> dates.get(localDate) - dates.get(localDate.minusDays(1))
															)))
													.join()
									)) :
							null
			);
			if (search == null)
				throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
			return search;
		});
	}

	@GetMapping("/2/get-deaths-per-country-between-dates")
	private CompletableFuture<Map<String, Map<LocalDate, Integer>>> getDeathsPerCountryBetweenDates(
			@RequestParam
					String user,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate from,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate to
	)
	{
		return getCasesPerCountryBetweenDatesByStatus(user, from, to, Status.DEATHS);
	}

	@GetMapping("/2/get-confirmed-per-country-between-dates")
	private CompletableFuture<Map<String, Map<LocalDate, Integer>>> getConfirmedPerCountryBetweenDates(
			@RequestParam
					String user,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate from,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate to
	)
	{
		return getCasesPerCountryBetweenDatesByStatus(user, from, to, Status.CONFIRMED);
	}

	private CompletableFuture<Map<LocalDate, CountryAndRatio>> getHighestDeathsPerCountryBetweenDatesRelativeToPopulationByStatus(
			String user,
			LocalDate from,
			LocalDate to,
			Status status
	)
	{
		return CompletableFuture.supplyAsync(() ->
		{
			final Map<LocalDate, CountryAndRatio> search = USER_TO_COUNTRIES_MAP.search(
					Runtime.getRuntime().availableProcessors(),
					(key, countries) -> key.equals(user) ?
							from.datesUntil(to).parallel()
									.collect(Collectors.toMap(
											Function.identity(),
											localDate -> countries.parallelStream()
													.map(country -> getDatesHistoryByCountryAndStatus(country, status)
															.thenCompose(dates -> client.sendAsync(
																			HttpRequest.newBuilder(URI.create("https://covid-api.mmediagroup.fr/v1/cases/?country=" + country))
																					.GET()
																					.build(),
																			HttpResponse.BodyHandlers.ofString()
																	)
																	.thenApplyAsync(HttpResponse::body)
																	.thenApplyAsync(body ->
																	{
																		try
																		{
																			return OBJECT_MAPPER.readTree(body)
																					.path("All")
																					.path("population");
																		} catch (JsonProcessingException e)
																		{
																			e.printStackTrace();
																			return MissingNode.getInstance();
																		}
																	})
																	.thenApplyAsync(JsonNode::intValue)
																	.thenApplyAsync(Number::doubleValue)
																	.thenApplyAsync(population -> new CountryAndRatio(
																			country,
																			(dates.get(localDate) - dates.get(localDate.minusDays(1))) / population
																	))))
													.map(CompletableFuture::join)
													.max(Comparator.comparingDouble(CountryAndRatio::ratio))
													.orElse(new CountryAndRatio("", Double.MAX_VALUE))
									)) :
							null
			);
			if (search == null)
				throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
			return search;
		});
	}

	@GetMapping("/2/get-highest-deaths-cases-relative-to-country")
	private CompletableFuture<Map<LocalDate, CountryAndRatio>> getHighestDeathsCasesRelativeToCountry(
			@RequestParam
					String user,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate from,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate to
	)
	{
		return getHighestDeathsPerCountryBetweenDatesRelativeToPopulationByStatus(user, from, to, Status.DEATHS);
	}

	@GetMapping("/2/get-highest-confirmed-cases-relative-to-country")
	private CompletableFuture<Map<LocalDate, CountryAndRatio>> getHighestConfirmedCasesRelativeToCountry(
			@RequestParam
					String user,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate from,
			@RequestParam
			@DateTimeFormat(pattern = "dd-MM-yyyy")
					LocalDate to
	)
	{
		return getHighestDeathsPerCountryBetweenDatesRelativeToPopulationByStatus(user, from, to, Status.CONFIRMED);
	}

	private enum Status
	{
		DEATHS, CONFIRMED
	}

	private record CountryAndRatio(String country, double ratio)
	{
	}
}