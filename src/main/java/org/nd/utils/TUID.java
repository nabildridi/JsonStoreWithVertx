package org.nd.utils;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * A ‘tuid’ is a chronologically sortable unique identifier with an embedded
 * nanosecond-resolution createdAt timestamp. It is a BigInteger, expressed as a
 * case-sensitive base-62 string (digits 0-9, characters A-Z, and characters
 * a-z). The left bits are the embedded nanoseconds since epoch, and the right
 * 32 bits are entropy (a SecureRandom integer). The IDs are currently 16
 * characters long. Example: 91Mq07yx9IxHCi5Y with UTC timestamp
 * 2021-03-08T05:54:09.208207Z
 */
public class TUID implements Comparable<TUID> {
	public static final ZoneId TIME_ZONE = ZoneId.of("US/Pacific");
	public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("E MM-dd-yyyy hh:mm:ss v");
	private static final BigInteger BASE = BigInteger.valueOf(62);
	private static final String DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	private static final Pattern REGEX = Pattern.compile("[" + DIGITS + "]*");
	private static final SecureRandom RANDOM = new SecureRandom();
	private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1000000000L); // 1 billion
	private static final Instant MIN_TIMESTAMP = ZonedDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")).toInstant();
	public static final String MIN_ID = firstId(MIN_TIMESTAMP); // 5Hr02eJHAfTt1tTM 2000-01-01T00:00:00Z
	private static final Instant MAX_TIMESTAMP = ZonedDateTime.of(2100, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")).toInstant();
	public static final String MAX_ID = firstId(MAX_TIMESTAMP); // MuklDY5bgW1s9Ev2 2100-01-01T00:00:00Z

	private final String id;
	private final Instant createdAt;
	private final BigInteger value;

	/**
	 * Create a new TUID, with the current time as the createdAt timestamp.
	 */
	public TUID() {
		this.createdAt = Instant.now();
		BigInteger nanoSeconds = BigInteger.valueOf(createdAt.getEpochSecond()).multiply(NANOS_PER_SECOND)
				.add(BigInteger.valueOf(createdAt.getNano()));
		BigInteger entropy = BigInteger.valueOf(RANDOM.nextInt()).abs();
		this.value = nanoSeconds.shiftLeft(32).or(entropy);
		this.id = encode(value);
	}

	/**
	 * Create a new TUID, with the given createdAt timestamp.
	 *
	 * @param createdAt the createdAt timestamp
	 */
	public TUID(Instant createdAt) {
		this.createdAt = createdAt;
		BigInteger nanoSeconds = BigInteger.valueOf(createdAt.getEpochSecond()).multiply(NANOS_PER_SECOND)
				.add(BigInteger.valueOf(createdAt.getNano()));
		BigInteger entropy = BigInteger.valueOf(RANDOM.nextInt()).abs();
		this.value = nanoSeconds.shiftLeft(32).or(entropy);
		this.id = encode(value);
	}

	/**
	 * Create a new TUID from the provided BigInteger value.
	 *
	 * @param value TUID encoded as a BigInteger
	 */
	public TUID(BigInteger value) {
		this.value = value;
		this.id = encode(value);
		BigInteger nanoSeconds = value.shiftRight(32);
		BigInteger[] secNano = nanoSeconds.divideAndRemainder(NANOS_PER_SECOND);
		this.createdAt = Instant.ofEpochSecond(secNano[0].longValue(), secNano[1].longValue());
	}

	/**
	 * Create a new TUID, parsed from the provided ID.
	 *
	 * @param id Base-62 encoded TUID
	 */
	public TUID(String id) {
		this.id = id;
		this.value = decode(id);
		BigInteger nanoSeconds = value.shiftRight(32);
		BigInteger[] secNano = nanoSeconds.divideAndRemainder(NANOS_PER_SECOND);
		this.createdAt = Instant.ofEpochSecond(secNano[0].longValue(), secNano[1].longValue());
	}

	/**
	 * Parse a TUID from the provided ID.
	 *
	 * @param id Base-62 encoded TUID
	 * @return TUID
	 */
	public static TUID fromString(String id) {
		return new TUID(id);
	}

	/**
	 * Create a new TUID with the specified timestamp and random entropy. This is a
	 * convenience method for the corresponding constructor.
	 *
	 * @param timestamp embedded createdAt timestamp
	 * @return TUID
	 */
	public static TUID withTimestamp(Instant timestamp) {
		return new TUID(timestamp);
	}

	/**
	 * Calculate the nanosecond-resolution duration between the two TUIDs. This is a
	 * convenience method for working with embedded timestamps.
	 *
	 * @param startId starting TUID
	 * @param stopId  ending TUID
	 * @return Duration
	 */
	public static Duration duration(String startId, String stopId) {
		return Duration.between(createdAt(startId), createdAt(stopId));
	}

	/**
	 * Get the createdAt timestamp of from a String representation of a TUID. This
	 * is a convenience method for extracting the createdAt timestamp from an ID.
	 *
	 * @param id Base-62 encoded TUID
	 * @return createdAt timestamp
	 */
	public static Instant createdAt(String id) {
		return new TUID(id).createdAt;
	}

	/**
	 * Supply a new TUID with the current time as the createdAt timestamp.
	 *
	 * @return a new TUID
	 */
	public static String createId() {
		return new TUID().id;
	}

	/**
	 * Create the first (in chronological sort order) ID at the provided timestamp.
	 * The entropy portion of the ID will be zeros. These values are useful as
	 * boundaries for paginating data chronologically, by TUIDs.
	 *
	 * @param instant Instant
	 * @return First TUID at the provided timestamp
	 */
	public static String firstId(Instant instant) {
		BigInteger nanoSeconds = BigInteger.valueOf(instant.getEpochSecond()).multiply(NANOS_PER_SECOND)
				.add(BigInteger.valueOf(instant.getNano()));
		BigInteger value = nanoSeconds.shiftLeft(32);
		return new TUID(value).id;
	}

	/**
	 * Validate that the supplied ID follows the correct pattern for a TUID, and
	 * that it was generated in the timespan deemed "valid". It does not validate an
	 * actual ID currently in use with an entity.
	 *
	 * @param id possible TUID
	 * @return true if the ID follows the correct pattern and possible time range
	 */
	public static Boolean isValidId(String id) {
		try {
			return id != null && !id.isBlank() && REGEX.matcher(id).matches() && MIN_ID.length() <= id.length()
					&& MAX_ID.length() >= id.length() && MIN_ID.compareTo(id) <= 0 && MAX_ID.compareTo(id) >= 0;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Encode a BigInteger value as a Base-62 String
	 *
	 * @param value BigInteger value
	 * @return Base-62 String
	 */
	private static String encode(BigInteger value) {
		if (value == null || BigInteger.ZERO.compareTo(value) > 0)
			throw new IllegalArgumentException("Base62 TUID encoding error: positive BigInteger value required");
		StringBuilder result = new StringBuilder();
		while (BigInteger.ZERO.compareTo(value) < 0) {
			BigInteger[] divmod = value.divideAndRemainder(BASE);
			result.insert(0, DIGITS.charAt(divmod[1].intValue()));
			value = divmod[0];
		}
		return result.length() == 0 ? DIGITS.substring(0, 1) : result.toString();
	}

	/**
	 * Decode a Base-62 String into a BigInteger value
	 *
	 * @param text Base-62 String
	 * @return BigInteger value
	 */
	private static BigInteger decode(final String text) {
		if (text == null || text.isEmpty())
			return BigInteger.ZERO;
		if (!REGEX.matcher(text).matches())
			throw new IllegalArgumentException("Base62 TUID decoding error: invalid character(s) in " + text);
		return IntStream
				.range(0, text.length()).mapToObj(index -> BigInteger
						.valueOf(DIGITS.indexOf(text.charAt(text.length() - 1 - index))).multiply(BASE.pow(index)))
				.reduce(BigInteger.ZERO, BigInteger::add);
	}

	/**
	 * Convert the provided ID to a BigInteger, including both timestamp and
	 * entropy. This is a convenience method for diagnostic or debugging purposes.
	 *
	 * @return BigInteger representation of the ID
	 */
	public BigInteger bigInteger() {
		return value;
	}

	/**
	 * Identify the number of bits used by the provided TUID. This is a convenience
	 * method for diagnostic or debugging purposes.
	 *
	 * @return number of bits used by the ID
	 */
	public Integer bitLength() {
		return value.bitLength();
	}

	/**
	 * Convert the provided TUID from Base-62 to a Base 2 representation. This is a
	 * convenience method for diagnostic or debugging purposes.
	 *
	 * @return Base 2 representation of the ID
	 */
	public String bits() {
		return value.toString(2);
	}

	/**
	 * Extract the entropy portion of the ID (the right-most 32 bits). This is a
	 * convenience method for diagnostic or debugging purposes.
	 *
	 * @return TUID entropy
	 */
	public BigInteger entropy() {
		BigInteger mask = BigInteger.ONE.shiftLeft(32).subtract(BigInteger.ONE);
		return value.and(mask);
	}

	/**
	 * Extract the embedded nanosecond-resolution timestamp in the UTC time zone
	 * from the provided ID. This is a convenience method for working with embedded
	 * timestamps.
	 *
	 * @return Zoned Date/Time (UTC)
	 */
	public ZonedDateTime timestamp() {
		return timestamp(ZoneId.of("UTC"));
	}

	/**
	 * Extract the embedded nanosecond-resolution timestamp in the preferred time
	 * zone from the provided ID. This is a convenience method for working with
	 * embedded timestamps.
	 *
	 * @param timeZone preferred time zone
	 * @return Zoned Date/Time
	 */
	public ZonedDateTime timestamp(ZoneId timeZone) {
		return ZonedDateTime.ofInstant(createdAt, timeZone);
	}

	/**
	 * Extract the embedded nanosecond-resolution timestamp in the preferred time
	 * zone from the provided ID. This is a convenience method for working with
	 * embedded timestamps.
	 *
	 * @param timeZone  preferred time zone
	 * @param formatter preferred date/time formatter
	 * @return formatted date/time
	 */
	public String timestamp(ZoneId timeZone, DateTimeFormatter formatter) {
		return timestamp(timeZone).format(formatter);
	}

	/**
	 * Extract the embedded timestamp from the provided ID as a UTC LocalDate. This
	 * is a convenience method for working with embedded timestamps.
	 *
	 * @return LocalDate
	 */
	public LocalDate localDate() {
		return timestamp().toLocalDate();
	}

	/**
	 * Extract the embedded timestamp from the provided ID as a LocalDate in the
	 * preferred time zone. This is a convenience method for working with embedded
	 * timestamps.
	 *
	 * @param timeZone preferred time zone
	 * @return LocalDate
	 */
	public LocalDate localDate(ZoneId timeZone) {
		return timestamp(timeZone).toLocalDate();
	}

	/**
	 * Calculate the nanosecond-resolution duration between the two TUIDs. This is a
	 * convenience method for working with embedded timestamps.
	 *
	 * @param startId TUID to compare
	 * @return Duration
	 */
	public Duration durationSince(TUID startId) {
		return Duration.between(startId.createdAt, this.createdAt);
	}

	/**
	 * Calculate the nanosecond-resolution duration between the two TUIDs. This is a
	 * convenience method for working with embedded timestamps.
	 *
	 * @param stopId TUID to compare
	 * @return Duration
	 */
	public Duration duration(TUID stopId) {
		return Duration.between(this.createdAt, stopId.createdAt);
	}

	/**
	 * Calculate the millisecond-resolution duration between two TUIDs, expressed in
	 * a formatted string. This is a convenience method for working with embedded
	 * timestamps.
	 *
	 * @param stopId TUID to compare
	 * @return formatted duration string
	 */
	public String durationString(TUID stopId) {
		Duration duration = this.duration(stopId);
		long days = duration.toDaysPart();
		String dayString = days > 0 ? days + " d " : "";
		return String.format("%s%02d:%02d:%02d.%03d", dayString, duration.toHoursPart(), duration.toMinutesPart(),
				duration.toSecondsPart(), duration.toMillisPart());
	}

	public String getId() {
		return id;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public BigInteger getValue() {
		return value;
	}

	@Override
	public String toString() {
		return id;
	}

	@Override
	public boolean equals(Object that) {
		if (this == that)
			return true;
		if (that == null || this.getClass() != that.getClass())
			return false;
		TUID tuid = (TUID) that;
		return Objects.equals(id, tuid.id);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public int compareTo(TUID that) {
		return this.id.compareTo(that.id);
	}
}
