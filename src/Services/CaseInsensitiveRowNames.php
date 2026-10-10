<?php

namespace LaravelSnowflakeApi\Services;

/**
 * Casing lookup for CaseInsensitiveRow, kept outside the row class so that the
 * row declares no members: any column name is then a plain public property,
 * never one PHP routes through the row's magic because a declared member
 * shares its name.
 *
 * @internal
 */
final class CaseInsensitiveRowNames
{
    /**
     * Per row: lowercased name => the last property of that name in property
     * order. A cache of that derivation, dropped whenever it may be stale.
     *
     * @var \WeakMap<object, array<string, string>>|null
     */
    private static ?\WeakMap $byRow = null;

    /**
     * Every row of one result has the same properties: the last map built is
     * kept with its property list, and a row with the same list shares it
     * (copy-on-write) instead of building its own.
     *
     * @var array{0: list<int|string>, 1: array<string, string>}|null
     */
    private static ?array $last = null;

    /**
     * @return array<string, string>
     */
    public static function of(object $row): array
    {
        self::$byRow ??= new \WeakMap;
        $names = self::$byRow[$row] ?? null;

        if ($names === null) {
            // Called from outside the row, so only its public properties.
            $properties = array_keys(get_object_vars($row));

            if (self::$last !== null && self::$last[0] === $properties) {
                $names = self::$last[1];
            } else {
                $names = [];

                foreach ($properties as $property) {
                    $names[strtolower((string) $property)] = (string) $property;
                }

                self::$last = [$properties, $names];
            }

            self::$byRow[$row] = $names;
        }

        return $names;
    }

    /**
     * Record that $name, just appended to $row, now answers its casings.
     */
    public static function appended(object $row, string $name): void
    {
        if (self::$byRow !== null && isset(self::$byRow[$row])) {
            $names = self::$byRow[$row];
            $names[strtolower($name)] = $name;
            self::$byRow[$row] = $names;
        }
    }

    public static function forget(object $row): void
    {
        self::$byRow?->offsetUnset($row);
    }
}
