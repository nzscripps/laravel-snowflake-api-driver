<?php

namespace LaravelSnowflakeApi\Services;

/**
 * A Snowflake result row: a stdClass whose public properties are the columns
 * exactly as Snowflake names them, readable in any other casing as well.
 *
 * Snowflake folds unquoted aliases to UPPERCASE (`AS Order_Number` comes back
 * as ORDER_NUMBER), while Laravel controllers and Blade templates often name a
 * column the way the SQL text spells it. The columns are real properties, so
 * everything that works on a PDO/stdClass row works here unchanged:
 *
 *   (array) $row, get_object_vars($row), foreach ($row as $column => $value),
 *   property_exists($row, 'ORDER_NUMBER'), $row instanceof \stdClass,
 *   json_encode($row), and Laravel's Builder::value()
 *
 * Any other casing resolves through the magic accessors and ArrayAccess:
 *
 *   $row->Order_Number, $row->order_number, isset($row->order_number),
 *   $row['Order_Number']
 *
 * isset() follows stdClass semantics in every casing: a selected column that
 * holds NULL is not set. To ask whether a column was selected, use
 * property_exists() with the column's own name, or array_key_exists() on
 * toArray().
 *
 * Writing a casing that matches no property adds a property with that exact
 * name; it does not overwrite the folded original, so both serialize. Reads in
 * any other casing resolve to the most recently added of them, or to the one
 * most recently written through ArrayAccess. A direct write to an existing
 * property updates it in place without moving those reads (0.12 moved them;
 * a public property write never reaches PHP magic).
 *
 * Before 0.13 the columns lived in a private array: casts,
 * get_object_vars() and property_exists() saw nothing, and isset() reported
 * a selected NULL column as set.
 */
class CaseInsensitiveRow extends \stdClass implements \ArrayAccess, \JsonSerializable
{
    /**
     * Per row: lowercased property name => property name, built on the first
     * read in a casing that is not a property. Held outside the instance so
     * that casts and get_object_vars() see only columns.
     *
     * @var \WeakMap<self, array<string, string>>|null
     */
    private static ?\WeakMap $propertyNames = null;

    /**
     * @param  array<string, mixed>  $row  Column name => value
     */
    public function __construct(array $row = [])
    {
        foreach ($row as $column => $value) {
            $this->{$column} = $value;
        }
    }

    /**
     * Reached only when $name is not a property: resolve its casing.
     */
    public function __get(string $name): mixed
    {
        $property = $this->propertyFor($name);

        return $property === null ? null : $this->{$property};
    }

    /**
     * Reached only when $name is not a property: add it with this exact name.
     */
    public function __set(string $name, mixed $value): void
    {
        // Inside __set, PHP writes $name directly instead of recursing.
        $this->{$name} = $value;
        $this->resolveCasingTo($name);
    }

    /**
     * Reached only when $name is not a property.
     */
    public function __isset(string $name): bool
    {
        $property = $this->propertyFor($name);

        return $property !== null && $this->{$property} !== null;
    }

    /**
     * Reached only when $name is not a property.
     */
    public function __unset(string $name): void
    {
        $property = $this->propertyFor($name);

        if ($property !== null) {
            unset($this->{$property});
            self::$propertyNames?->offsetUnset($this);
        }
    }

    /**
     * @return array<string, mixed>
     */
    public function jsonSerialize(): array
    {
        return get_object_vars($this);
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return get_object_vars($this);
    }

    /**
     * The 0.12 envelope, byte for byte: private `data` (columns) and `keyMap`
     * (lowercased name => the casing reads resolve to). A process still on
     * 0.12, during a rolling deploy or after a rollback, has no __unserialize()
     * and maps exactly these keys onto its private arrays.
     *
     * @return array<string, array<string, mixed>>
     */
    public function __serialize(): array
    {
        return [
            "\0".self::class."\0data" => get_object_vars($this),
            "\0".self::class."\0keyMap" => $this->names(),
        ];
    }

    /**
     * Reads the 0.12 envelope (written by this version or by 0.12 itself) and,
     * defensively, a bare column array.
     *
     * @param  array<string, mixed>  $data
     */
    public function __unserialize(array $data): void
    {
        $columns = $data["\0".self::class."\0data"] ?? null;
        $keyMap = $data["\0".self::class."\0keyMap"] ?? null;

        if (! is_array($columns)) {
            $columns = $data;
            $keyMap = null;
        }

        foreach ($columns as $column => $value) {
            $this->{$column} = $value;
        }

        if (is_array($keyMap)) {
            $names = $this->names();

            foreach ($keyMap as $lower => $property) {
                if (is_string($property) && property_exists($this, $property)) {
                    $names[(string) $lower] = $property;
                }
            }

            self::$propertyNames[$this] = $names;
        }
    }

    // ArrayAccess: case-insensitive, with the same semantics as property access.

    public function offsetExists(mixed $offset): bool
    {
        return isset($this->{(string) $offset});
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->{(string) $offset};
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        $name = (string) $offset;
        $this->{$name} = $value;
        // A write to an existing property skips __set; ArrayAccess can still
        // send other casings to it, as 0.12 did for every write.
        $this->resolveCasingTo($name);
    }

    public function offsetUnset(mixed $offset): void
    {
        unset($this->{(string) $offset});
    }

    /**
     * The property a casing of $name refers to, or null when there is none.
     */
    private function propertyFor(string $name): ?string
    {
        $lower = strtolower($name);
        $names = $this->names();

        // A property removed by an exact-name unset() bypasses __unset, so a
        // hit is re-checked before it is trusted.
        if (isset($names[$lower]) && ! property_exists($this, $names[$lower])) {
            self::$propertyNames?->offsetUnset($this);
            $names = $this->names();
        }

        return $names[$lower] ?? null;
    }

    /**
     * Lowercased name => property name; built from the properties in order, so
     * of two casings of one name the later one wins.
     *
     * @return array<string, string>
     */
    private function names(): array
    {
        self::$propertyNames ??= new \WeakMap;
        $names = self::$propertyNames[$this] ?? null;

        if ($names === null) {
            $names = [];

            foreach (array_keys(get_object_vars($this)) as $property) {
                $names[strtolower((string) $property)] = (string) $property;
            }

            self::$propertyNames[$this] = $names;
        }

        return $names;
    }

    /**
     * Point reads in every casing of $name at the property $name, if the map
     * has been built (otherwise it is built later from the properties).
     */
    private function resolveCasingTo(string $name): void
    {
        if (self::$propertyNames !== null && isset(self::$propertyNames[$this])) {
            $names = self::$propertyNames[$this];
            $names[strtolower($name)] = $name;
            self::$propertyNames[$this] = $names;
        }
    }
}
