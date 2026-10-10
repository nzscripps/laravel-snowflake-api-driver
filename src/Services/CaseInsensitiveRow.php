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
 * name; it does not overwrite the folded original, so both serialize. When
 * several casings of one name exist, the LAST in property order answers every
 * other casing. A new casing is added last, and an ArrayAccess write to an
 * existing casing moves it last (as 0.12 sent other casings to the latest
 * write). A direct property write to an existing casing updates it in place
 * and moves nothing: PHP never routes a public property write through magic.
 *
 * Before 0.13 the columns lived in a private array: casts,
 * get_object_vars() and property_exists() saw nothing, and isset() reported
 * a selected NULL column as set.
 */
class CaseInsensitiveRow extends \stdClass implements \ArrayAccess, \JsonSerializable
{
    /**
     * Per row: lowercased name => the property that answers it. A cache
     * derived from property order, rebuilt whenever it may be stale. Held
     * outside the instance so that casts and get_object_vars() see only
     * columns.
     *
     * @var \WeakMap<self, array<string, string>>|null
     */
    private static ?\WeakMap $propertyNames = null;

    /**
     * Every row of one result has the same properties: the last map built is
     * kept with its property list, and a row with the same list shares it
     * (copy-on-write) instead of building its own.
     *
     * @var array{0: list<int|string>, 1: array<string, string>}|null
     */
    private static ?array $lastPropertyNames = null;

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
     * Reached only when $name is not a property: add it, last, with this
     * exact name.
     */
    public function __set(string $name, mixed $value): void
    {
        // Inside __set, PHP writes $name directly instead of recursing.
        $this->{$name} = $value;

        if (self::$propertyNames !== null && isset(self::$propertyNames[$this])) {
            $names = self::$propertyNames[$this];
            $names[strtolower($name)] = $name;
            self::$propertyNames[$this] = $names;
        }
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
            $this->forgetPropertyNames();
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
     * and maps exactly these keys onto its private arrays, so every keyMap
     * target must be a live column.
     *
     * @return array<string, array<string, mixed>>
     */
    public function __serialize(): array
    {
        $this->forgetPropertyNames();

        return [
            "\0".self::class."\0data" => get_object_vars($this),
            "\0".self::class."\0keyMap" => $this->propertyNames(),
        ];
    }

    /**
     * Reads the 0.12 envelope (written by this version or by 0.12 itself) and,
     * defensively, a bare column array. A 0.12 keyMap is honoured by moving
     * each casing it names last.
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

        foreach (is_array($keyMap) ? $keyMap : [] as $property) {
            if (is_string($property) && property_exists($this, $property)) {
                $this->moveLast($property);
            }
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

        if (property_exists($this, $name)) {
            $this->moveLast($name);
        }

        $this->{$name} = $value;
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
        $property = $this->propertyNames()[$lower] ?? null;

        // An exact-name unset() bypasses __unset; rebuild rather than trust a
        // hit on a removed property.
        if ($property !== null && ! property_exists($this, $property)) {
            $this->forgetPropertyNames();
            $property = $this->propertyNames()[$lower] ?? null;
        }

        return $property;
    }

    /**
     * Lowercased name => the last property of that name in property order.
     *
     * @return array<string, string>
     */
    private function propertyNames(): array
    {
        self::$propertyNames ??= new \WeakMap;
        $names = self::$propertyNames[$this] ?? null;

        if ($names === null) {
            $properties = array_keys(get_object_vars($this));

            if (self::$lastPropertyNames !== null && self::$lastPropertyNames[0] === $properties) {
                $names = self::$lastPropertyNames[1];
            } else {
                $names = [];

                foreach ($properties as $property) {
                    $names[strtolower((string) $property)] = (string) $property;
                }

                self::$lastPropertyNames = [$properties, $names];
            }

            self::$propertyNames[$this] = $names;
        }

        return $names;
    }

    private function forgetPropertyNames(): void
    {
        self::$propertyNames?->offsetUnset($this);
    }

    /**
     * Re-add an existing property so that it answers every other casing of
     * its name. Without a twin it already does, and nothing moves.
     */
    private function moveLast(string $property): void
    {
        if ($this->propertyFor($property) === $property) {
            return;
        }

        $value = $this->{$property};
        unset($this->{$property});
        $this->{$property} = $value;
    }
}
