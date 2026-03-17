<?php

namespace LaravelSnowflakeApi\Services;

/**
 * A stdClass-like object that resolves property access case-insensitively.
 *
 * Snowflake's API returns column names in UPPERCASE (e.g., ORDER_NUMBER),
 * but Laravel controllers and Blade templates often use mixed-case property
 * names (e.g., Order_Number, order_number). This class stores values with
 * their original UPPERCASE keys and resolves any casing on read.
 *
 * Supports:
 *   $row->ORDER_NUMBER   (original)
 *   $row->Order_Number   (mixed)
 *   $row->order_number   (lower)
 *
 * Also works with isset(), property_exists() patterns, and json_encode().
 */
class CaseInsensitiveRow implements \JsonSerializable, \ArrayAccess
{
    /** @var array<string, mixed> Original UPPERCASE key → value */
    private array $data = [];

    /** @var array<string, string> lowercase key → original UPPERCASE key */
    private array $keyMap = [];

    public function __construct(array $row)
    {
        foreach ($row as $key => $value) {
            $this->data[$key] = $value;
            $this->keyMap[strtolower($key)] = $key;
        }
    }

    public function __get(string $name): mixed
    {
        // Try exact match first
        if (array_key_exists($name, $this->data)) {
            return $this->data[$name];
        }

        // Case-insensitive lookup
        $lower = strtolower($name);
        if (isset($this->keyMap[$lower])) {
            return $this->data[$this->keyMap[$lower]];
        }

        return null;
    }

    public function __set(string $name, mixed $value): void
    {
        $this->data[$name] = $value;
        $this->keyMap[strtolower($name)] = $name;
    }

    public function __isset(string $name): bool
    {
        if (array_key_exists($name, $this->data)) {
            return true;
        }

        return isset($this->keyMap[strtolower($name)]);
    }

    public function __unset(string $name): void
    {
        $lower = strtolower($name);
        $original = $this->keyMap[$lower] ?? $name;
        unset($this->data[$original], $this->keyMap[$lower]);
    }

    public function jsonSerialize(): array
    {
        return $this->data;
    }

    /** Allow array-style access for backwards compatibility */
    public function toArray(): array
    {
        return $this->data;
    }

    // ArrayAccess implementation (case-insensitive)

    public function offsetExists(mixed $offset): bool
    {
        return $this->__isset((string) $offset);
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->__get((string) $offset);
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        $this->__set((string) $offset, $value);
    }

    public function offsetUnset(mixed $offset): void
    {
        $this->__unset((string) $offset);
    }
}
