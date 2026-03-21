<?php

namespace LaravelSnowflakeApi\Flavours\Snowflake;

use Illuminate\Database\Query\Builder;
use LaravelSnowflakeApi\Services\CaseInsensitiveRow;

/**
 * Custom query builder for Snowflake connections.
 *
 * Overrides exists() and aggregate() because Laravel's base Builder uses
 * `(array) $results[0]` to read result columns. PHP's (array) cast on
 * objects with private properties produces mangled keys, so CaseInsensitiveRow
 * (which stores data in a private $data array) doesn't survive the cast.
 *
 * CaseInsensitiveRow implements ArrayAccess, so $row['column'] works correctly
 * for case-insensitive access. These overrides use ArrayAccess instead of (array).
 */
class SnowflakeQueryBuilder extends Builder
{
    /**
     * Determine if any rows exist for the current query.
     *
     * Uses ArrayAccess on CaseInsensitiveRow instead of (array) cast.
     */
    public function exists()
    {
        $this->applyBeforeQueryCallbacks();

        $results = $this->connection->select(
            $this->grammar->compileExists($this), $this->getBindings(), ! $this->useWritePdo
        );

        if (isset($results[0])) {
            $row = $results[0];

            // CaseInsensitiveRow implements ArrayAccess for case-insensitive lookup.
            // Laravel's base Builder uses (array) which breaks on private properties.
            if ($row instanceof CaseInsensitiveRow) {
                return (bool) ($row['exists'] ?? $row['row_exists'] ?? false);
            }

            return (bool) ((array) $row)['exists'];
        }

        return false;
    }

    /**
     * Execute an aggregate function on the database.
     *
     * Uses ArrayAccess on CaseInsensitiveRow instead of (array) cast.
     */
    public function aggregate($function, $columns = ['*'])
    {
        $results = $this->cloneWithout($this->unions || $this->havings ? [] : ['columns'])
            ->cloneWithoutBindings($this->unions || $this->havings ? [] : ['select'])
            ->setAggregate($function, $columns)
            ->get($columns);

        if (! $results->isEmpty()) {
            $row = $results[0];

            // CaseInsensitiveRow implements ArrayAccess for case-insensitive lookup.
            if ($row instanceof CaseInsensitiveRow) {
                return $row['aggregate'];
            }

            return array_change_key_case((array) $row)['aggregate'];
        }
    }
}
