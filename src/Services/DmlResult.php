<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

use LaravelSnowflakeApi\Exceptions\StatementOutcomeUnknownException;

/**
 * Row counts changed by a single DML statement (INSERT/UPDATE/DELETE/MERGE),
 * read from the Snowflake SQL API response — never guessed.
 *
 * @see https://docs.snowflake.com/en/developer-guide/sql-api/reference (ResultSet_stats)
 */
final class DmlResult
{
    /**
     * @var int
     */
    private $inserted;

    /**
     * @var int
     */
    private $updated;

    /**
     * @var int
     */
    private $deleted;

    public function __construct(int $inserted, int $updated, int $deleted)
    {
        $this->inserted = $inserted;
        $this->updated = $updated;
        $this->deleted = $deleted;
    }

    public function getInserted(): int
    {
        return $this->inserted;
    }

    public function getUpdated(): int
    {
        return $this->updated;
    }

    public function getDeleted(): int
    {
        return $this->deleted;
    }

    /**
     * Total rows changed by the statement, across all three operations.
     */
    public function affectedRows(): int
    {
        return $this->inserted + $this->updated + $this->deleted;
    }

    /**
     * Build from an executed Result. Prefers the response's `stats` object;
     * falls back to the statement's own output row; throws when neither
     * carries a verifiable count rather than guessing.
     *
     * @throws StatementOutcomeUnknownException When no verifiable count is present
     */
    public static function fromResult(Result $result): self
    {
        $stats = $result->getStats();

        if (is_array($stats) && self::hasAnyStatsKey($stats)) {
            return new self(
                (int) ($stats['numRowsInserted'] ?? 0),
                (int) ($stats['numRowsUpdated'] ?? 0),
                (int) ($stats['numRowsDeleted'] ?? 0)
            );
        }

        $row = self::recognizedOutputRow($result);

        if ($row !== null) {
            return new self(
                (int) ($row['number of rows inserted'] ?? 0),
                (int) ($row['number of rows updated'] ?? 0),
                (int) ($row['number of rows deleted'] ?? 0)
            );
        }

        throw StatementOutcomeUnknownException::countsUnreadable((string) $result->getId());
    }

    private static function hasAnyStatsKey(array $stats): bool
    {
        return array_key_exists('numRowsInserted', $stats)
            || array_key_exists('numRowsUpdated', $stats)
            || array_key_exists('numRowsDeleted', $stats);
    }

    /**
     * Read the statement's own output row, matching Snowflake's documented
     * column names case-insensitively. Snowflake's MERGE/UPDATE/DELETE/INSERT
     * responses carry one row with columns named "number of rows inserted",
     * "number of rows updated", "number of rows deleted" (any subset); values
     * arrive as strings and Result::toArray() already numeric-converts them.
     *
     * @return array<string, mixed>|null Row keyed by lower-cased column name, or null if unrecognized
     */
    private static function recognizedOutputRow(Result $result): ?array
    {
        $rows = $result->toArray();
        $row = $rows[0] ?? null;

        if (! is_array($row)) {
            return null;
        }

        $normalized = [];
        foreach ($row as $column => $value) {
            $normalized[strtolower((string) $column)] = $value;
        }

        $recognizedColumns = [
            'number of rows inserted',
            'number of rows updated',
            'number of rows deleted',
        ];

        foreach ($recognizedColumns as $column) {
            if (array_key_exists($column, $normalized)) {
                return $normalized;
            }
        }

        return null;
    }
}
