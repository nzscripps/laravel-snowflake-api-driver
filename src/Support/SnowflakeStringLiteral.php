<?php

namespace LaravelSnowflakeApi\Support;

/**
 * A string value written into Snowflake SQL text as a string constant.
 *
 * Backslash is an escape character inside Snowflake single-quoted string
 * constants, so it is doubled before the single quote is. Doubling only the
 * quote lets a value ending in a backslash escape its own closing quote.
 */
final class SnowflakeStringLiteral
{
    public static function quote(string $value): string
    {
        return "'".str_replace(['\\', "'"], ['\\\\', "''"], $value)."'";
    }
}
