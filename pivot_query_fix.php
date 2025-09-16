<?php

namespace Tests\SnowflakeDriverFix;

/**
 * This script demonstrates the issue with Snowflake PIVOT column names and
 * a potential fix for the QueryGrammar class to better handle quoted identifiers.
 */

// The current implementation in LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar
class QueryGrammar
{
    /**
     * Current implementation that strips all double quotes
     */
    public function wrapColumnCurrent($column)
    {
        if ($column !== '*') {
            return str_replace('"', '', $column);
        }

        return $column;
    }

    /**
     * Fixed implementation that preserves nested quotes
     */
    public function wrapColumnFixed($column)
    {
        if ($column !== '*') {
            // If we have a column name with nested quotes like "'column_name'"
            // preserve the inner quotes while removing the outer ones
            if (substr($column, 0, 1) === '"' && substr($column, -1) === '"') {
                return substr($column, 1, -1);
            }

            // For normal column names, just strip all quotes
            return str_replace('"', '', $column);
        }

        return $column;
    }
}

// Example usage with PIVOT column names
$grammar = new QueryGrammar;

// Test cases
$tests = [
    // Regular column name
    'regular_column',

    // Double-quoted regular column
    '"regular_column"',

    // Column name with single quotes inside (PIVOT generated)
    "\"'column_with_quotes'\"",

    // Real-world example from the PIVOT query
    "\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\"",
];

echo "Current Implementation vs Fixed Implementation:\n\n";
echo "-----------------------------------------------------\n";
echo sprintf("%-30s | %-25s | %-25s\n", 'Original Column Name', 'Current Behavior', 'Fixed Behavior');
echo "-----------------------------------------------------\n";

foreach ($tests as $column) {
    $currentResult = $grammar->wrapColumnCurrent($column);
    $fixedResult = $grammar->wrapColumnFixed($column);

    echo sprintf("%-30s | %-25s | %-25s\n",
        $column,
        $currentResult,
        $fixedResult
    );
}

echo "\n";
echo "THE PROBLEM:\n";
echo "When Snowflake creates a PIVOT, it generates column names that include single quotes:\n";
echo "PIVOT(SUM(value) FOR category IN ('value1','value2')) → creates columns named 'value1', 'value2'\n\n";

echo "THE SOLUTION:\n";
echo "1. Current approach in the driver removes all double quotes, which works fine for simple columns.\n";
echo "2. But when dealing with PIVOT columns, we need to preserve the inner quotes.\n";
echo "3. The fix is to modify wrapColumn() to check if we have an identifier with nested quotes.\n\n";

echo "IMPLEMENTATION:\n";
echo "To fix this issue, you would need to modify the wrapColumn method in:\n";
echo "- src/Flavours/Snowflake/Grammars/QueryGrammar.php\n\n";

echo "Replace:\n";
echo "  if ('*' !== \$column) { return str_replace('\"', '', \$column); }\n\n";

echo "With:\n";
echo "  if ('*' !== \$column) {\n";
echo "    // If we have a column name with nested quotes like \"'column_name'\"\n";
echo "    // preserve the inner quotes while removing the outer ones\n";
echo "    if (substr(\$column, 0, 1) === '\"' && substr(\$column, -1) === '\"') {\n";
echo "      return substr(\$column, 1, -1);\n";
echo "    }\n";
echo "    // For normal column names, just strip all quotes\n";
echo "    return str_replace('\"', '', \$column);\n";
echo "  }\n";
?> 