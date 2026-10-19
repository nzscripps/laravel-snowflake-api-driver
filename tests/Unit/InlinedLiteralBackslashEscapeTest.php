<?php

declare(strict_types=1);

namespace Tests\Unit;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use Mockery;
use ReflectionClass;
use Tests\TestCase;

/**
 * Backslash is an escape character inside Snowflake string constants. Every
 * value the driver inlines as a literal must escape it before doubling the
 * single quote, or a value ending in a backslash escapes its own closing
 * quote and the following SQL is read as string content (and the text after
 * the next quote as SQL).
 */
class InlinedLiteralBackslashEscapeTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    private function grammar(): QueryGrammar
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getConfig')->andReturn(null);

        return new QueryGrammar($connection);
    }

    public function test_inserted_values_escape_a_trailing_backslash(): void
    {
        $grammar = $this->grammar();
        $connection = Mockery::mock(Connection::class);
        $builder = new Builder($connection, $grammar, new Processor);
        $builder->from('notes');

        $sql = $grammar->compileInsert($builder, [['a' => 'x\\', 'b' => ', 1); DROP TABLE notes; --']]);

        $this->assertStringContainsString("('x\\\\', ', 1); DROP TABLE notes; --')", $sql);
    }

    public function test_query_grammar_quote_string_escapes_backslash_before_quote(): void
    {
        $this->assertSame("'a\\\\''b\\\\'", $this->grammar()->quoteString("a\\'b\\"));
    }

    public function test_schema_grammar_quote_string_escapes_backslash_before_quote(): void
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getConfig')->andReturn(null);

        $this->assertSame("'a\\\\''b\\\\'", (new SchemaGrammar($connection))->quoteString("a\\'b\\"));
    }

    public function test_connection_quote_escapes_backslash_before_quote(): void
    {
        $connection = (new ReflectionClass(SnowflakeApiConnection::class))->newInstanceWithoutConstructor();

        $this->assertSame("'a\\\\''b\\\\'", $connection->quote("a\\'b\\"));
    }
}
