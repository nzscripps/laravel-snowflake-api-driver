<?php

declare(strict_types=1);

namespace Tests\Unit;

use DateTimeImmutable;
use InvalidArgumentException;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use ReflectionClass;
use ReflectionMethod;
use Tests\TestCase;

/**
 * Snowflake's SQL API receives finished SQL text, so the connection fills `?`
 * placeholders itself (prepareQuery, used by select/cursor/statement/
 * affectingStatement). Only a `?` that is SQL syntax is a placeholder. Taking
 * a `?` inside a string constant for one shifts every later binding, and the
 * shifted value lands OUTSIDE quotes as SQL text.
 */
class SnowflakeApiConnectionPlaceholderTest extends TestCase
{
    private function prepare(string $query, array $bindings): string
    {
        $connection = (new ReflectionClass(SnowflakeApiConnection::class))->newInstanceWithoutConstructor();

        return (new ReflectionMethod(SnowflakeApiConnection::class, 'prepareQuery'))
            ->invoke($connection, $query, $bindings);
    }

    public function test_it_fills_placeholders_in_order(): void
    {
        $this->assertSame(
            "select * from t where a = 'x' and b = 2 and c is NULL and d = 1",
            $this->prepare('select * from t where a = ? and b = ? and c is ? and d = ?', ['x', 2, null, true]),
        );
    }

    public function test_a_question_mark_inside_a_string_constant_does_not_take_a_binding(): void
    {
        // An inlined literal containing `?`, followed by a real placeholder. The
        // literal's `?` must not take the binding, or the bound value lands
        // outside its quotes.
        $query = "select * from t where (t.a = 'x') and (t.b LIKE '%y?%' ESCAPE '\\\\') and (t.c = ?)";

        $this->assertSame(
            "select * from t where (t.a = 'x') and (t.b LIKE '%y?%' ESCAPE '\\\\') and (t.c = ') OR 1=1 OR (1 LIKE')",
            $this->prepare($query, [') OR 1=1 OR (1 LIKE']),
        );
    }

    public function test_doubled_quotes_and_backslash_escapes_keep_a_string_constant_open(): void
    {
        // SQL text: a = 'it''s?' and b = 'x\'?' and c = ?
        $query = "select 1 where a = 'it''s?' and b = 'x\\'?' and c = ?";

        $this->assertSame(
            "select 1 where a = 'it''s?' and b = 'x\\'?' and c = 'v'",
            $this->prepare($query, ['v']),
        );
    }

    public function test_a_backslash_pair_closes_a_string_constant_normally(): void
    {
        // SQL text: a = 'x\\' and b = ?   (the literal is x\ and ends there)
        $this->assertSame(
            "select 1 where a = 'x\\\\' and b = 'v'",
            $this->prepare("select 1 where a = 'x\\\\' and b = ?", ['v']),
        );
    }

    public function test_question_marks_in_identifiers_dollar_strings_and_comments_are_not_placeholders(): void
    {
        $query = implode("\n", [
            'select "odd?col", "a""?b" from t -- why?',
            '// still a comment?',
            '/* block? */ where x = $$raw ? text$$ and y = ?',
        ]);

        $this->assertSame(
            str_replace('y = ?', "y = 'v'", $query),
            $this->prepare($query, ['v']),
        );
    }

    public function test_dollar_signs_inside_an_unquoted_identifier_do_not_open_a_dollar_string(): void
    {
        // Unquoted identifiers may contain `$`; `amount$$usd` is a column, and
        // `$1` is a positional column reference.
        $this->assertSame(
            "select amount$\$usd, $1 from t where a = 'x' and amount$\$usd = 42",
            $this->prepare('select amount$$usd, $1 from t where a = ? and amount$$usd = ?', ['x', 42]),
        );
    }

    public function test_multibyte_text_before_a_placeholder_is_preserved(): void
    {
        $this->assertSame(
            "select 1 where name = 'Café? Médias' and b = 'Zoë'",
            $this->prepare("select 1 where name = 'Café? Médias' and b = ?", ['Zoë']),
        );
    }

    public function test_a_bound_string_ending_in_a_backslash_cannot_escape_its_closing_quote(): void
    {
        // Backslash is an escape character inside Snowflake string constants.
        // Undoubled, 'x\' would swallow its closing quote and the next bound
        // value would become SQL text.
        $this->assertSame(
            "select 1 where a = 'x\\\\' and b = ' OR 1=1 --'",
            $this->prepare('select 1 where a = ? and b = ?', ['x\\', ' OR 1=1 --']),
        );
    }

    public function test_a_bound_string_keeps_backslash_and_quote_escapes_paired(): void
    {
        // Value: a\'b  ->  literal 'a\\''b'
        $this->assertSame(
            "select 'a\\\\''b'",
            $this->prepare('select ?', ["a\\'b"]),
        );
    }

    public function test_a_statement_whose_values_the_grammar_inlined_is_sent_unchanged(): void
    {
        // compileInsert()/compileUpsert() inline every value yet Laravel still
        // passes the values as bindings; a `?` inside an inlined value is text.
        $query = "insert into t (a, b) values ('what?', 'x')";

        $this->assertSame($query, $this->prepare($query, ['what?', 'x']));
    }

    public function test_more_placeholders_than_bindings_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('2 placeholder(s) but 1 binding(s)');

        $this->prepare('select 1 where a = ? and b = ?', ['x']);
    }

    public function test_more_bindings_than_placeholders_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('1 placeholder(s) but 2 binding(s)');

        $this->prepare("select 1 where a = ? and b = 'y?'", ['x', 'y']);
    }

    public function test_a_date_binding_is_formatted_and_quoted(): void
    {
        $connection = (new ReflectionClass(SnowflakeApiConnection::class))->newInstanceWithoutConstructor();
        $connection->setQueryGrammar(new QueryGrammar($connection));

        $this->assertSame(
            "select 1 where d >= '2026-10-19 12:34:56.000000'",
            (new ReflectionMethod(SnowflakeApiConnection::class, 'prepareQuery'))
                ->invoke($connection, 'select 1 where d >= ?', [new DateTimeImmutable('2026-10-19 12:34:56')]),
        );
    }

    public function test_a_stringable_binding_is_quoted_like_a_string(): void
    {
        $value = new class
        {
            public function __toString(): string
            {
                return "x\\') OR 1=1 --";
            }
        };

        $this->assertSame(
            "select 1 where a = 'x\\\\'') OR 1=1 --'",
            $this->prepare('select 1 where a = ?', [$value]),
        );
    }

    public function test_a_binding_that_cannot_be_a_literal_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Snowflake bindings must be');

        $this->prepare('select 1 where a = ?', [new \stdClass]);
    }

    public function test_a_statement_without_bindings_is_sent_unchanged(): void
    {
        $query = "select 1 where a = 'x?'";

        $this->assertSame($query, $this->prepare($query, []));
    }
}
