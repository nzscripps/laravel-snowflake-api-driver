<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Exceptions;

use RuntimeException;
use Throwable;

/**
 * Thrown when a DML statement (INSERT/UPDATE/DELETE/MERGE) was submitted to
 * Snowflake but whether it completed — and so what it changed — could not be
 * confirmed.
 *
 * This is the UNKNOWN outcome, distinct from a statement Snowflake actively
 * REFUSED (an HTTP 4xx response, where nothing ran and it is safe to treat
 * the call as a no-op). Every named constructor below means "this may or may
 * not have executed"; callers of a conditional write (e.g. an
 * optimistic-concurrency `UPDATE ... WHERE WRITE_VERSION = ?`) must treat
 * that as a possible partial success and re-check state, never assume
 * either success or failure.
 *
 * Extends RuntimeException rather than the package's SnowflakeApiException:
 * SnowflakeApiException's constructor requires `?Exception $previous`, but
 * the cause wrapped here should be typed `Throwable` (matching PHP's own
 * Exception::__construct()) so a non-Exception Throwable can never trigger a
 * TypeError while we are trying to report an unrelated problem.
 *
 * IMPORTANT: the message is a fixed, generic sentence plus (optionally) the
 * bare statement handle. It never includes the wrapped cause's own message
 * text (available via getPrevious()). Illuminate\Database\Connection retries
 * a query when the *original* exception's message matches one of
 * Illuminate\Database\LostConnectionDetector's substrings (e.g. "reset by
 * peer", "Broken pipe", "Connection timed out", "went away") — see
 * Connection::tryAgainIfCausedByLostConnection(), which inspects
 * $queryException->getPrevious()->getMessage(). If this exception's own
 * message echoed a transport error's text, Laravel could silently RE-RUN a
 * DML statement whose first submission's fate is already unknown, doubling
 * an update/delete. Keep the message text fixed for that reason.
 */
class StatementOutcomeUnknownException extends RuntimeException
{
    private const FIXED_MESSAGE = 'The Snowflake statement was submitted, but whether it completed could not be confirmed.';

    /**
     * @var string|null
     */
    private $statementHandle;

    public function __construct(string $message, ?string $statementHandle = null, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);

        $this->statementHandle = $statementHandle;
    }

    /**
     * The Snowflake statement handle, when one was assigned before the
     * outcome became unknown. Absent only when submission itself failed
     * before Snowflake ever returned a handle.
     */
    public function getStatementHandle(): ?string
    {
        return $this->statementHandle;
    }

    /**
     * postStatement() itself failed for a non-refusal reason. No statement
     * handle was ever assigned, so we don't know whether Snowflake received
     * the request at all.
     */
    public static function whileSubmitting(Throwable $cause): self
    {
        return new self(self::FIXED_MESSAGE, null, $cause);
    }

    /**
     * The statement was accepted (we have a handle) but waiting for its
     * result failed for a non-refusal reason — a transport failure while
     * polling, or the timeout-cancel path itself failing.
     */
    public static function whileWaiting(string $statementHandle, Throwable $cause): self
    {
        return new self(self::withHandle($statementHandle), $statementHandle, $cause);
    }

    /**
     * waitForStatement()'s own timeout elapsed and it cancelled the
     * statement. The cancel call can race the statement's own commit, so
     * cancellation is not proof that nothing committed.
     */
    public static function afterCancel(string $statementHandle): self
    {
        return new self(self::withHandle($statementHandle), $statementHandle);
    }

    /**
     * The statement executed, but its response carried neither a `stats`
     * object nor a recognizable output row to count rows changed from.
     */
    public static function countsUnreadable(string $statementHandle): self
    {
        return new self(self::withHandle($statementHandle), $statementHandle);
    }

    private static function withHandle(string $statementHandle): string
    {
        return sprintf('%s (statement handle: %s)', self::FIXED_MESSAGE, $statementHandle);
    }
}
