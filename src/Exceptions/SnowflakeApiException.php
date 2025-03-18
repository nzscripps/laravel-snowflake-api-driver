<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Exceptions;

use Exception;

class SnowflakeApiException extends Exception
{
    /**
     * Context information related to the exception
     * 
     * @var array
     */
    protected $context = [];

    /**
     * Create a new Snowflake API exception
     * 
     * @param string $message The exception message
     * @param int $code The exception code
     * @param Exception|null $previous The previous exception for chaining
     * @param array $context Additional context information
     */
    public function __construct(string $message = "", int $code = 0, Exception $previous = null, array $context = [])
    {
        parent::__construct($message, $code, $previous);
        $this->context = $context;
    }

    /**
     * Get the context information for this exception
     * 
     * @return array
     */
    public function getContext()
    {
        return $this->context;
    }
} 