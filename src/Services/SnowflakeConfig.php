<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

class SnowflakeConfig
{
    private $baseUrl;

    private $account;

    private $user;

    private $publicKey;

    private $privateKey;

    private $privateKeyPassphrase;

    private $warehouse;

    private $database;

    private $schema;

    private $timeout;

    /**
     * Initialize the Snowflake API configuration
     *
     * @param  string  $baseUrl  The base URL for the Snowflake API
     * @param  string  $account  The Snowflake account identifier
     * @param  string  $user  The Snowflake username
     * @param  string  $publicKey  The public key fingerprint
     * @param  string  $privateKey  The private key content (PEM format)
     * @param  string  $privateKeyPassphrase  The passphrase for the private key
     * @param  string  $warehouse  The Snowflake warehouse to use
     * @param  string  $database  The Snowflake database to use
     * @param  string  $schema  The Snowflake schema to use
     * @param  int  $timeout  Timeout in seconds for query execution
     */
    public function __construct(
        string $baseUrl,
        string $account,
        string $user,
        string $publicKey,
        string $privateKey,
        string $privateKeyPassphrase,
        string $warehouse,
        string $database,
        string $schema,
        int $timeout
    ) {
        $this->baseUrl = $baseUrl;
        $this->account = $account;
        $this->user = $user;
        $this->publicKey = $publicKey;
        $this->privateKey = $privateKey;
        $this->privateKeyPassphrase = $privateKeyPassphrase;
        $this->warehouse = $warehouse;
        $this->database = $database;
        $this->schema = $schema;
        $this->timeout = $timeout;
    }

    /**
     * @return string
     */
    public function getBaseUrl()
    {
        return $this->baseUrl;
    }

    /**
     * @return string
     */
    public function getAccount()
    {
        return $this->account;
    }

    /**
     * @return string
     */
    public function getUser()
    {
        return $this->user;
    }

    /**
     * @return string
     */
    public function getPublicKey()
    {
        return $this->publicKey;
    }

    /**
     * @return string
     */
    public function getPrivateKey()
    {
        return $this->privateKey;
    }

    /**
     * @return string
     */
    public function getPrivateKeyPassphrase()
    {
        return $this->privateKeyPassphrase;
    }

    /**
     * @return string
     */
    public function getWarehouse()
    {
        return $this->warehouse;
    }

    /**
     * @return string
     */
    public function getDatabase()
    {
        return $this->database;
    }

    /**
     * @return string
     */
    public function getSchema()
    {
        return $this->schema;
    }

    /**
     * @return int
     */
    public function getTimeout()
    {
        return $this->timeout;
    }
}
