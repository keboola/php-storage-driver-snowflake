<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateDevBranchCommand;
use Keboola\StorageDriver\Command\Project\CreateDevBranchResponse;
use Keboola\StorageDriver\Snowflake\Handler\Project\CreateDevBranchHandler;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class CreateDevBranchHandlerTest extends BaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->connection->executeQuery(sprintf(
            'CREATE OR REPLACE ROLE %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $this->getTestHash() . '_RO',
            ),
        ));
        $this->connection->executeQuery(sprintf(
            'CREATE OR REPLACE ROLE %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $this->getTestHash() . '_PRJ',
            ),
        ));
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $this->getTestHash() . '_RO',
            ),
        ));
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $this->getTestHash() . '_PRJ',
            ),
        ));
    }

    public function testCreateDevBranch(): void
    {
        $credentials = $this->createCredentialsWithKeyPair();
        $command = new CreateDevBranchCommand([
            'stackPrefix' => $this->getStackPrefix(),
            'projectId' => '123',
            'branchId' => '456',
            'projectRoleName' => $this->getTestHash() . '_PRJ',
            'projectReadOnlyRoleName' => $this->getTestHash() . '_RO',
        ]);

        $roleName = (new NameGenerator($command->getStackPrefix()))
            ->createReadOnlyRoleNameForBranch(
                $command->getProjectId(),
                $command->getBranchId(),
            );
        $this->connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $roleName,
            ),
        ));

        $response = (new CreateDevBranchHandler)(
            $credentials,
            $command,
            [
                'input-mapping-read-only-storage',
            ],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateDevBranchResponse::class, $response);

        $roles = $this->connection->fetchAllAssociative(sprintf(
            'SHOW ROLES STARTS WITH %s;',
            SnowflakeQuote::quoteSingleIdentifier(
                $roleName,
            ),
        ));
        $this->assertCount(1, $roles);
    }
}
